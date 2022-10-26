package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"github.com/qinxiaogit/medispatcher/broker"
	"github.com/qinxiaogit/medispatcher/broker/beanstalk"
	"github.com/qinxiaogit/medispatcher/config"
	"github.com/qinxiaogit/medispatcher/data"
	"github.com/qinxiaogit/medispatcher/msgredist"
	"github.com/qinxiaogit/medispatcher/osutil"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
)

const (
	prefix = "subscription"
)

var (
	dataDir   string
	subId     int
	brCmdPool *beanstalk.SafeBrokerkPool
	exitChan  chan struct{}
)

var (
	totalMessage int
)

func Init() {
	flags := config.GetFlags()
	flags.StringVar(&dataDir, "d", ".", "drop message log dir")
	flags.IntVar(&subId, "i", 0, "subscription id")
	err := config.Setup()
	if err != nil {
		log.Panicf("Config setup err: %v", err)
	}
}

func shouldExit() bool {
	select {
	case <-exitChan:
		return true
	default:
		return false
	}
}

func main() {
	Init()

	exitChan = make(chan struct{})
	brCmdPool = broker.GetBrokerPoolWithBlock(uint32(config.GetConfig().ListenersOfMainQueue), msgredist.INTERVAL_OF_RETRY_ON_CONN_FAIL, shouldExit)

	var wg sync.WaitGroup
	err := os.Chdir(dataDir)
	if err != nil {
		log.Fatalf("chdir %s: %s", dataDir, err.Error())
	}

	if subId != 0 {
		wg.Add(1)
		go func() {
			recoverDropMessage(exitChan, int32(subId))
			wg.Done()
		}()
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGSTOP)
	for {
		s := <-c
		log.Printf("get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
			close(exitChan)
			wg.Wait()
			brCmdPool.Close(false)
			log.Printf("总处理消息数: %d", totalMessage)
			return
		}
	}
}

func recoverDropMessage(exitChan chan struct{}, subId int32) {
	metaData, err := getMetaData(subId)
	if err != nil {
		log.Fatalf("get subscription [%d] metadata: %s", subId, err.Error())
	}

	files, err := listRecoverFiles(subId, metaData)
	if err != nil {
		log.Fatalf("list subscription [%d] recover files: %s", subId, err.Error())
	}
	for _, file := range files {
		log.Printf("found recover file: %s", file)
	}

	for _, file := range files {
		select {
		case <-exitChan:
			log.Printf("stop recover files")
			return
		default:
			metaData.filename = file
			log.Printf("recover file %s:%d", metaData.filename, metaData.position)
			err := recoverFile(exitChan, file, metaData)
			if err != nil {
				log.Fatalf("recover file %s: %s", file, err.Error())
			}
			metaData.position = 0
		}
	}
}

func getMetaData(subId int32) (*MetaData, error) {
	metaData := &MetaData{}
	mfileName := fmt.Sprintf("%s_%d.metadata", prefix, subId)
	if !osutil.FileExists(mfileName) {
		log.Printf("no metadata found")
		f, err := os.Create(mfileName)
		if err != nil {
			return metaData, err
		}
		metaData.f = f
		return metaData, nil
	}
	f, err := os.OpenFile(mfileName, os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("open metadata file %s: %s", mfileName, err.Error())
	}
	metaData.f = f
	err = metaData.read()
	log.Printf("found metadata %s:%d", metaData.filename, metaData.position)
	return metaData, err
}

func listRecoverFiles(subId int32, m *MetaData) ([]string, error) {
	f, err := os.Open(".")
	if err != nil {
		return nil, err
	}
	list, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name() < list[j].Name() })
	var files []string
	for _, fi := range list {
		match, err := filepath.Match(fmt.Sprintf("%s_%d_*.data", prefix, subId), fi.Name())
		if err != nil {
			return nil, fmt.Errorf("match subscription [%d]: %s", subId, err.Error())
		}
		if match && fi.Name() >= m.filename {
			files = append(files, fi.Name())
		}
	}
	return files, nil
}

func recoverFile(exitChan chan struct{}, fname string, metaData *MetaData) error {
	f, err := os.Open(fname)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Seek(metaData.position, 0)
	if err != nil {
		return err
	}

	r := bufio.NewReader(f)
	for {
		select {
		case <-exitChan:
			log.Printf("stop recover file %s:%d", fname, metaData.position)
			return nil
		default:
			rawData, err := r.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					return nil
				} else {
					return err
				}
			}
			msg, err := decodeMessage(rawData)
			if err != nil {
				return fmt.Errorf("decode message: %s", err.Error())
			}

			err = handleMessageStruct(msg)
			if err != nil {
				return fmt.Errorf("handle message: %s", err.Error())
			}
			metaData.position += int64(len(rawData))
			if err := metaData.write(); err != nil {
				return fmt.Errorf("write metadata: %s", err.Error())
			}
		}
	}
}

func decodeMessage(rawData []byte) (*data.MessageStuct, error) {
	var msgR beanstalk.Msg
	err := json.Unmarshal(rawData[:len(rawData)-1], &msgR)
	if err != nil {
		return nil, err
	}

	msg, err := data.UnserializeMessage(msgR.Body)
	if err != nil {
		return nil, err
	}
	msg.OriginJobId = msgR.Id
	return msg, nil
}

// 恢复消息到队列
func handleMessageStruct(msg *data.MessageStuct) error {
	subChannel := config.GetChannelName(msg.MsgKey, int32(subId))
	jobBody, err := data.SerializeMessage(*msg)
	if err != nil {
		return err
	}
	_, err = brCmdPool.Pub(subChannel, jobBody, broker.DEFAULT_MSG_PRIORITY, 0, broker.DEFAULT_MSG_TTR)
	totalMessage++
	return err
}

type MetaData struct {
	filename string
	position int64
	f        *os.File
}

func (m *MetaData) write() error {
	err := m.f.Truncate(0)
	if err != nil {
		return err
	}
	_, err = m.f.Seek(0, 0)
	if err != nil {
		return err
	}
	_, err = m.f.Write([]byte(fmt.Sprintf("%s\n%d\n", m.filename, m.position)))
	return err
}

func (m *MetaData) read() error {
	r := bufio.NewReader(m.f)
	fname, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read metadata: %s", err.Error())
	}

	position, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read metadata: %s", err.Error())
	}

	pos, err := strconv.ParseInt(position[:len(position)-1], 10, 64)
	if err != nil {
		return fmt.Errorf("read metadata: %s", err.Error())
	}
	m.filename = fname[:len(fname)-1]
	m.position = pos
	return nil
}
