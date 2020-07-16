// Package beanstalk wrappers beanstalk client as a broker.
package beanstalk

import (
	"bufio"
	"medispatcher/logger"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"git.oschina.net/chaos.su/beanstalkc"
)

const (
	ERROR_CONN_CLOSED          = "EOF"
	ERROR_CONN_BROKEN          = "broken pipe"
	ERROR_CONN_RESET           = "connection reset by peer"
	ERROR_CONN_READ_CLOSE      = "use of closed network connection"
	DEFAULT_CONNECTION_TIMEOUT = 3
)

type longCmd struct {
	name string
	arg  string
}
type Broker struct {
	beanstalkc.Client
	addr string
	// 任何一个命令都需调用lock
	locker *sync.Mutex
	// 执行多条命令时独占的lock
	transLocker *sync.Mutex
	// watch, unwatch, use
	// these will be needed when rebuild connection.
	longCmdsHistory     []longCmd
	longCmdsHistoryLock sync.Mutex
	// 等于1时表示正在重建链接
	rebuildingConnection int32

	currentUsingTopic string
	exitStage         chan bool
}

func createClient(hostAddr string) (*beanstalkc.Client, error) {
	conn, err := net.DialTimeout("tcp", hostAddr, time.Second*DEFAULT_CONNECTION_TIMEOUT)
	if err != nil {
		return nil, err
	}
	r := bufio.NewReaderSize(conn, 256)
	w := bufio.NewWriterSize(conn, 256)
	return &beanstalkc.Client{
		Conn:       conn,
		ReadWriter: bufio.NewReadWriter(r, w),
	}, nil
}
func New(hostAddr string) (br *Broker, err error) {
	var client *beanstalkc.Client
	client, err = createClient(hostAddr)
	if err != nil {
		return
	}
	br = &Broker{
		locker:      new(sync.Mutex),
		transLocker: new(sync.Mutex),
	}
	br.Client = *client
	br.addr = hostAddr
	br.exitStage = make(chan bool)
	br.longCmdsHistory = []longCmd{}
	return
}

func (br *Broker) lock() {
	br.locker.Lock()
}

func (br *Broker) unlock() {
	br.locker.Unlock()
}

func (br *Broker) pushLongCmd(cmd longCmd) {
	br.longCmdsHistoryLock.Lock()
	defer br.longCmdsHistoryLock.Unlock()
	switch cmd.name {
	case "use":
		for _, oCmd := range br.longCmdsHistory {
			if oCmd.name == "use" && oCmd.arg == cmd.arg {
				return
			}
		}
		br.longCmdsHistory = append(br.longCmdsHistory, cmd)
	case "watch":
		newHistory := []longCmd{}
		for _, oCmd := range br.longCmdsHistory {
			if (oCmd.name == "watch" || oCmd.name == "unwatch") && oCmd.arg == cmd.arg {
				continue
			}
			newHistory = append(newHistory, oCmd)
		}
		br.longCmdsHistory = append(br.longCmdsHistory, cmd)
	case "unwatch":
		newHistory := []longCmd{}
		for _, oCmd := range br.longCmdsHistory {
			if (oCmd.name == "watch" || oCmd.name == "unwatch") && oCmd.arg == cmd.arg {
				continue
			}
			newHistory = append(newHistory, oCmd)
		}
	}
}

func (br *Broker) rebuildConn() {
	// one rebuild routine only
	if !atomic.CompareAndSwapInt32(&br.rebuildingConnection, 0, 1) {
		return
	}
	br.lock()
	defer br.unlock()
	for {
		select {
		case <-br.exitStage:
			return
		default:
		}
		logger.GetLogger("INFO").Printf("Re-connecting to %v...", br.addr)
		client, err := createClient(br.addr)
		if err == nil {
			br.Client.Conn.Close()
			br.Client = *client
			logger.GetLogger("INFO").Printf("Re-connected to %v", br.addr)
			break
		} else {
			logger.GetLogger("INFO").Printf("Failed to re-connected to %v--%v", br.addr, err)
		}
	}
	for _, cmd := range br.longCmdsHistory {
		switch cmd.name {
		case "watch":
			br.Client.Watch(cmd.arg)
		case "use":
			br.Client.Use(cmd.arg)
		}
	}
	br.rebuildingConnection = 0
}

func (br *Broker) Pub(topic string, priority uint32, delay, ttr uint64, data []byte) (jobId uint64, err error) {
	br.lock()
	defer br.unlock()
	if len(topic) > 0 {
		if topic != br.currentUsingTopic {
			// use topic again, in case multiple goroutines are blocked on re-buildconnection stage and overrides other's topics.
			br.currentUsingTopic = topic
			err = br.Client.Use(topic)
			if err != nil {
				return
			}
		}
	}
	jobId, _, err = br.Put(priority, delay, ttr, data)
	return
}

func (br *Broker) ListTopics() (topics []string, err error) {
	br.lock()
	defer br.unlock()
	topics, err = br.ListTubes()
	return
}

func (br *Broker) StatsTopic(topicName string) (stats map[string]interface{}, err error) {
	br.lock()
	defer br.unlock()
	stats, err = br.StatsTube(topicName)
	return
}

func (br *Broker) StatsJob(jobId uint64) (stats map[string]interface{}, err error) {
	br.lock()
	defer br.unlock()
	stats, err = br.Client.StatsJob(jobId)
	return
}

func (br *Broker) Stats() (stats map[string]interface{}, err error) {
	br.lock()
	defer br.unlock()
	return br.Client.Stats()
}

func (br *Broker) Release(jobId uint64, priority uint32, delay uint64) (err error) {
	br.lock()
	defer br.unlock()
	_, err = br.Client.Release(jobId, priority, delay)
	return
}

func (br *Broker) Close() {
	br.lock()
	defer func() {
		br.unlock()
	}()

	select {
	case <-br.exitStage:
		return
	default:
		close(br.exitStage)
	}
	if br.Conn == nil {
		return
	}
	br.Conn.Close()
}

func (br *Broker) ForceClose() {
	select {
	case <-br.exitStage:
		return
	default:
		close(br.exitStage)
	}
	if br.Conn == nil {
		return
	}
	br.Conn.Close()
}

func (br *Broker) UnWatch(topicName string) (err error) {
	br.lock()
	defer br.unlock()
	_, err = br.Client.Ignore(topicName)
	br.pushLongCmd(longCmd{"unwatch", topicName})
	return
}

func (br *Broker) Watch(topicName string) (err error) {
	br.lock()
	defer br.unlock()
	br.pushLongCmd(longCmd{"watch", topicName})
	return br.Client.Watch(topicName)
}

func (br *Broker) Peek(jobId uint64) (jobData []byte, err error) {
	br.lock()
	defer br.unlock()
	return br.Client.Peek(jobId)
}

func (br *Broker) Bury(jobId uint64) (err error) {
	br.lock()
	defer br.unlock()
	return br.Client.Bury(jobId, 1024)
}

func (br *Broker) Delete(jobId uint64) (err error) {
	br.lock()
	defer br.unlock()
	return br.Client.Delete(jobId)
}

func (br *Broker) KickJob(jobId uint64) (jerr error) {
	br.lock()
	defer br.unlock()
	return br.Client.KickJob(jobId)
}

func (br *Broker) Reserve() (jobId uint64, jobData []byte, err error) {
	br.lock()
	defer br.unlock()
	jobId, jobData, err = br.Client.Reserve()
	return
}

func (br *Broker) Use(topicName string) error {
	br.lock()
	defer br.unlock()
	if br.currentUsingTopic == topicName {
		return nil
	}
	br.currentUsingTopic = topicName
	br.pushLongCmd(longCmd{"use", topicName})
	return br.Client.Use(topicName)
}
