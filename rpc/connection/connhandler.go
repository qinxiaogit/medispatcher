package connection

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"github.com/qinxiaogit/medispatcher/config"
	"github.com/qinxiaogit/medispatcher/logger"
	"github.com/qinxiaogit/medispatcher/rpc"
	"github.com/qinxiaogit/medispatcher/rpc/handlers"
	"net"
	"reflect"
	"strconv"
	"time"
)

const (
	HEAD_TAG_LENGTH   = 8
	STATUS_TAG_LENGTH = 8
)

var current_connctions = 0

var byte0 = byte(0)

var tagClose = []byte("CLOSE")

var counterCh = make(chan int32, 100)
var masterCommCh = make(chan int, 1)
var readConnCountCh = make(chan int, 1)
var stoppingLocalServer = false
var localServerStoppingStateQueryLock = make(chan int, 1)
var sso *StoppingSigalOp

var errMsgOutOfMaxConn = fmt.Sprintf("Too many connections, out of %d.\n", config.MAX_CONNECTIONS)

func init() {
	ssoLockChan := make(chan bool, 1)
	sso = &StoppingSigalOp{lockChan: &ssoLockChan, stoppingSigal: make(chan bool, 100)}
}

func (sso *StoppingSigalOp) lock() {
	*sso.lockChan <- true
}

func (sso *StoppingSigalOp) unlock() {
	<-*sso.lockChan
}

// Use adds an reference count of the stopping signal.
func (sso *StoppingSigalOp) Use() {
	sso.lock()
	defer sso.unlock()
	sso.userCounter += 1
}

// Unuse minus an reference count of the stopping signal.
func (sso *StoppingSigalOp) Unuse() {
	sso.lock()
	defer sso.unlock()
	sso.userCounter -= 1
}

func (sso *StoppingSigalOp) SendSignal() {
	sso.lock()
	defer sso.unlock()
	for c := sso.userCounter; c > 0; c-- {
		sso.stoppingSigal <- true
	}
}

// status: 不超过8字节
// data: 一般为json序列化后的数据
func response(conn *net.Conn, status string, data []byte) error {
	statusTag := []byte(status)
	statusTag = append(statusTag, make([]byte, STATUS_TAG_LENGTH-len(statusTag))...)

	dataLenBtye := strconv.Itoa(len(data))
	padLen := 8 - len(dataLenBtye)
	headTag := append([]byte(dataLenBtye), make([]byte, padLen)...)
	_, err := (*conn).Write(headTag)
	if err != nil {
		return err
	}
	_, err = (*conn).Write(statusTag)
	if err != nil {
		return err
	}
	_, err = (*conn).Write(data)
	return err
}

func isTagClose(tag []byte) bool {
	return bytes.Compare(tagClose, tag) == 0
}

func parseHeadTagLength(tag []byte) []byte {
	var headerLength []byte
	for i := 0; i < HEAD_TAG_LENGTH && tag[i] != byte0; i++ {
		headerLength = append(headerLength, tag[i])
	}
	return headerLength
}

func readRequest(conn *net.Conn, dataChan chan *Request) {
	var count int
	var err error
	var headComplete, closed bool
	headerTag := make([]byte, HEAD_TAG_LENGTH)
	result := &Request{}
	defer func() {
		result.Err = err
		result.Closed = closed
		dataChan <- result
	}()
	start := 0

	for start < HEAD_TAG_LENGTH {
		if start == 0 {
			// 设置客户端空闲超时
			(*conn).SetDeadline(time.Now().Add(config.CLIENT_MAX_IDLE))
		} else {
			// 设置客户端返回超时时间.
			(*conn).SetReadDeadline(time.Now().Add(config.CLIENT_TIMEOUT))
		}
		count, err = (*conn).Read(headerTag[start:])
		if err != nil {
			headComplete = bytes.Compare(tagClose, headerTag[0:5]) == 0
			// 如果客户端发送了CLOSE标识，或者直接关闭连接都认为是正常关闭链接，不再进一步进行确认。
			if start > 0 && !headComplete {
				// 数据未读完, 且不为关闭连接命令
				err = errors.New(fmt.Sprintf("client data length read error: %s", err))
			} else {
				err = nil
				closed = true
			}
			return
		}
		start += count
	}

	headerTagLength := parseHeadTagLength(headerTag)
	closed = isTagClose(headerTagLength)
	if closed {
		return
	}

	var bodyLength int
	bodyLength, err = strconv.Atoi(string(headerTagLength))

	if err != nil {
		err = errors.New(fmt.Sprintf("Request length parse error: %s", err))
		return
	}

	if bodyLength < 1 {
		err = errors.New("数据长度小于1.")
		return
	}
	result.Body = make([]byte, bodyLength)
	start = 0
	for start < bodyLength {
		(*conn).SetReadDeadline(time.Now().Add(config.CLIENT_TIMEOUT))
		count, err = (*conn).Read(result.Body[start:])
		if err != nil {
			if err != io.EOF && start < bodyLength-1 {
				err = errors.New(fmt.Sprint("client data read premature"))
			} else {
				err = errors.New(fmt.Sprintf("read client data error: %s", err))
			}
			return
		}
		start += count
	}
	return
}

func connCounter(ch *chan int32) {
	var m int32
	for {
		m = <-counterCh
		readConnCountCh <- 1
		switch m {
		case 1:
			current_connctions += 1
		case -1:
			current_connctions -= 1
		}
		<-readConnCountCh
	}
}

func GetCurrentConnectionCount() int {
	readConnCountCh <- 1
	c := current_connctions
	<-readConnCountCh
	return c
}

func setStoppingState() {
	localServerStoppingStateQueryLock <- 1
	stoppingLocalServer = true
	<-localServerStoppingStateQueryLock
}

func isServerStopping() bool {
	localServerStoppingStateQueryLock <- 1
	state := stoppingLocalServer
	<-localServerStoppingStateQueryLock
	return state
}

func Stop(exitSigChan *chan string) {
	// 如果有多次调用，后边的协程将被阻塞
	masterCommCh <- 1
	setStoppingState()
	sso.SendSignal()
	for GetCurrentConnectionCount() > 0 {
		time.Sleep(time.Millisecond * 10)
	}

	// 等待队列清理服务终止.
	handlers.SetStoppingState()
	handlers.QueueCleanWaitGroup.Wait()

	*exitSigChan <- "rpcservices"
}

// 启动本地配置服务
func StartServer() {
	var addr, netType string

	addr = config.GetConfig().ListenAddr

	netType = "tcp"
	server, err := net.Listen(netType, addr)
	if err != nil {
		panic("Rpc Server start error:" + err.Error())
	}
	go connCounter(&counterCh)
	go receiveStats()
	go startConsole()
	// 迁移到k8s后, 管理后台不能够直接调用推送服务的api, 因此通过etcd转发相关操作请求
	// go adminEventWatch()

	for !isServerStopping() {
		inConn, err := server.Accept()
		if err != nil {
			logger.GetLogger("ERROR").Printf("Accept connection error: %s. Retry in 0.1 seconds.", err)
			time.Sleep(time.Millisecond * 100)
			continue
		}
		if GetCurrentConnectionCount() > config.MAX_CONNECTIONS {
			go func(inConn *net.Conn) {
				logger.GetLogger("ERROR").Print(errMsgOutOfMaxConn)
				err := response(inConn, "failed", []byte(errMsgOutOfMaxConn))
				if err != nil {
					logger.GetLogger("ERROR").Printf("Error when response to client: %s", err)
				}
				(*inConn).Close()
			}(&inConn)
			time.Sleep(time.Millisecond * 10)
			continue
		}
		go HandleConn(&inConn)
		counterCh <- int32(1)
	}
}

// 处理获取配置的连接请求
func HandleConn(conn *net.Conn) {
	defer func() {
		(*conn).Write([]byte("CLOSE"))
		time.Sleep(time.Millisecond * 20)
		err := (*conn).Close()
		if err != nil {
			//	logger.Printf("连接关闭出错: %s", err)
		}
		counterCh <- int32(-1)
	}()

	if isServerStopping() {
		return
	}
	sso.Use()
	requestDataChan := make(chan *Request)
	go readRequest(conn, requestDataChan)
	for {
		select {
		case <-sso.stoppingSigal:
			return
		case request := <-requestDataChan:
			if request.Closed {
				sso.Unuse()
				return
			}
			if request.Err != nil {
				logger.GetLogger("ERROR").Printf("Client data read error: %s", request.Err)
				sso.Unuse()
				return
			}

			re, err := processRequest(request.Body)
			var status string
			if err == nil {
				status = "ok"
			} else {
				status = "failed"
				re, _ = json.Marshal(err.Error())
			}
			err = response(conn, status, re)
			if err != nil {
				logger.GetLogger("ERROR").Printf("Response write error: %s\n", err)
				sso.Unuse()
				return
			} else {
				go readRequest(conn, requestDataChan)
			}
		}
	}
}

// requestdata
// map[sring]interface{}
//request["cmd"]
//request["args"]interface...
func processRequest(requestData []byte) (re []byte, err error) {
	var clientData map[string]interface{}
	err = json.Unmarshal(requestData, &clientData)
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to decode client data: %s\nRaw data: %s", err, requestData))
		return
	}
	// TODO: more strict client data checks to avoid server crash.
	if _, exists := clientData["cmd"]; !exists {
		err = errors.New("Illegal client request: 'cmd' field not found!")
		return
	}

	if _, ok := clientData["cmd"].(string); !ok {
		err = errors.New("Args needs string while received: " + reflect.TypeOf(clientData["cmd"]).String())
		return
	}

	if _, exists := clientData["args"]; !exists {
		err = errors.New("Illegal client request: 'args' field not found!")
		return
	}

	if _, ok := clientData["args"].(map[string]interface{}); !ok {
		err = errors.New("Args needs map[string]interface while received: " + reflect.TypeOf(clientData["args"]).String())
		return
	}

	pqCh := make(chan ProcessResult)
	go runRequestHandler(pqCh, clientData["cmd"].(string), clientData["args"].(map[string]interface{}))
	pTimeout := time.After(time.Second * time.Duration(config.PROCESS_TIMEOUT))

	select {
	case result := <-pqCh:
		re = result.Result
		err = result.Error
	case <-pTimeout:
		err = errors.New("Process timeout.")
		logger.GetLogger("WARN").Printf("Cmd: %s: %v", clientData["cmd"], err)
	}
	return
}

func runRequestHandler(reCh chan ProcessResult, cmd string, args map[string]interface{}) {
	go sendStats(StatsMessage{cmd, CONNACTION_ACCEPT, CONNACTION_STATUS_OK})
	re, err := rpc.GetHandlerContainer().Run(cmd, args)
	var reB []byte
	if err == nil {
		reB, err = json.Marshal(re)
		if err != nil {
			err = errors.New(fmt.Sprintf("Failed to encode result: %v", err))
		}
	}
	reCh <- ProcessResult{reB, err}
	var finStatus ConnActionStatus
	if err != nil {
		finStatus = CONNACTION_STATUS_FAILED
	} else {
		finStatus = CONNACTION_STATUS_OK
	}
	go sendStats(StatsMessage{cmd, CONNACTION_PROCESS_FIN, finStatus})
}

// 向数据统计器发送统计信息
func sendStats(msg StatsMessage) {
	getStatsRWLock()
	statsInChan <- msg
	releaseStatsRWLock()
}

// adminEventWatch 通过watch etcd从而响应消息中心后台的操作请求
// 通过k8s运行推送服务的时候, 只能通过这种方法获取到后台对订阅配置的修改.
/*func adminEventWatch() {
	dc := doveclientCli.NewDoveClient("unix:////var/lib/doveclient/doveclient.sock")
	status, result, err := dc.Call("GetEtcdAddr", map[string]interface{}{})
	if err != nil {
		logger.GetLogger("ERROR").Printf("获取配置环境信息时发生错误: %v", err)
		panic(err)
	}
	dc.Close()

	if status != "ok" {
		logger.GetLogger("ERROR").Printf("调用doveclient接口失败: status=%s, result=%s", status, result)
		panic(fmt.Errorf("调用doveclient接口失败: status=%s, result=%s", status, result))
	}

	addrs := []string{}
	if err := json.Unmarshal(result, &addrs); err != nil || len(result) == 0 {
		logger.GetLogger("ERROR").Printf("从doveclient api返回结果中解析etcd地址失败: result=%s, err=%v", result, err)
		panic(fmt.Errorf("从doveclient api返回结果中解析etcd地址失败: result=%s, err=%v", result, err))
	}

	var once sync.Once
	var client *clientv3.Client
	var rev int64
	bootstrap := true
try:
	if !bootstrap {
		if client != nil {
			client.Close()
		}
		logger.GetLogger("WARN").Printf("[etcd watch] Re create connection")
		time.Sleep(1 * time.Second)
	}
	bootstrap = false

	client, err = clientv3.New(clientv3.Config{
		Endpoints:            addrs,
		DialTimeout:          3 * time.Second,
		DialKeepAliveTime:    5 * time.Second,
		DialKeepAliveTimeout: 3 * time.Second,
		AutoSyncInterval:     10 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	// 上报GetDefaultSubscriptionSettings配置
	once.Do(func() {
		// GetSubscriptionParams 用处不大, 暂不实现
		re, _ := rpc.GetHandlerContainer().Run("GetDefaultSubscriptionSettings", nil)
		body, _ := json.Marshal(re)
		if _, err := client.Put(context.Background(), "__mec.event.GetDefaultSubscriptionSettings", string(body)); err != nil {
			panic(err)
		}
	})

	logger.GetLogger("INFO").Printf("[etcd watch] enter watch loop")

	// __mec.event.ClearDataCache
	// __mec.event.SetDefaultAlarm
	// __mec.event.SetSubscriptionParams.{subscriptionID}
	// __mec.event.GetSubscriptionParams.{subscriptionID}
	// __mec.event.GetDefaultSubscriptionSettings
	// __mec.event.CleanTopic
	watchOpts := []clientv3.OpOption{clientv3.WithPrefix()}
	if rev > 0 {
		watchOpts = append(watchOpts, clientv3.WithRev(rev))
	}
	watchChan := client.Watch(context.Background(), "__mec.event.", watchOpts...)
	for {
		select {
		case resp, ok := <-watchChan:
			if !ok || resp.Err() != nil {
				goto try
			}
			rev = resp.Header.Revision

			for _, ev := range resp.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					switch strings.Split(string(ev.Kv.Key), ".")[2] {
					case "ClearDataCache", "SetDefaultAlarm", "SetSubscriptionParams", "CleanTopic":
						args := map[string]interface{}{}
						// ClearDataCache 默认传入的参数是"[]"会导致解析异常, 所以这里忽略ClearDataCache参数的解析错误
						if err := json.Unmarshal(ev.Kv.Value, &args); err != nil && strings.Split(string(ev.Kv.Key), ".")[2] != "ClearDataCache" {
							logger.GetLogger("INFO").Printf("[etcd watch] type=%d; key=%s; value=%s; rev=%d; err=%s", ev.Type, ev.Kv.Key, ev.Kv.Value, rev, "Error parsing parameter")
							continue
						}

						re, err := rpc.GetHandlerContainer().Run(strings.Split(string(ev.Kv.Key), ".")[2], args)
						if err != nil {
							logger.GetLogger("INFO").Printf("[etcd watch] type=%d; key=%s; value=%s; rev=%d; err=%v", ev.Type, ev.Kv.Key, ev.Kv.Value, rev, err)
							continue
						}
						logger.GetLogger("INFO").Printf("[etcd watch] type=%d; key=%s; value=%s; rev=%d; result=%v", ev.Type, ev.Kv.Key, ev.Kv.Value, rev, re)
					default:
						logger.GetLogger("INFO").Printf("[etcd watch] type=%d; key=%s; value=%s; rev=%d; err=%s", ev.Type, ev.Kv.Key, ev.Kv.Value, rev, "Ignored actions")
					}
				case clientv3.EventTypeDelete:
					logger.GetLogger("INFO").Printf("[etcd watch] type=%d; key=%s; value=%s; rev=%d", ev.Type, ev.Kv.Key, ev.Kv.Value, rev)
					if strings.HasPrefix(string(ev.Kv.Key), "__mec.event.SetSubscriptionParams") {
						continue
					}
				default:
					logger.GetLogger("INFO").Printf("[etcd watch] type=%d; key=%s; value=%s; rev=%d; err=%s", ev.Type, ev.Kv.Key, ev.Kv.Value, rev, "unknown operation")
				}
			}
		}
	}
}*/
