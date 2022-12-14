package beanstalk

import (
	"errors"
	"fmt"
	"math/rand"
	"github.com/qinxiaogit/medispatcher/logger"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"runtime/debug"

	"git.oschina.net/chaos.su/beanstalkc"
)

type errCheck struct {
	err error
	br  *Broker
}

type SafeBrokerkPool struct {
	sync.RWMutex
	poolAvailable   map[string]*Broker
	poolBroken      map[string]*Broker
	errCheckChan    chan *errCheck
	exitStage       chan bool
	closeLock       *sync.Mutex
	reserveCalled   int32
	reserveStopChan chan bool
}

func NewSafeBrokerPool(hostAddr string, concurrency uint32) (pool *SafeBrokerkPool, err error) {
	if concurrency < 1 {
		concurrency = 1
	}
	addrs := strings.Split(hostAddr, ",")
	if len(addrs) < 1 {
		err = errors.New("Empty addresses.")
	}

	newPool := &SafeBrokerkPool{
		poolAvailable: map[string]*Broker{},
		errCheckChan:  make(chan *errCheck, 100),
		exitStage:     make(chan bool),
		closeLock:     new(sync.Mutex),
	}
	defer func() {
		if err != nil {
			if len(newPool.poolAvailable) > 0 {
				for _, br := range newPool.poolAvailable {
					br.Close()
				}
			}
		}
	}()
	var br *Broker
	for _, hostAddr = range addrs {
		for i := int(concurrency) - 1; i >= 0; i-- {
			br, err = New(hostAddr)
			if err != nil {
				return nil, err
			}
			newPool.poolAvailable[fmt.Sprintf("%v_%v", hostAddr, i)] = br
		}
	}
	go newPool.connErrorMonitor()

	return newPool, err
}

func (p *SafeBrokerkPool) getOneBroker() *Broker {
	var br *Broker
TRY:
	for {
		select {
		case <-p.exitStage:
			return nil
		default:
		}

		rand.Seed(time.Now().UnixNano())
		i := rand.Intn(len(p.poolAvailable))
		n := 0
		for _, br = range p.poolAvailable {
			if i == n {
				// prevent all routines from being blocked by the broken broker.
				if atomic.LoadInt32(&br.rebuildingConnection) == 1 {
					logger.GetLogger("INFO").Printf("br rebuilding")
					// sleep to avoid cpu consuming overhead when all broker connections are gone.
					time.Sleep(time.Microsecond * 100)
					continue TRY
				}
				break
			}
			n++
		}
		return br
	}
}

func (p *SafeBrokerkPool) getOneBrokerByAddr(queueServer string) *Broker {
	var br *Broker
	for _, br = range p.poolAvailable {
		if br.addr == queueServer {
			return br
		}
	}
	return nil
}

func (p *SafeBrokerkPool) notifyBrokerErr(br *Broker, err error) {
	if strings.Contains(err.Error(), "NOT_FOUND") {
		return
	}
	logger.GetLogger("WARN").Printf("Client ERR: %v, TRACE: %v", err, string(debug.Stack()))
	p.errCheckChan <- &errCheck{err, br}
}

// monitor connection error and rebuild it if needed.
func (p *SafeBrokerkPool) connErrorMonitor() {
	for {
		errCheck := <-p.errCheckChan
		errStr := errCheck.err.Error()
		if strings.Contains(errStr, ERROR_CONN_READ_CLOSE) || errStr == ERROR_CONN_CLOSED || errStr == ERROR_CONN_BROKEN || strings.Contains(errStr, ERROR_CONN_RESET) || strings.Contains(errStr, ERROR_CONN_BROKEN) {
			select {
			case <-p.exitStage:
				return
			default:
				errCheck.br.rebuildConn()
			}
		} else {
			logger.GetLogger("WARN").Printf("Broker client get error: %v", errCheck.err)
		}
	}
}

func (p *SafeBrokerkPool) Pub(queueName string, data []byte, priority uint32, delay, ttr uint64) (jobId uint64, err error) {
	br := p.getOneBroker()
	if br == nil {
		err = errors.New("no broker avaiable")
		return
	}
	br.transLocker.Lock()
	defer func() {
		br.transLocker.Unlock()
		if err != nil {
			p.notifyBrokerErr(br, err)
		}
	}()

	jobId, err = br.Pub(queueName, priority, delay, ttr, data)
	return
}

func (p *SafeBrokerkPool) ListTopics() (topics map[string][]string, err error) {
	var brTopics []string
	topics = map[string][]string{}
	errs := []string{}
	for _, br := range p.poolAvailable {
		if _, ok := topics[br.addr]; ok {
			continue
		}
		brTopics, err = br.ListTopics()
		if err != nil {
			p.notifyBrokerErr(br, err)
			errs = append(errs, err.Error())
		} else {
			topics[br.addr] = brTopics
		}

	}
	if len(errs) > 0 {
		err = fmt.Errorf("List topics err: %v", errs)
	}
	return
}

// StatsTopic gets the topic stats on all endpoints.
func (p *SafeBrokerkPool) StatsTopic(topicName string) (stats map[string]map[string]interface{}, err error) {
	var brStats map[string]interface{}
	stats = map[string]map[string]interface{}{}
	errs := []string{}
	for _, br := range p.poolAvailable {
		if _, ok := stats[br.addr]; ok {
			continue
		}
		brStats, err = br.StatsTopic(topicName)
		if err != nil {
			if strings.Contains(err.Error(), "NOT_FOUND") {
				err = nil
				continue
			}
			p.notifyBrokerErr(br, err)
			errs = append(errs, err.Error())
		} else {
			stats[br.addr] = brStats
		}

	}
	if len(errs) > 0 {
		err = fmt.Errorf("Stats topic err: %v", errs)
	}
	return
}

func (p *SafeBrokerkPool) Release(msg *Msg, priority uint32, delay uint64) (err error) {
	br := p.getOneBrokerByAddr(msg.QueueServer)
	defer func() {
		if err != nil {
			p.notifyBrokerErr(br, err)
		}
	}()
	err = br.Release(msg.Id, priority, delay)
	return
}

func (p *SafeBrokerkPool) KickJob(quename string, msg *Msg) (err error) {
	br := p.getOneBrokerByAddr(msg.QueueServer)
	defer func() {
		if err != nil {
			p.notifyBrokerErr(br, err)
		}
	}()
	err = br.KickJob(msg.Id)
	return
}

func (p *SafeBrokerkPool) Close(force bool) {
	p.closeLock.Lock()
	defer p.closeLock.Unlock()
	select {
	case <-p.exitStage:
		// already closed.
		return
	default:
		close(p.exitStage)
	}
	for _, br := range p.poolAvailable {
		if force {
			br.ForceClose()
		} else {
			br.Close()
		}
	}
}

func (p *SafeBrokerkPool) UnWatch(topicName string) (err error) {
	errs := []string{}
	for _, br := range p.poolAvailable {
		err = br.UnWatch(topicName)
		if err != nil {
			p.notifyBrokerErr(br, err)
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		err = errors.New(fmt.Sprintf("UnWatch err: %v", errs))
	}
	return
}

func (p *SafeBrokerkPool) Watch(topicName string) (err error) {
	errs := []string{}
	for _, br := range p.poolAvailable {
		err = br.Watch(topicName)
		if err != nil {
			p.notifyBrokerErr(br, err)
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		err = fmt.Errorf("Watch err: %v", errs)
	}
	return
}

func (p *SafeBrokerkPool) StopReserve() {
	if p.reserveStopChan == nil {
		return
	}
	close(p.reserveStopChan)
}

func (p *SafeBrokerkPool) shouldStopReseve() bool {
	select {
	case <-p.exitStage:
		return true
	case <-p.reserveStopChan:
		return true
	default:
		return false
	}
}

// Reserve messages from all beanstalkd servers.
// This function can be called only once.
//
// IMPORTANT: after this func is called, no further commands should be invoked, otherwise the caller goroutines maybe blocked. please consider using new pool instances for other commands.
func (p *SafeBrokerkPool) Reserve() (msgChan chan *Msg) {
	if !atomic.CompareAndSwapInt32(&p.reserveCalled, 0, 1) {
		return
	}
	// TODO: may has data race with StopReserve method.
	p.reserveStopChan = make(chan bool, 1)
	msgChan = make(chan *Msg)
	reserveLoop := func(br *Broker) {
		defer func() {
			errI := recover()
			if errI != nil {
				logger.GetLogger("ERROR").Printf("Server routine error: %v", errI)
			}
		}()
		for {
			if p.shouldStopReseve() {
				return
			}
			id, body, err := br.Reserve()
			if err == nil {
				if p.shouldStopReseve() {
					br.Release(id, beanstalkc.DEFAULT_MSG_PRIORITY, 0)
					return
				}
				// put into buried state instantly for later processing by other connections.
				err = br.Bury(id)
				if err == nil {
					msgChan <- &Msg{
						Id:          id,
						Body:        body,
						QueueServer: br.addr,
					}
				}
			}
			if err != nil {
				p.notifyBrokerErr(br, err)
				time.Sleep(time.Second * 3)
			}
		}
	}
	for _, br := range p.poolAvailable {
		go reserveLoop(br)
	}
	return msgChan
}

func (p *SafeBrokerkPool) Stats() (stats map[string]map[string]interface{}, err error) {
	var brStats map[string]interface{}
	stats = map[string]map[string]interface{}{}
	errs := []string{}
	for _, br := range p.poolAvailable {
		if _, ok := stats[br.addr]; ok {
			continue
		}
		brStats, err = br.Stats()
		if err != nil {
			p.notifyBrokerErr(br, err)
			errs = append(errs, err.Error())
		} else {
			stats[br.addr] = brStats
		}

	}
	if len(errs) > 0 {
		err = errors.New(fmt.Sprintf("Stats err: %v", errs))
	}
	return
}

func (p *SafeBrokerkPool) StatsJob(msg *Msg) (stats map[string]interface{}, err error) {
	br := p.getOneBrokerByAddr(msg.QueueServer)
	defer func() {
		if err != nil {
			p.notifyBrokerErr(br, err)
		}
	}()
	if br == nil {
		return nil, errors.New("no brokers with addr: " + msg.QueueServer)
	}
	stats, err = br.StatsJob(msg.Id)
	return
}

func (p *SafeBrokerkPool) Delete(msg *Msg) (err error) {
	br := p.getOneBrokerByAddr(msg.QueueServer)
	defer func() {
		if err != nil {
			p.notifyBrokerErr(br, err)
		}
	}()
	err = br.Delete(msg.Id)
	return
}

func (p *SafeBrokerkPool) Bury(msg *Msg) (err error) {
	br := p.getOneBrokerByAddr(msg.QueueServer)
	defer func() {
		if err != nil {
			p.notifyBrokerErr(br, err)
		}
	}()
	err = br.Bury(msg.Id)
	return
}

func (p *SafeBrokerkPool) Peek(msg *Msg) (jobData []byte, err error) {
	br := p.getOneBrokerByAddr(msg.QueueServer)
	defer func() {
		if err != nil {
			p.notifyBrokerErr(br, err)
		}
	}()
	jobData, err = br.Peek(msg.Id)
	return
}
