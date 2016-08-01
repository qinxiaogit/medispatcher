package sender

import (
	"medispatcher/broker"
	"medispatcher/broker/beanstalk"
	"medispatcher/config"
	"medispatcher/logger"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type brokerPools struct {
	sync.RWMutex
	pools map[string]*beanstalk.SafeBrokerkPool
}

func (bp *brokerPools) setPool(name string, pool *beanstalk.SafeBrokerkPool) *beanstalk.SafeBrokerkPool {
	bp.Lock()
	defer bp.Unlock()
	if _, exists := bp.pools[name]; exists {
		pool.Close(true)
		return bp.pools[name]
	}
	bp.pools[name] = pool
	return pool
}

// create if not exists , despite of exiting stage.
func (bp *brokerPools) getPool(name string) *beanstalk.SafeBrokerkPool {
	bp.RLock()
	if _, exists := bp.pools[name]; !exists {
		bp.RUnlock()
		bp.Lock()
		pool := broker.GetBrokerPoolWithBlock(config.GetConfig().QueueServerPoolCmdConnCount, 3, func() bool { return false })
		bp.pools[name] = pool
		bp.Unlock()
		return pool
	}
	p := bp.pools[name]
	bp.RUnlock()
	return p
}

func (bp *brokerPools) removePool(name string) {
	bp.Lock()
	defer bp.Unlock()
	bp.pools[name].Close(true)
	delete(bp.pools, name)
}

func (bp *brokerPools) len() int {
	bp.RLock()
	defer bp.RUnlock()
	return len(bp.pools)
}

func (bp *brokerPools) Close() {
	bp.Lock()
	defer bp.Unlock()
	for n, p := range bp.pools {
		p.Close(true)
		delete(bp.pools, n)
	}
}

var (
	brokerCmdPools    = &brokerPools{pools: map[string]*beanstalk.SafeBrokerkPool{}}
	brokerListenPools = &brokerPools{pools: map[string]*beanstalk.SafeBrokerkPool{}}

	// jobDeleteChan carries job IDs that to be deleted.
	jobDeleteChan = make(chan *Msg)
)

type Msg struct {
	*beanstalk.Msg
}

//
// nil returned if it's in exiting stage
func newMessasgeListener(queueName string, subParam *SubscriptionParams, exitSigChan chan bool) (msgChan chan *Msg) {
	msgChan = make(chan *Msg)
	go func() {
		defer func() {
			errI := recover()
			if errI != nil {
				logger.GetLogger("ERROR").Printf("Listenner for %v exited abnormally: %s....%s", queueName, errI, debug.Stack())
			}
			procExitWG.Done()
		}()
		var (
			err error
			// jobStats map[string]interface{}
			brMsg *beanstalk.Msg
			st    time.Time
		)

		brReservePool := broker.GetBrokerPoolWithBlock(config.GetConfig().QueueServerPoolListenConnCount, 3, shouldExit)
		if brReservePool == nil {
			return
		}
		brReservePool = brokerListenPools.setPool(queueName, brReservePool)

		// loop until watching successful
		for !shouldExit() {
			err = brReservePool.Watch(queueName)
			if err != nil {
				time.Sleep(time.Second * 3)
				logger.GetLogger("WARN").Println(err)
				continue
			} else {
				break
			}

		}

		brMsgChan := brReservePool.Reserve()
		var reserveTimeoutTimer *time.Timer
		for {
			st = time.Now()
			brMsg = nil
			select {
			case <-exitSigChan:
				if reserveTimeoutTimer == nil {
					// exit stage, close the brokers to make no more reserved messages.
					brReservePool.Close(true)
					reserveTimeoutTimer = time.NewTimer(time.Second * DEFAULT_RESERVE_TIMEOUT)
				} else {
					reserveTimeoutTimer.Reset(time.Second * DEFAULT_RESERVE_TIMEOUT)
				}
				// exit stage, wait for all reserved messages to be sent.
				select {
				case <-reserveTimeoutTimer.C:
					return
				case brMsg = <-brMsgChan:
				}
			case brMsg = <-brMsgChan:
			}

			if brMsg == nil {
				continue
			}
			msg := &Msg{Msg: brMsg}
			// jobStats, err = brokerCmdPools.getPool(queueName).StatsJob(brMsg)
			// if err != nil {
			// 	logger.GetLogger("WARN").Printf("Failed to stats job[%v] on channel: %v ERR: %v", brMsg.Id, queueName, err)
			// 	continue
			// }
			// broker.NormalizeJobStats(&jobStats)
			// b, _ := json.Marshal(jobStats)
			// json.Unmarshal(b, &(brMsg.Stats))
			msg.Msg.Stats.QueueName = queueName
			msg.Msg.Stats.Delay = 0
			msg.Msg.Stats.Pri = broker.DEFAULT_MSG_PRIORITY
			msg.Msg.Stats.TTR = broker.DEFAULT_MSG_TTR
			msgChan <- msg
			tn := time.Now()
			elapsed := tn.Sub(st)
			minDu := time.Millisecond * time.Duration(subParam.IntervalOfSending)
			if elapsed < minDu {
				<-time.After(minDu - elapsed)
			}
		}
	}()
	return
}

func listenAndDeleteMessage() {
	curWorkerNum := int64(0)
	exitSigChan := make(chan bool)
	spawnSigChan := make(chan int64)
	procExitWG.Add(1)
	defer func() {
		procExitWG.Done()
	}()

	workRun := func() {
		// wait for deleting job complete.
		procExitWG.Add(1)
		atomic.AddInt64(&curWorkerNum, 1)
		defer func() {
			atomic.AddInt64(&curWorkerNum, -1)
			procExitWG.Done()
		}()
		var (
			msg *Msg
		)
		for {
			var err error
			select {
			case <-exitSigChan:
				return
			case msg = <-jobDeleteChan:
				err = brokerCmdPools.getPool(msg.Stats.QueueName).Delete(msg.Msg)
				if err != nil {
					//TODO: deal with not deleted job ids.
					logger.GetLogger("WARN").Printf("Failed to delete job[%v] ERR: %v", msg.Id, err)
					logger.GetLogger("DATA").Printf("DELFAIL %v %v %v", msg.QueueServer, msg.Id, string(msg.Body))
				}
			}
		}
	}

	var leastWorkNum int64 = 10
	for n := leastWorkNum; n > 0; n-- {
		go workRun()
	}

	// coordinate cmd broker pool connections.
	go func() {
		for {
			if atomic.LoadInt64(&curWorkerNum) > leastWorkNum {
				spawnSigChan <- int64(brokerCmdPools.len())*int64(config.GetConfig().QueueServerPoolCmdConnCount) - atomic.LoadInt64(&curWorkerNum)
			}
			time.Sleep(time.Millisecond * 50)
		}
	}()
	// spawn or reclaim the worker routine.
	go func() {
		for {
			n := <-spawnSigChan
			if n < 0 {
				for ; n < 0; n++ {
					exitSigChan <- true
				}
			} else if n > 0 {
				for ; n > 0; n-- {
					go workRun()
				}
			}
		}
	}()

	// wait for exit procedure.
	<-exitChan

	// stop the redundant workers.
	for n := leastWorkNum; n > 0; n-- {
		exitSigChan <- true
	}

	var reserveTimeoutTimer *time.Timer
	// cleanup all undeleted messages after the messge deleting workers exited.
CLEANUP_DEL:
	for {
		for {
			if reserveTimeoutTimer == nil {
				reserveTimeoutTimer = time.NewTimer(time.Second * DEFAULT_RESERVE_TIMEOUT)
			} else {
				reserveTimeoutTimer.Reset(time.Second * DEFAULT_RESERVE_TIMEOUT)
			}
			select {
			case <-reserveTimeoutTimer.C:
				// all messages that to be deleted are  deleted
				break CLEANUP_DEL
			case msg := <-jobDeleteChan:
				err := brokerCmdPools.getPool(msg.Stats.QueueName).Delete(msg.Msg)
				if err != nil {
					//TODO: deal with not deleted job ids.
					logger.GetLogger("WARN").Printf("Failed to delete job[%v] ERR: %v", msg.Id, err)
					logger.GetLogger("DATA").Printf("DELFAIL %v %v %v", msg.QueueServer, msg.Id, string(msg.Body))
				}
			}
		}
	}
}

func deleteMessage(msg *Msg) {
	jobDeleteChan <- msg
}
