package msgredist

import (
	"medispatcher/broker"
	"medispatcher/broker/beanstalk"
	"medispatcher/config"
	"medispatcher/data"
	"medispatcher/logger"
	"medispatcher/sender"
	"runtime/debug"
	"sync"
	"time"
)

// StartAndWait starts the redispatch process until Stop is called.
func StartAndWait() {
	exitWg.Add(1)
	var err error
	var brListenPool *beanstalk.SafeBrokerkPool
	var brCmdPool *beanstalk.SafeBrokerkPool
	defer func() {
		err := recover()
		if err != nil {
			logger.GetLogger("ERROR").Printf("msgredist routine exited abnormally: %v: %s", err, debug.Stack())
		}
		if brListenPool != nil {
			brListenPool.Close(true)
		}
		if brCmdPool != nil {
			brCmdPool.Close(false)
		}
		exitWg.Done()
	}()
	brListenPool = broker.GetBrokerPoolWithBlock(uint32(config.GetConfig().ListenersOfMainQueue), INTERVAL_OF_RETRY_ON_CONN_FAIL, shouldExit)
	if brListenPool == nil {
		return
	}

	brCmdPool = broker.GetBrokerPoolWithBlock(uint32(config.GetConfig().ListenersOfMainQueue), INTERVAL_OF_RETRY_ON_CONN_FAIL, shouldExit)
	if brCmdPool == nil {
		return
	}

	for !shouldExit() {
		err = brListenPool.Watch(config.GetConfig().NameOfMainQueue)
		if err != nil {
			logger.GetLogger("WARN").Printf("Watch main queue error: %v", err)
			time.Sleep(time.Second * INTERVAL_OF_RETRY_ON_CONN_FAIL)
		} else {
			break
		}
	}
	if shouldExit() {
		return
	}

	workerWg := new(sync.WaitGroup)
	msgChan := brListenPool.Reserve()
	workerRun := func() {
		defer func() {
			workerWg.Done()
		}()

		var reserveTimeoutTimer *time.Timer
		var jobBody []byte
		var err error
		for {
			var msgR *beanstalk.Msg
			select {
			case <-exitChan:
				// close to stop reserving more messages from the ready queue.
				brListenPool.Close(true)
				// with timeInterval timed out, until stop signal received and all messages in the channel have been re-distributed.
				// ensure all messages that reserved from the queue to the channel have been re-distributed.
				if reserveTimeoutTimer == nil {
					reserveTimeoutTimer = time.NewTimer(time.Second * DEFAULT_RESERVE_TIMEOUT)
				} else {
					reserveTimeoutTimer.Reset(time.Second * DEFAULT_RESERVE_TIMEOUT)
				}

				select {
				case <-reserveTimeoutTimer.C:
					return

				case msgR = <-msgChan:
				}
			case msgR = <-msgChan:

			}

			// put message to subscription channel queues.
			var msg *data.MessageStuct
			msg, err = data.UnserializeMessage(msgR.Body)
			if err != nil {
				logger.GetLogger("WARN").Printf("Failed to decode msg[%v] body: %v", msgR.Id, err)
				logger.GetLogger("DATA").Printf("DECODERR %s", msgR.Body)
				err = brCmdPool.Delete(msgR)
				if err != nil {
					logger.GetLogger("WARN").Printf("Failed to delete broken msg[%v]: %v", msgR.Id, err)
				}
				continue
			} else {
				msg.OriginJobId = msgR.Id
				jobBody, err = data.SerializeMessage(*msg)
				if err != nil {
					logger.GetLogger("WARN").Printf("Failed to re-serialize message: %v:ERR %v", msg.MsgKey, err)
					continue
				}
			}
			var subscriptions []data.SubscriptionRecord
			for {
				subscriptions, err = data.GetSubscriptionsByTopicWithCache(msg.MsgKey)
				if err != nil {
					logger.GetLogger("WARN").Printf("Failed to get subscription information for message: %v. Try again later!:ERR %v", msg.MsgKey, err)
					time.Sleep(time.Second * DELAY_OF_RE_DISTRIBUTE_MESSAGE_ON_FAILURE)
				} else {
					break
				}
			}

			for _, sub := range subscriptions {
				// 如果当前medis实例运行在bench模式，那么需要检查订阅者是否订阅压测消息.
				if config.GetConfig().RunAtBench {
					// 订阅者不接受bench环境的消息.
					if !sender.ReceiveBenchMsgs(sub.Subscription_id) {
						logger.GetLogger("INFO").Printf("Ignore the bench environment message %v %v", msg.MsgKey, sub.Subscription_id)
						continue
					}
				}
				// 检查当前队列的长度，是否可以丢弃消息
				subParams := sender.NewSubscriptionParams()
				err = subParams.Load(sub.Subscription_id)
				if err != nil {
					logger.GetLogger("WARN").Printf("Failed to load subscription[%v] params: %v", sub.Subscription_id, err)
					continue
				}

				topicStats := sender.GetTopicStats()
				if stat, ok := topicStats.Stats[sub.Subscription_id]; ok {
					// 直接丢弃消息，不发送到订阅队列
					if subParams.DropMessageThreshold > 0 && stat.BlockedMessageCount > subParams.DropMessageThreshold {
						logger.GetLogger("WARN").Printf("drop message %v %v", msg.MsgKey, sub.Subscription_id)
						continue
					}
				}

				subChannel := config.GetChannelName(msg.MsgKey, sub.Subscription_id)
				_, err = brCmdPool.Pub(subChannel, jobBody, broker.DEFAULT_MSG_PRIORITY, 0, broker.DEFAULT_MSG_TTR)
				if err != nil {
					logger.GetLogger("WARN").Printf("Failed to redispatch message:[%v] to channel [%v] : %v", msg.MsgKey, subChannel, err)
					logger.GetLogger("DATA").Printf("REDISTFAIL %v %v %v", msg.MsgKey, sub.Subscription_id, jobBody)
				}
				if err != nil {
					logger.GetLogger("WARN").Printf("Failed to delete distributed msg[%v]: %v", msgR.Id, err)
				}
			}

			// ensure deleted of job
			jobDeleted := false
			for !jobDeleted {
				err = brCmdPool.Delete(msgR)
				if err == nil {
					jobDeleted = true
				} else {
					switch err.Error() {
					case broker.ERROR_JOB_NOT_FOUND:
						jobDeleted = true
					default:
						logger.GetLogger("WARN").Printf("Failed to delete job: [%v] [%v] : %v", msg.MsgKey, msgR.Id, err)
						time.Sleep(time.Second * (INTERVAL_OF_RETRY_ON_CONN_FAIL * 2))
					}
				}
			}
		}
	}
	for n := config.GetConfig().ListenersOfMainQueue * 5; n > 0; n-- {
		workerWg.Add(1)
		go workerRun()
	}
	workerWg.Wait()
}
