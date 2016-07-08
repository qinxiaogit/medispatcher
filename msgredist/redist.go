package msgredist

import (
	"medispatcher/broker"
	"medispatcher/config"
	"medispatcher/data"
	"medispatcher/logger"
	"time"
	"runtime/debug"
)

// StartAndWait starts the redispatch process until Stop is called.
func StartAndWait() {
	var routineChans []*chan int8
	for i := uint16(0); i < config.GetConfig().ListenersOfMainQueue; i++ {
		sigChan := make(chan int8, 1)
		routineChans = append(routineChans, &sigChan)
		go redistMainQueue(&sigChan)
	}
	// wait for all worker routines to exit.
	for _, ch := range routineChans {
		<-*ch
	}
	for !shouldExit() {
		time.Sleep(time.Millisecond * 30)
	}
	<-exitChan
}

func redistMainQueue(sigChan *chan int8) {
	var brokerConnected bool
	var br broker.Broker
	var err error
	defer func() {
		err := recover()
		if err != nil {
			logger.GetLogger("ERROR").Printf("msgredist routine exited abnormally: %v: %s", err, debug.Stack())
			*sigChan <- 1
		}
	}()

	for !shouldExit() {
		if !brokerConnected {
			br,err = broker.GetBrokerWitBlock(INTERVAL_OF_RETRY_ON_CONN_FAIL, shouldExit)
			if err != nil {
				continue
			}
			brokerConnected = true
			// ensure successful watching
			for {
				err = br.Watch(config.GetConfig().NameOfMainQueue)
				if err != nil {
					logger.GetLogger("WARN").Printf("Failed to watch main queue: %v", err)
					switch err.Error() {
					case broker.ERROR_CONN_CLOSED, broker.ERROR_CONN_BROKEN:
						brokerConnected = false
					}
				} else {
					break
				}

				// if disconnected then break the loop watch.
				if !brokerConnected {
					break
				}
			}

			// disconnected during watching, then retry connecting.
			if !brokerConnected {
				continue
			} else {
				time.Sleep(time.Second * INTERVAL_OF_RETRY_ON_CONN_FAIL)
			}
		}
		// need to use ReserveWithTimeout rather than Reserve, in order to have chance to take care of the exit signal.
		jobId, jobBody, err := br.ReserveWithTimeout(DEFAULT_RESERVE_TIMEOUT)
		if err == nil {
			var stats map[string]interface{}
			// put message to subscription channel queues.
			var msg data.MessageStuct
			msg, err = data.UnserializeMessage(jobBody)
			if err != nil {
				// in this case, do not try to distribute it again, just bury it for further manual investigation.
				logger.GetLogger("WARN").Printf("Failed to unserialize message: %v : %v", jobId, err)
				br.Bury(jobId)
				continue
			} else {
				msg.OriginJobId = jobId
				jobBody, err = data.SerializeMessage(msg)
				if err != nil {
					br.Bury(jobId)
					logger.GetLogger("WARN").Printf("Failed to re-serialize message: %v. Buried!", msg.MsgKey, err)
					continue
				}
			}
			var subscriptions []data.SubscriptionRecord
			subscriptions, err = data.GetSubscriptionsByTopicWithCache(msg.MsgKey)
			if err != nil {
				stats, err = br.StatsJob(jobId)
				if err != nil {
					logger.GetLogger("WARN").Printf("Failed to stats job when releasing the job: %v. waits for trr and try again later.", err)
					continue
				}
				broker.NormalizeJobStats(&stats)
				br.Release(jobId, stats["pri"].(uint32), stats["delay"].(uint64))
				logger.GetLogger("WARN").Printf("Failed to get subscription information for message: %v. Try again later!", msg.MsgKey, err)
				time.Sleep(time.Second * DELAY_OF_RE_DISTRIBUTE_MESSAGE_ON_FAILURE)
				continue
			} else {
				for _, sub := range subscriptions {
					subChannel := config.GetChannelName(msg.MsgKey, sub.Subscription_id)
					err = br.Use(subChannel)
					if err == nil {
						_, err = br.Pub(broker.DEFAULT_MSG_PRIORITY, 0, broker.DEFAULT_MSG_TTR, jobBody)
					}
					if err != nil {
						logger.GetLogger("WARN").Printf("Failed to redispatch message:[%v] to channel [%v] : %v", msg.MsgKey, subChannel, err)
						logger.GetLogger("DATA").Printf("REDISTFAIL %v %v %v", jobId, sub.Subscription_id, jobBody)
					}
				}

				// ensure deleted of job
				jobDeleted := false
				for {
					if jobDeleted {
						break
					}
					err = br.Delete(jobId)
					if err == nil {
						jobDeleted = true
					} else {
						switch err.Error() {
						case broker.ERROR_JOB_NOT_FOUND:
							jobDeleted = true
						// TODO: unknown reason.
						case "RESERVED", "TIMED_OUT":
							logger.GetLogger("WARN").Printf("Deleting  of job[%v] may failed! %v", jobId, err)
							time.Sleep(time.Second * (INTERVAL_OF_RETRY_ON_CONN_FAIL * 2))
						case broker.ERROR_CONN_CLOSED, broker.ERROR_CONN_BROKEN:
							// retrieve a usable broker.
							// if br is nil means the medispatcher service should exit,
							// so the deleting is to fail, then fallthrough to log the failure.
							br, err = broker.GetBrokerWitBlock(INTERVAL_OF_RETRY_ON_CONN_FAIL, shouldExit)
							// in exiting stage
							if err != nil{
								logger.GetLogger("WARN").Printf("Job may not be deleted : in exiting stage.%v", jobId)
								jobDeleted = true
								brokerConnected = false
							}
							fallthrough
						default:
							logger.GetLogger("WARN").Printf("Failed to delete job: %v : %v", err, jobId)
							time.Sleep(time.Second * (INTERVAL_OF_RETRY_ON_CONN_FAIL * 2))
						}
					}
				}
			}
		} else {
			if err.Error() == broker.ERROR_CONN_CLOSED || err.Error() == broker.ERROR_CONN_BROKEN{
				logger.GetLogger("WARN").Printf("Connection lost when waiting for message from queue server: %v. Retry later.", err)
				brokerConnected = false
			} else if err.Error() != broker.ERROR_JOB_RESERVE_TIMEOUT{
				logger.GetLogger("WARN").Printf("Unexpected error when waiting for message from queue server: %v ", err)
			}
		}
	}
	if brokerConnected {
		br.Close()
	}
	*sigChan <- 1
}
