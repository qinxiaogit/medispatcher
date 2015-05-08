// TODO: When subscription is canceled.
package sender

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"medispatcher/broker"
	"medispatcher/config"
	"medispatcher/data"
	"medispatcher/logger"
	httproxy "medispatcher/transproxy/http"
	"net/url"
	"strconv"
	"time"
)

// StartAndWait starts the recover process until Stop is called.
func StartAndWait() {
	for !shouldExit() {
		subscriptions, err := data.GetAllSubscriptionsWithCache()
		if err != nil {
			logger.GetLogger("WARN").Printf("Failed to get subscriptions: %v", err)
		} else {
			getSenderRoutineStatsRWLock()
			for _, sub := range subscriptions {
				_, exists := senderRoutineStatus[sub.Subscription_id]
				if !exists {
					go handleSubscription(sub)
				}
			}
			releaseSenderRoutineStatsRWLock()
		}
		time.Sleep(time.Second * 1)
	}
	// exit
	for _, status := range senderRoutineStatus {
		(*status).sigChan <- SENDER_ROUTINE_SIG_EXIT_ALL_ROUTINES
	}
	for {
		allExited := true
		for _, status := range senderRoutineStatus {
			if !status.Exited() {
				allExited = false
			}
		}
		if allExited {
			break
		} else {
			time.Sleep(time.Millisecond * 20)
		}
	}
	<-exitChan
}

// handel a subscription.
func handleSubscription(sub data.SubscriptionRecord) {

	sossr := StatusOfSubSenderRoutine{
		subscription:   &sub,
		coCount:        0,
		coCountOfRetry: 0,
		sigChan:        make(chan SubSenderRoutineChanSig, 1),
	}

	senderRoutineSigChans := []*chan SubSenderRoutineChanSig{}
	senderRoutineOfRetrySigChans := []*chan SubSenderRoutineChanSig{}
	for i := uint16(0); i < config.GetConfig().SendersPerChannel; i++ {
		ch := make(chan SubSenderRoutineChanSig, 1)
		go sendSubscription(sub, &ch)
		senderRoutineSigChans = append(senderRoutineSigChans, &ch)
		sossr.coCount += int32(1)
	}

	for i := uint16(0); i < config.GetConfig().SendersPerRetryChannel; i++ {
		ch := make(chan SubSenderRoutineChanSig, 1)
		go sendSubscriptionAsRetry(sub, &ch)
		senderRoutineOfRetrySigChans = append(senderRoutineOfRetrySigChans, &ch)
		sossr.coCountOfRetry += int32(1)
	}
	getSenderRoutineStatsRWLock()
	senderRoutineStatus[sub.Subscription_id] = &sossr
	releaseSenderRoutineStatsRWLock()
	// wait for management signals.
	for {
		sig := <-sossr.sigChan
		switch sig {
		case SENDER_ROUTINE_SIG_INCREASE_ROUTINE:
			ch := make(chan SubSenderRoutineChanSig)
			senderRoutineSigChans = append(senderRoutineSigChans, &ch)
			go sendSubscription(sub, &ch)
			sossr.IncreaseCoCount(1)
		case SENDER_ROUTINE_SIG_DECREASE_ROUTINE:
			if len(senderRoutineSigChans) > 0 {
				*senderRoutineSigChans[0] <- SENDER_ROUTINE_SIG_EXIT
				<-*senderRoutineSigChans[0]
				if len(senderRoutineSigChans) > 1 {
					senderRoutineSigChans = senderRoutineSigChans[1:]
				} else {
					senderRoutineSigChans = nil
				}
				sossr.DecreaseCoCount(1)
			}
		case SENDER_ROUTINE_SIG_EXIT_ALL_ROUTINES:
			allSigChans := append(senderRoutineSigChans, senderRoutineOfRetrySigChans...)
			// test routines which has exited abnormally.
			var tempChans []*chan SubSenderRoutineChanSig
			for _, ch := range allSigChans {
				select {
				case sig := <-*ch:
					if sig != SENDER_ROUTINE_SIG_EXITED {
						tempChans = append(tempChans, ch)
					}
				default:
					tempChans = append(tempChans, ch)
				}
			}
			allSigChans = tempChans

			// waits for all routines to exit.
			for _, ch := range allSigChans {
				*ch <- SENDER_ROUTINE_SIG_EXIT
			}
			coLen := len(allSigChans)
			exitedCo := 0
			exitedM := map[int]bool{}
			for exitedCo < coLen {
				for index, ch := range allSigChans {
					if _, exists := exitedM[index]; exists {
						continue
					}
					select {
					case *ch <- 0:
						exitedCo += 1
						exitedM[index] = true
					default:
					}
				}
				time.Sleep(time.Microsecond * 100)
			}

			sossr.SetCoCount(0)
			sossr.SetCoCountOfRetry(0)
			sossr.SetExited()
		case SENDER_ROUTINE_SIG_DECREASE_ROUTINE_FOR_RETRY:
			if len(senderRoutineSigChans) > 0 {
				*senderRoutineOfRetrySigChans[0] <- SENDER_ROUTINE_SIG_EXIT
				<-*senderRoutineSigChans[0]
				if len(senderRoutineOfRetrySigChans) > 1 {
					senderRoutineOfRetrySigChans = senderRoutineOfRetrySigChans[1:]
				} else {
					senderRoutineOfRetrySigChans = nil
				}
				sossr.DecreaseCoCountOfRetry(1)
			}
		case SENDER_ROUTINE_SIG_INCREASE_ROUTINE_FOR_RETRY:
			ch := make(chan SubSenderRoutineChanSig)
			senderRoutineOfRetrySigChans = append(senderRoutineOfRetrySigChans, &ch)
			go sendSubscriptionAsRetry(sub, &ch)
			sossr.InCoCountOfRetry(1)
		}
	}
}

func sendSubscription(sub data.SubscriptionRecord, ch *chan SubSenderRoutineChanSig) {
	defer func() {
		err := recover()
		if err != nil {
			logger.GetLogger("ERROR").Printf("sender routine exiting abnormally: %v", err)
			*ch <- SENDER_ROUTINE_SIG_EXITED
		}
	}()
	var (
		br                     broker.Broker
		brPt                   *broker.Broker
		err                    error
		jobId, logId           uint64
		jobBody                []byte
		httpStatusCode         int
		returnData             []byte
		sentSuccess            bool
		respd, jobStats        map[string]interface{}
		errMsgInSending        string
		timerOfSendingInterval time.Time
	)
	queueName := config.GetChannelName(sub.Class_key, sub.Subscription_id)
	for {
		select {
		case sig := <-*ch:
			if sig == SENDER_ROUTINE_SIG_EXIT {
				if brPt != nil {
					br.Close()
				}
				return
			}
		default:
			timerOfSendingInterval = time.Now()
			if brPt == nil {
				brPt = broker.GetBrokerWitBlock(INTERVAL_OF_RETRY_ON_CONN_FAIL, shouldExit)
				if brPt != nil {
					br = *brPt
					err = br.Watch(queueName)
					if err != nil {
						logger.GetLogger("WARN").Printf("Failed to watch sub channel queue: %v : %v", queueName, err)
						switch err.Error() {
						case broker.ERROR_CONN_CLOSED:
							brPt = nil
						}
					} else {
						time.Sleep(time.Second * INTERVAL_OF_RETRY_ON_CONN_FAIL)
						continue
					}
				}
			}
			if brPt == nil {
				continue
			}
			jobId, jobBody, err = br.ReserveWithTimeout(DEFAULT_RESERVE_TIMEOUT)
			if err != nil {
				if err.Error() != broker.ERROR_JOB_RESERVE_TIMEOUT {
					logger.GetLogger("WARN").Printf("Failed to reserve job from sub channel queue: %v : %v", queueName, err)
				}
				if err.Error() == broker.ERROR_CONN_CLOSED {
					brPt = nil
					continue
				}

			} else {
				msg, err := data.UnserializeMessage(jobBody)
				if err != nil {
					logger.GetLogger("WARN").Printf("Failed to decode msg body for job[%v] when send to subscriber: %v", jobId, err)
					br.Bury(jobId)
				} else {
					sentSuccess = false
					st := time.Now()
					httpStatusCode, returnData, err = transferSubscriptionViaHttp(&msg, &sub, 0)
					et := time.Now()
					if httpStatusCode == 200 {
						err = json.Unmarshal(returnData, &respd)
						if err != nil {
							logger.GetLogger("WARN").Printf("Subscriber did not response a valid data: %v", err)
						} else {
							reStatus, exists := respd["status"]
							if !exists {
								logger.GetLogger("WARN").Print("Subscriber's response didnot contains the \"status\" field.")
							} else {
								sentSuccess = fmt.Sprintf("%v", reStatus) == "1"
							}
						}
					}
					if sentSuccess {
						br.Delete(jobId)
					} else {
						// logging failure.
						if httpStatusCode == 0 {
							errMsgInSending = fmt.Sprintf("Code: 0\nContent:\n\tFailed to get response: %v", err)
						} else if httpStatusCode == -1 {
							// encode body failed
							errMsgInSending = fmt.Sprintf("not send. failed to encode message body.\nerr:\n\t%v", err)
						} else if httpStatusCode == -2 {
							errMsgInSending = fmt.Sprintf("not send, failed to parse url. \nContent:\n\t%s", sub.Reception_channel)
						} else {
							errMsgInSending = fmt.Sprintf("Code: %v\nContent:\n\t%s", httpStatusCode, returnData)
						}
						logId, err = data.LogFailure(msg, sub, errMsgInSending, 0, genUniqueJobId(msg.Time, msg.OriginJobId, sub.Subscription_id))
						if err != nil {
							logger.GetLogger("WARN").Printf("Failed to log failure: %v", err)
						} else {
							msg.LogId = logId
							err = data.SetFinalStatusOfFailureLog(logId, 0, 1, 0)
							if err != nil {
								logger.GetLogger("WARN").Print(err)
							}
						}

						jobStats, err = br.StatsJob(jobId)
						broker.NormalizeJobStats(&jobStats)
						// failed stats job or failed to log the job, then try to release it to the normal queue.
						if err != nil || msg.LogId < 1 {
							if err != nil {
								logger.GetLogger("WARN").Printf("Failed to stats job when puting to retry channel: %v", err)
							}
							if msg.LogId < 1 {
								logger.GetLogger("WARN").Print("Failed to log job when puting to retry channel.")
							}

							br.Release(jobId, jobStats["pri"].(uint32), 1)
						} else {
							err = putToRetryChannel(brPt, &sub, &msg, &jobStats)
							if err != nil {
								logger.GetLogger("WARN").Print(err)
								br.Release(jobId, jobStats["pri"].(uint32), 1)
							} else {
								err = br.Delete(jobId)
								if err != nil {
									logger.GetLogger("WARN").Printf("Failed to delete job[%v] from the normal sub queue: %v", jobId, err)
								}
							}
						}
					}
					if config.GetConfig().EnableMsgSentLog {
						logger.GetLogger("DATA").Printf("SENT key:%v result:%v code:%v elapse:%vms dest:%v DATA: %+v",
							msg.MsgKey,
							sentSuccess,
							httpStatusCode,
							fmt.Sprintf("%.3f", float64(et.Sub(st).Nanoseconds())/1e6),
							sub.Reception_channel,
							msg.Body,
						)
					}
					elapsed := time.Now().Sub(timerOfSendingInterval)
					minInterval := time.Millisecond * time.Duration(config.GetConfig().IntervalOfSendingForSendRoutine)
					if elapsed < minInterval {
						time.Sleep(minInterval - elapsed)
					}
				}
			}
		}
	}
}

func sendSubscriptionAsRetry(sub data.SubscriptionRecord, ch *chan SubSenderRoutineChanSig) {
	defer func() {
		err := recover()
		if err != nil {
			logger.GetLogger("ERROR").Printf("sender routine for retry-channel exiting abnormally: %v", err)
			*ch <- SENDER_ROUTINE_SIG_EXITED
		}
	}()
	var (
		br                     broker.Broker
		brPt                   *broker.Broker
		err, sendingErr        error
		jobId                  uint64
		jobBody                []byte
		httpStatusCode         int
		returnData             []byte
		sentSuccess            bool
		sentStatus             uint8
		respd, jobStats        map[string]interface{}
		errMsgInSending        string
		timerOfSendingInterval time.Time
	)
	queueName := config.GetChannelNameForReSend(sub.Class_key, sub.Subscription_id)

	for {
		select {
		case sig := <-*ch:
			if sig == SENDER_ROUTINE_SIG_EXIT {
				if brPt != nil {
					br.Close()
				}
				return
			}
		default:
			timerOfSendingInterval = time.Now()
			if brPt == nil {
				brPt = broker.GetBrokerWitBlock(INTERVAL_OF_RETRY_ON_CONN_FAIL, shouldExit)
				if brPt != nil {
					br = *brPt
					err = br.Watch(queueName)
					if err != nil {
						logger.GetLogger("WARN").Printf("Failed to watch sub channel queue of resend: %v : %v. Retry later.", queueName, err)
						switch err.Error() {
						case broker.ERROR_CONN_CLOSED:
							brPt = nil
						}
					} else {
						time.Sleep(time.Second * INTERVAL_OF_RETRY_ON_CONN_FAIL)
						continue
					}
				}
			}
			if brPt == nil {
				continue
			}
			jobId, jobBody, err = br.ReserveWithTimeout(DEFAULT_RESERVE_TIMEOUT)
			if err != nil {
				if err.Error() != broker.ERROR_JOB_RESERVE_TIMEOUT {
					logger.GetLogger("WARN").Printf("Failed to reserve job from sub channel queue of resend: %v : %v", queueName, err)
				}
				if err.Error() == broker.ERROR_CONN_CLOSED {
					brPt = nil
					continue
				}

			} else {
				msg, err := data.UnserializeMessage(jobBody)
				if err != nil {
					//TODO: when log could not be updated.
					logger.GetLogger("WARN").Printf("Failed to decode msg body for job[%v] when send to subscriber from retry queue: %v", jobId, err)
					br.Bury(jobId)
					continue
				} else {
					msg.RetryTimes += 1
					sentSuccess = false
					st := time.Now()
					httpStatusCode, returnData, sendingErr = transferSubscriptionViaHttp(&msg, &sub, msg.RetryTimes)
					et := time.Now()
					if httpStatusCode == 200 {
						err = json.Unmarshal(returnData, &respd)
						if err != nil {
							logger.GetLogger("WARN").Printf("Subscriber(%s) did not response a valid data on message(%s): %v: %s", sub.Subscriber_id, msg.MsgKey, err, returnData)
						} else {
							reStatus, exists := respd["status"]
							if !exists {
								logger.GetLogger("WARN").Printf("Subscriber(%s)'s response didnot contains the \"status\" field on message().", sub.Subscriber_id, msg.MsgKey)
							} else {
								sentSuccess = fmt.Sprintf("%v", reStatus) == "1"
							}
						}
					}

					if sentSuccess {
						sentStatus = 1
					} else {
						sentStatus = 0
					}
					// Delete it from the queue if success or exceeded the maximum retry times
					if sentSuccess || msg.RetryTimes >= config.GetConfig().MaxRetryTimesOfSendingMessage {
						err = br.Delete(jobId)
						if err != nil {
							logger.GetLogger("WARN").Printf("Failed to delete message that successfully sent from queue: %v", err)
							err = data.SetFinalStatusOfFailureLog(msg.LogId, sentStatus, 1, msg.RetryTimes)
						} else {
							err = data.SetFinalStatusOfFailureLog(msg.LogId, sentStatus, 0, msg.RetryTimes)
						}
						if err != nil {
							logger.GetLogger("WARN").Print(err)
						}
					} else {
						jobStats, err = br.StatsJob(jobId)
						broker.NormalizeJobStats(&jobStats)
						if err != nil {
							logger.GetLogger("WARN").Printf("Failed to stats job when puting to retry channel: %v", err)
							br.Release(jobId, jobStats["pri"].(uint32), 1)
						} else {
							err = putToRetryChannel(brPt, &sub, &msg, &jobStats)
							if err != nil {
								logger.GetLogger("WARN").Print(err)
								br.Release(jobId, jobStats["pri"].(uint32), 1)
							} else {
								err = br.Delete(jobId)
								if err != nil {
									logger.GetLogger("WARN").Printf("Failed to delete job[%v] from the sub queue of resend: %v", jobId, err)
								}
							}
						}

						if httpStatusCode == 0 {
							errMsgInSending = fmt.Sprintf("Code: 0\nContent:\n\tFailed to get response: %v", sendingErr)
						} else if httpStatusCode == -1 {
							// encode body failed
							errMsgInSending = fmt.Sprintf("not send. failed to encode message body.\nerr:\n\t%v", err)
						} else if httpStatusCode == -2 {
							errMsgInSending = fmt.Sprintf("not send, failed to parse url. \nContent:\n\t%s", sub.Reception_channel)
						} else {
							errMsgInSending = fmt.Sprintf("Code: %v\nContent:\n\t%s", httpStatusCode, returnData)
						}
						logId, err := data.LogFailure(msg, sub, errMsgInSending, msg.LogId, genUniqueJobId(msg.Time, msg.OriginJobId, sub.Subscription_id))
						if err != nil {
							logger.GetLogger("WARN").Printf("Failed to log failure: %v", err)
						}

						err = data.SetFinalStatusOfFailureLog(logId, sentStatus, 1, msg.RetryTimes)
						if err != nil {
							logger.GetLogger("WARN").Print(err)
						}
					}
					if config.GetConfig().EnableMsgSentLog {
						logger.GetLogger("DATA").Printf("RESENT key:%v result:%v code:%v elapse:%vms dest:%v DATA: %+v",
							msg.MsgKey,
							sentSuccess,
							httpStatusCode,
							fmt.Sprintf("%.3f", float64(et.Sub(st).Nanoseconds())/1e6),
							sub.Reception_channel, msg.Body,
						)
					}
				}
			}

			elapsed := time.Now().Sub(timerOfSendingInterval)
			minInterval := time.Millisecond * time.Duration(config.GetConfig().IntervalOfSendingForSendRoutineOfRetry)
			if elapsed < minInterval {
				time.Sleep(minInterval - elapsed)
			}
		}
	}
}

func transferSubscriptionViaHttp(msg *data.MessageStuct, sub *data.SubscriptionRecord, retryTimes uint16) (httpStatusCode int, returnData []byte, err error) {
	var subUrl *url.URL
	var msgBody []byte
	uniqJobId := genUniqueJobId((*msg).Time, (*msg).OriginJobId, (*sub).Subscription_id)
	subUrl, err = url.Parse(sub.Reception_channel)
	if err != nil {
		httpStatusCode = -2
		err = errors.New(fmt.Sprintf("Failed to parse subscription url: %v : %v", (*sub).Reception_channel, err))
		return
	}
	var appendedQueryStr string
	if len(subUrl.RawQuery) > 0 {
		appendedQueryStr = fmt.Sprintf("&jobid=%v&retry_times=%v", uniqJobId, retryTimes)
	} else {
		appendedQueryStr = fmt.Sprintf("jobid=%v&retry_times=%v", uniqJobId, retryTimes)
	}
	subUrl.RawQuery += appendedQueryStr
	postFields := map[string]string{"retry_times": strconv.Itoa(int(retryTimes)), "jobid": uniqJobId}
	msgBody, err = json.Marshal(msg.Body)
	if err != nil {
		httpStatusCode = -1
		err = errors.New(fmt.Sprintf("Failed to encode msg body: %v", err))
		return
	}
	postFields["message"] = string(msgBody)
	return httproxy.Transfer(subUrl.String(), postFields, time.Second*time.Duration((*sub).Timeout))
}

func putToRetryChannel(br *broker.Broker, sub *data.SubscriptionRecord, msg *data.MessageStuct, stats *map[string]interface{}) error {
	delay := math.Pow(float64((*msg).RetryTimes), float64(2)) * float64(config.GetConfig().CoeOfIntervalForRetrySendingMsg)
	msgData, err := data.SerializeMessage(*msg)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to serialize msg: %v", err))
	}
	err = (*br).Use(config.GetChannelNameForReSend((*msg).MsgKey, (*sub).Subscription_id))
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to use channel when put message to retry channel: %v", err))
	}
	_, err = (*br).Pub(broker.DEFAULT_MSG_PRIORITY, uint64(delay), (*stats)["ttr"].(uint64), msgData)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to put message to retry channel: %v", err))
	}
	return nil
}

func genUniqueJobId(recvTime float64, originJobId uint64, subscriptionId int32) string {
	return fmt.Sprintf("%f-%v-%v", recvTime, originJobId, subscriptionId)
}
