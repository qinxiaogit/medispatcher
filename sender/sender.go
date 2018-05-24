// TODO: When subscription is canceled.
package sender

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"medispatcher/broker"
	"medispatcher/config"
	"medispatcher/data"
	"medispatcher/logger"
	"medispatcher/pushstatistics"
	httproxy "medispatcher/transproxy/http"
	"net/http"
	"net/url"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	l "github.com/sunreaver/gotools/logger"
)

var subHandlerWorkWg = new(sync.WaitGroup)
var subscriptionHandlerSpwanLock = new(sync.RWMutex)

// StartAndWait starts the recover process until Stop is called.
func StartAndWait() {
	procExitWG.Add(1)
	senderErrorMonitor = newErrorMonitor()
	senderErrorMonitor.start()
	go listenAndDeleteMessage()

	for !shouldExit() {
		subscriptions, err := data.GetAllSubscriptionsWithCache()
		if err != nil {
			logger.GetLogger("WARN").Printf("Failed to get subscriptions: %v", err)
		} else {
			for _, sub := range subscriptions {
				if !senderRoutineStats.statusExists(sub.Subscription_id) {
					go handleSubscription(sub)
				}
			}
		}
		// exits the routines for the subscriptions which have been canceled.
		handlingSubIds := senderRoutineStats.getHandlingSubscriptionIds()
		for _, id := range handlingSubIds {
			enabled := false
			for _, sub := range subscriptions {
				if sub.Subscription_id == id {
					enabled = true
					break
				}
			}
			if !enabled {
				status := senderRoutineStats.getStatus(id)
				if status != nil {
					status.sigChan <- SENDER_ROUTINE_SIG_EXIT_ALL_ROUTINES
					senderRoutineStats.removeStatus(id)
				}
			}
		}
		time.Sleep(time.Second * 2)
	}
	subscriptionHandlerSpwanLock.Lock()
	// exit
	for _, status := range senderRoutineStats.routineStatus {
		(*status).sigChan <- SENDER_ROUTINE_SIG_EXIT_ALL_ROUTINES
	}

	subHandlerWorkWg.Wait()
	brokerCmdPools.Close()
	procExitWG.Done()
}

// handel a subscription.
func handleSubscription(sub data.SubscriptionRecord) {
	subscriptionHandlerSpwanLock.RLock()
	defer subscriptionHandlerSpwanLock.RUnlock()
	subParams := NewSubscriptionParams()
	sossr := StatusOfSubSenderRoutine{
		subscription:   &sub,
		coCount:        0,
		coCountOfRetry: 0,
		sigChan:        make(chan SubSenderRoutineChanSig, 1),
		subParams:      subParams,
	}
	senderRoutineStats.addStatus(sub.Subscription_id, &sossr)

	err := subParams.RefreshAndLoad(sub.Subscription_id)
	if err != nil {
		logger.GetLogger("INFO").Printf("Failed to load subscription params: %v, ignore customized subscription performance params.", err)
	}

	if subParams.Concurrency > config.GetConfig().MaxSendersPerChannel {
		subParams.Concurrency = config.GetConfig().MaxSendersPerChannel
	}

	if subParams.ProcessTimeout == 0 {
		subParams.ProcessTimeout = config.GetConfig().DefaultMaxMessageProcessTime
	} else if subParams.ProcessTimeout > config.GetConfig().MaxMessageProcessTime {
		subParams.ProcessTimeout = config.GetConfig().MaxMessageProcessTime
	}

	if subParams.ConcurrencyOfRetry > config.GetConfig().MaxSendersPerRetryChannel {
		subParams.ConcurrencyOfRetry = config.GetConfig().MaxSendersPerRetryChannel
	}

	listenerExitSigChan := make(chan bool)
	procExitWG.Add(1)
	// start message channel listener
	msgChan := newMessasgeListener(config.GetChannelName(sub.Class_key, sub.Subscription_id), subParams, listenerExitSigChan)
	if msgChan == nil {
		// exiting stage
		return
	}
	procExitWG.Add(1)
	retryMsgChan := newMessasgeListener(config.GetChannelNameForReSend(sub.Class_key, sub.Subscription_id), subParams, listenerExitSigChan)
	if retryMsgChan == nil {
		// exiting stage
		return
	}

	senderWorkerWg := new(sync.WaitGroup)

	senderRoutineExitSigChans := []chan bool{}
	senderRoutineOfRetryExitSigChans := []chan bool{}
	for i := uint32(0); i < subParams.Concurrency; i++ {
		exitSigCh := make(chan bool)
		senderWorkerWg.Add(1)
		go sendSubscription(msgChan, sub, &sossr, exitSigCh, senderWorkerWg)
		senderRoutineExitSigChans = append(senderRoutineExitSigChans, exitSigCh)
		sossr.IncreaseCoCount(1)
	}

	for i := uint32(0); i < subParams.ConcurrencyOfRetry; i++ {
		exitSigCh := make(chan bool)
		senderWorkerWg.Add(1)
		go sendSubscriptionAsRetry(retryMsgChan, sub, &sossr, exitSigCh, senderWorkerWg)
		senderRoutineOfRetryExitSigChans = append(senderRoutineOfRetryExitSigChans, exitSigCh)
		sossr.InCoCountOfRetry(1)
	}

	senderRoutineStats.setStatus(sub.Subscription_id, &sossr)
	subHandlerWorkWg.Add(1)
	// wait for management signals.
	go func() {
		defer subHandlerWorkWg.Done()
		for {
			sig := <-sossr.sigChan
			switch sig {
			case SENDER_ROUTINE_SIG_INCREASE_ROUTINE:
				ch := make(chan bool)
				senderRoutineExitSigChans = append(senderRoutineExitSigChans, ch)
				senderWorkerWg.Add(1)
				go sendSubscription(msgChan, sub, &sossr, ch, senderWorkerWg)
				sossr.IncreaseCoCount(1)
			case SENDER_ROUTINE_SIG_DECREASE_ROUTINE:
				if len(senderRoutineExitSigChans) > 0 {
					close(senderRoutineExitSigChans[0])
					if len(senderRoutineExitSigChans) > 1 {
						senderRoutineExitSigChans = senderRoutineExitSigChans[1:]
					} else {
						senderRoutineExitSigChans = nil
					}
					sossr.DecreaseCoCount(1)
				}
			case SENDER_ROUTINE_SIG_EXIT_ALL_ROUTINES:
				close(listenerExitSigChan)
				allSigChans := append(senderRoutineExitSigChans, senderRoutineOfRetryExitSigChans...)

				for _, ch := range allSigChans {
					close(ch)
				}

				// waits for all routines to exit.
				senderWorkerWg.Wait()
				sossr.SetCoCount(0)
				sossr.SetCoCountOfRetry(0)
				sossr.SetExited()
				return
			case SENDER_ROUTINE_SIG_DECREASE_ROUTINE_FOR_RETRY:
				if len(senderRoutineOfRetryExitSigChans) > 0 {
					close(senderRoutineOfRetryExitSigChans[0])
					if len(senderRoutineOfRetryExitSigChans) > 1 {
						senderRoutineOfRetryExitSigChans = senderRoutineOfRetryExitSigChans[1:]
					} else {
						senderRoutineOfRetryExitSigChans = nil
					}
					sossr.DecreaseCoCountOfRetry(1)
				}
			case SENDER_ROUTINE_SIG_INCREASE_ROUTINE_FOR_RETRY:
				sigCh := make(chan bool, 1)
				senderRoutineOfRetryExitSigChans = append(senderRoutineOfRetryExitSigChans, sigCh)
				senderWorkerWg.Add(1)
				go sendSubscriptionAsRetry(retryMsgChan, sub, &sossr, sigCh, senderWorkerWg)
				sossr.InCoCountOfRetry(1)
			}
		}
	}()
}

func sendSubscription(msgChan chan *Msg, sub data.SubscriptionRecord, sossr *StatusOfSubSenderRoutine, exitSigCh chan bool, wg *sync.WaitGroup) {
	var (
		err             error
		httpStatusCode  int
		returnData      []byte
		sentSuccess     bool
		respd           map[string]interface{}
		errMsgInSending string
		msgR            *Msg
		logId           uint64
		msg             *data.MessageStuct
	)
	defer func() {
		err := recover()
		if err != nil {
			logger.GetLogger("ERROR").Printf("sender routine exiting abnormally: %v.   %s", err, debug.Stack())
		} else {
			logger.GetLogger("INFO").Printf("Sender routine for '%v:%v' exited.", sub.Class_key, sub.Subscription_id)
		}
		wg.Done()
	}()

	var reserveTimeoutTimer *time.Timer
	for {
		msgR = nil
		select {
		case <-exitSigCh:
			// this case means just decrease a routine. other routines may take the message from the channel.
			// TODO: cannot ensure there're other routines taking messages, so messages can be lost in very special cases.
			if !shouldExit() {
				return
			} else {
				// this case means the service is in exiting stage, all messages in the channel should be processed before exit.
				if reserveTimeoutTimer == nil {
					reserveTimeoutTimer = time.NewTimer(time.Second * DEFAULT_RESERVE_TIMEOUT)
				} else {
					reserveTimeoutTimer.Reset(time.Second * DEFAULT_RESERVE_TIMEOUT)
				}
				select {
				// all messages have been processed.
				case <-reserveTimeoutTimer.C:
					return
				case msgR = <-msgChan:
				}
			}
		case msgR = <-msgChan:

		}
		if msgR == nil {
			continue
		}
		msg, err = data.UnserializeMessage(msgR.Body)
		if err != nil {
			logger.GetLogger("WARN").Printf("Failed to decode msg body for job[%v] when send to subscriber: %v", msgR.Id, err)
			logger.GetLogger("DATA").Printf("DECODERR %s", msgR.Body)
			deleteMessage(msgR)
		} else {
			sentSuccess = false
			// subscription parameters may changes dynamically, while "sub" object won't  until the dispatcher service is restarted.
			procesTimeout := sossr.GetSubParams().ProcessTimeout
			receptionUri := sossr.GetSubParams().ReceptionUri
			if procesTimeout > 0 {
				sub.Timeout = procesTimeout
			}
			if receptionUri != "" {
				sub.Reception_channel = receptionUri
			}
			st := time.Now()
			var sendUrl string
			sendUrl, httpStatusCode, returnData, err = transferSubscriptionViaHttp(msg, &sub, 0)
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
			if sentSuccess || sendUrl == "" {
				// TODO: performance test for deleting messages.
				deleteMessage(msgR)
				if sendUrl == "" {
					logger.GetLogger("WARN").Print(err)
				}
			} else {
				// logging failure.
				if httpStatusCode == 0 {
					errMsgInSending = fmt.Sprintf("Code: 0\nElapsed:%.3f ms\nContent:\n\tFailed to get response: %v", float64(time.Now().Sub(st).Nanoseconds())/1e6, err)
				} else if httpStatusCode == -1 {
					// encode body failed
					errMsgInSending = fmt.Sprintf("not send. failed to encode message body.\nerr:\n\t%v", err)
				} else if httpStatusCode == -2 {
					errMsgInSending = fmt.Sprintf("not send, failed to parse url. \nContent:\n\t%s", sub.Reception_channel)
				} else {
					errMsgInSending = fmt.Sprintf("Code: %v\nContent:\n\t%s", httpStatusCode, returnData)
				}
				logSub := sub
				logSub.Reception_channel = sendUrl
				logId, err = data.LogFailure(msg, logSub, errMsgInSending, 0, genUniqueJobId(msg.Time, msg.OriginJobId, sub.Subscription_id))
				if err != nil {
					logger.GetLogger("WARN").Printf("Failed to log failure: %v", err)
				} else {
					msg.LogId = logId
					err = data.SetFinalStatusOfFailureLog(logId, 0, 1, 0)
					if err != nil {
						logger.GetLogger("WARN").Print(err)
					}
				}

				err = putToRetryChannel(&sub, msg, msgR)
				if err != nil {
					logger.GetLogger("WARN").Print(err)
				} else {
					deleteMessage(msgR)
				}
			}
			if config.GetConfig().EnableMsgSentLog {
				msgBody, logErr := json.Marshal(msg.Body)
				if logErr != nil {
					logger.GetLogger("WARN").Printf("Failed to log sent message: %v.", logErr)
				} else {
					logger.GetLogger("DATA").Printf("SENT key:%v result:%v code:%v elapse:%vms dest:%v DATA: %v",
						msg.MsgKey,
						sentSuccess,
						httpStatusCode,
						fmt.Sprintf("%.3f", float64(et.Sub(st).Nanoseconds())/1e6),
						sendUrl,
						string(msgBody),
					)
				}
			}

			if !sentSuccess && sossr.GetSubParams().AlerterEnabled {
				l.LoggerByDay.Debugw("SendSubscription", "sossr", sossr.GetSubParams(),
					"msg", msg)

				senderErrorMonitor.addSubscriptionCheck(&sub, sossr.GetSubParams())
				senderErrorMonitor.addMessageCheck(&sub, sossr.GetSubParams(), logId, errMsgInSending, 1)
			}
		}
	}

}

func sendSubscriptionAsRetry(msgChan chan *Msg, sub data.SubscriptionRecord, sossr *StatusOfSubSenderRoutine, exitSigCh chan bool, wg *sync.WaitGroup) {
	var (
		err, sendingErr error
		httpStatusCode  int
		returnData      []byte
		sentSuccess     bool
		sentStatus      uint8
		respd           map[string]interface{}
		errMsgInSending string
		msgR            *Msg
		logId           uint64
		msg             *data.MessageStuct
	)

	defer func() {
		err := recover()
		if err != nil {
			logger.GetLogger("ERROR").Printf("sender routine for retry-channel exiting abnormally: %v", err)
		}
		wg.Done()
	}()

	var reserveTimeoutTimer *time.Timer
	for {
		msgR = nil
		select {
		case <-exitSigCh:
			// this case means just decrease a routine.
			if !shouldExit() {
				return
			} else {
				// this case means just decrease a routine. other routines may take the message from the channel.
				// TODO: cannot ensure there're other routines taking messages, so messages can be lost in very special cases.
				if reserveTimeoutTimer == nil {
					reserveTimeoutTimer = time.NewTimer(time.Second * DEFAULT_RESERVE_TIMEOUT)
				} else {
					reserveTimeoutTimer.Reset(time.Second * DEFAULT_RESERVE_TIMEOUT)
				}
				select {
				// all messages have been processed.
				case <-reserveTimeoutTimer.C:
					return
				case msgR = <-msgChan:
				}
			}
		case msgR = <-msgChan:

		}
		if msgR == nil {
			continue
		}
		msg, err = data.UnserializeMessage(msgR.Body)
		if err != nil {
			logger.GetLogger("WARN").Printf("Failed to decode msg body for job[%v] when send to subscriber: %v", msgR.Id, err)
			logger.GetLogger("DATA").Printf("DECODERR %s", msgR.Body)
			deleteMessage(msgR)
		} else {
			msg.RetryTimes += 1
			sentSuccess = false
			// subscription parameters may changes dynamically, while "sub" object won't  until the dispatcher service is restarted.
			procesTimeout := sossr.GetSubParams().ProcessTimeout
			receptionUri := sossr.GetSubParams().ReceptionUri
			if procesTimeout > 0 {
				sub.Timeout = procesTimeout
			}
			if receptionUri != "" {
				sub.Reception_channel = receptionUri
			}
			st := time.Now()
			var sendUrl string
			sendUrl, httpStatusCode, returnData, sendingErr = transferSubscriptionViaHttp(msg, &sub, msg.RetryTimes)
			et := time.Now()
			if httpStatusCode == 200 {
				err = json.Unmarshal(returnData, &respd)
				if err != nil {
					logger.GetLogger("WARN").Printf("Subscriber(%v) did not response a valid data on message(%v): %v: %s", sub.Subscriber_id, msg.MsgKey, err, returnData)
				} else {
					reStatus, exists := respd["status"]
					if !exists {
						logger.GetLogger("WARN").Printf("Subscriber(%v)'s response didnot contains the \"status\" field on message(%v).", sub.Subscriber_id, msg.MsgKey)
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
			// Delete it from the queue if success or exceeded the maximum retry times, or sendUrl is empty.
			if sendUrl == "" || sentSuccess || msg.RetryTimes >= config.GetConfig().MaxRetryTimesOfSendingMessage {
				if sendUrl == "" {
					logger.GetLogger("WARN").Print(sendingErr)
				}
				//TODO: because of async command, this may be unsuccessful
				deleteMessage(msgR)
				err = data.SetFinalStatusOfFailureLog(msg.LogId, sentStatus, 0, msg.RetryTimes)
				if err != nil {
					logger.GetLogger("WARN").Print(err)
				}
			} else {

				err = putToRetryChannel(&sub, msg, msgR)
				if err == nil {
					deleteMessage(msgR)
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
				logSub := sub
				logSub.Reception_channel = sendUrl
				logId, err = data.LogFailure(msg, logSub, errMsgInSending, msg.LogId, genUniqueJobId(msg.Time, msg.OriginJobId, sub.Subscription_id))
				if err != nil {
					logger.GetLogger("WARN").Printf("Failed to log failure: %v", err)
				}

				err = data.SetFinalStatusOfFailureLog(logId, sentStatus, 1, msg.RetryTimes)
				if err != nil {
					logger.GetLogger("WARN").Print(err)
				}
			}
			if config.GetConfig().EnableMsgSentLog {
				msgBody, logErr := json.Marshal(msg.Body)
				if logErr != nil {
					logger.GetLogger("WARN").Printf("Failed to log sent message: %v.", logErr)
				} else {
					logger.GetLogger("DATA").Printf("RESENT key:%v result:%v code:%v elapse:%vms dest:%v DATA: %v",
						msg.MsgKey,
						sentSuccess,
						httpStatusCode,
						fmt.Sprintf("%.3f", float64(et.Sub(st).Nanoseconds())/1e6),
						sendUrl, string(msgBody),
					)
				}
			}
			if !sentSuccess && sossr.GetSubParams().AlerterEnabled {
				l.LoggerByDay.Debugw("SendSubscriptionAsRetry", "sossr", sossr.GetSubParams(),
					"msg", msg)
				senderErrorMonitor.addSubscriptionCheck(&sub, sossr.GetSubParams())
				senderErrorMonitor.addMessageCheck(&sub, sossr.GetSubParams(), msg.LogId, errMsgInSending, msg.RetryTimes)
			}
		}
	}
}

func transferSubscriptionViaHttp(msg *data.MessageStuct, sub *data.SubscriptionRecord, retryTimes uint16) (sendUrl string, httpStatusCode int, returnData []byte, err error) {
	var subUrl *url.URL
	var msgBody []byte
	uniqJobId := genUniqueJobId((*msg).Time, (*msg).OriginJobId, (*sub).Subscription_id)
	subUrls := strings.Split(sub.Reception_channel, "\n")
	// Check for reception env tags
	receptionEnv := strings.ToUpper(config.GetConfig().RECEPTION_ENV)
	// tagged urls that match the configured reception env.
	taggedUrls := []string{}
	// nonTaggedUrls maybe be used as the default urls, if there're no matched tagged urls.s
	nonTaggedUrls := []string{}
	for i, url := range subUrls {
		subUrls[i] = strings.TrimSpace(url)
		testUrl := strings.ToUpper(subUrls[i])
		tag := regexp.MustCompile("^\\[.*?\\]").FindString(testUrl)
		// has a reception env tag.
		if tag != "" {
			subUrls[i] = string([]byte(subUrls[i])[len(tag):])
			if receptionEnv != "" {
				tagPortions := strings.Split(strings.Trim(tag, "[]"), ":")
				if len(tagPortions) > 1 && tagPortions[0] == "T_ENV" && tagPortions[1] == receptionEnv {
					taggedUrls = append(taggedUrls, subUrls[i])
				}
			}
		} else {
			nonTaggedUrls = append(nonTaggedUrls, subUrls[i])
		}
	}
	if len(taggedUrls) > 0 {
		subUrls = taggedUrls
	} else {
		subUrls = nonTaggedUrls
	}
	if len(subUrls) < 1 {
		err = errors.New(fmt.Sprintf("No qualified urls to use for sending message, please check the subscription info[subscription id: %v, key: %v]", sub.Subscription_id, sub.Class_key))
		return
	}
	rand.Seed(int64(time.Now().Nanosecond()))
	subUrl, err = url.Parse(strings.TrimSpace(subUrls[rand.Intn(len(subUrls))]))
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
		err = fmt.Errorf("Failed to encode msg body: %v", err)
		return
	}

	// TODO 统计发送情况
	defer func() {
		if httpStatusCode == http.StatusOK {
			//发送成功
			pushstatistics.Add(sub, 1)
		}
	}()

	postFields["message"] = string(msgBody)

	// 上下文传递.
	var headers map[string]string = map[string]string{}
	if msg.Context != "" {
		headers["context"] = msg.Context
	}

	if msg.Owl_context != "" {
		headers["owl_context"] = msg.Owl_context
	}

	httpStatusCode, returnData, err = httproxy.Transfer(subUrl.String(), postFields, headers, time.Millisecond*time.Duration((*sub).Timeout))
	return subUrl.String(), httpStatusCode, returnData, err
}

func putToRetryChannel(sub *data.SubscriptionRecord, msg *data.MessageStuct, msgR *Msg) error {
	delay := getRetryDelay(msg.RetryTimes+1, config.GetConfig().CoeOfIntervalForRetrySendingMsg)
	msgData, err := data.SerializeMessage(*msg)
	if err != nil {
		return fmt.Errorf("Failed to serialize msg: %v", err)
	}
	queueName := config.GetChannelNameForReSend(sub.Class_key, sub.Subscription_id)
	_, err = brokerCmdPools.getPool(queueName).Pub(queueName, msgData, broker.DEFAULT_MSG_PRIORITY, uint64(delay), msgR.Stats.TTR)
	if err != nil {
		return fmt.Errorf("Failed to put message to retry channel: %v", err)
	}
	return nil
}

func genUniqueJobId(recvTime float64, originJobId uint64, subscriptionId int32) string {
	return fmt.Sprintf("%f-%v-%v", recvTime, originJobId, subscriptionId)
}
