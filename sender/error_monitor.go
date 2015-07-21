package sender

import (
	"fmt"
	"medispatcher/Alerter"
	_ "medispatcher/Alerter/proxy/Email"
	_ "medispatcher/Alerter/proxy/Sms"
	"medispatcher/broker"
	"medispatcher/config"
	"medispatcher/data"
	"medispatcher/logger"
	"sync"
	"time"
)

type errorSubscriptionCheck struct {
	subscriptionId      int32
	errorSum            int32
	errorCountStartTime int64
	lastAlertTime       int64
}

type errorMessageCheck struct {
	lastAlertTime  int64
	subscriptionId int32
}

type errorCheckPoints struct {
	subscriptions map[int32]errorSubscriptionCheck
	messages      map[int32]errorMessageCheck
}

type errorMonitor struct {
	alerterEmail *Alerter.Alerter
	alerterSms   *Alerter.Alerter
	// Subscription checkpoints lock
	scLock *sync.Mutex
	// Message checkpoints lock
	mcLock      *sync.Mutex
	checkPoints errorCheckPoints
}

func newErrorMonitor() *errorMonitor {
	alerterEmailCfg := config.GetConfig().AlerterEmail
	alerterEmailCfg.Set("logger", logger.GetLogger("INFO"))
	alerterSmsCfg := config.GetConfig().AlerterSms
	alerterSmsCfg.Set("logger", logger.GetLogger("INFO"))
	alerterEmail, err := Alerter.New(alerterEmailCfg)
	if err != nil {
		logger.GetLogger("WARN").Printf("Failed to create Email alerter: %v", err)
	}
	alerterSms, err := Alerter.New(alerterSmsCfg)
	if err != nil {
		logger.GetLogger("WARN").Printf("Failed to create Sms alerter: %v", err)
	}
	monitor := &errorMonitor{
		alerterEmail: alerterEmail,
		alerterSms:   alerterSms,
		scLock:       &sync.Mutex{},
		mcLock:       &sync.Mutex{},
		checkPoints: errorCheckPoints{
			subscriptions: map[int32]errorSubscriptionCheck{},
			messages:      map[int32]errorMessageCheck{},
		},
	}
	return monitor
}

func (em *errorMonitor) start() {
	go em.checkQueueBlocks()
}

func (em *errorMonitor) addSubscriptionCheck(sub *data.SubscriptionRecord, subParam SubscriptionParams) {
	if (em.alerterSms == nil || subParam.AlerterPhoneNumbers == "") && (em.alerterSms == nil || subParam.AlerterEmails == "") {
		return
	}
	em.scLock.Lock()
	sc, ok := em.checkPoints.subscriptions[sub.Subscription_id]
	if !ok {
		sc = errorSubscriptionCheck{
			subscriptionId:      sub.Subscription_id,
			errorSum:            1,
			errorCountStartTime: time.Now().Unix(),
		}
	} else {
		sc.errorSum += 1
	}
	errorSum := sc.errorSum
	var shouldAlert, reCount bool
	currentTime := time.Now().Unix()
	if currentTime-sc.lastAlertTime > INTERVAL_OF_ERROR_MONITOR_ALERT {
		// to many failures in the specified period, should alert.
		if sc.errorSum > SUBSCRIPTION_TOTAL_FAILURE_ALERT_THRESHOLD {
			shouldAlert = true
			reCount = true
		}
	}
	if currentTime-sc.errorCountStartTime > INTERVAL_OF_ERROR_MONITOR_ALERT {
		// re-count the failures, if the last error occured long ago.
		reCount = true
	}

	// alert is to be sent, reset the stats.
	if shouldAlert {
		sc.errorCountStartTime = currentTime
		sc.lastAlertTime = currentTime
	}
	if reCount {
		sc.errorCountStartTime = currentTime
		sc.errorSum = 1
	}
	em.checkPoints.subscriptions[sub.Subscription_id] = sc
	em.scLock.Unlock()
	if !shouldAlert {
		return
	}
	// TODO: language localization
	alert := Alerter.Alert{
		Subject: "消息中心警报",
		Content: fmt.Sprintf(
			"订阅者(%v)处理消息(%v)出错过于频繁. %v分钟内错误次数已达%v. 订阅ID: %v, 处理地址: %v",
			sub.Subscriber_id, sub.Class_key,
			INTERVAL_OF_ERROR_MONITOR_ALERT/60, errorSum,
			sub.Subscription_id, sub.Reception_channel,
		),
	}
	if em.alerterEmail != nil && subParam.AlerterEmails != "" {
		alert.Recipient = subParam.AlerterEmails
		alert.TemplateName = "MessageSendingFailed.eml"
		em.alerterEmail.Alert(alert)
	}

	if em.alerterSms != nil && subParam.AlerterPhoneNumbers != "" {
		alert.Recipient = subParam.AlerterPhoneNumbers
		alert.TemplateName = "MessageSendingFailed.sms"
		em.alerterSms.Alert(alert)
	}
}

func (em *errorMonitor) addMessageCheck(sub *data.SubscriptionRecord, subParam SubscriptionParams, logId uint64, lastErrorString string, errorTimes uint16) {
	if errorTimes < MESSAGE_FAILURE_ALERT_THRESHOLD {
		return
	}
	if (em.alerterSms == nil || subParam.AlerterPhoneNumbers == "") && (em.alerterSms == nil || subParam.AlerterEmails == "") {
		return
	}
	em.mcLock.Lock()
	mc, ok := em.checkPoints.messages[sub.Subscription_id]
	if !ok {
		mc = errorMessageCheck{
			subscriptionId: sub.Subscription_id,
		}
	}
	currentTime := time.Now().Unix()
	if currentTime-mc.lastAlertTime >= INTERVAL_OF_ERROR_MONITOR_ALERT {
		mc.lastAlertTime = currentTime
		em.checkPoints.messages[sub.Subscription_id] = mc
		em.mcLock.Unlock()
	} else {
		em.mcLock.Unlock()
		return
	}
	var delay float64
	maxRetry := config.GetConfig().MaxRetryTimesOfSendingMessage
	coe := config.GetConfig().CoeOfIntervalForRetrySendingMsg
	count := errorTimes + 1
	for count <= maxRetry {
		delay += getRetryDelay(count, coe)
		count += 1
	}
	emailMsg := fmt.Sprintf(
		"订阅者(%v)处理当前消息失败已达%v次，即将超限: %v .\n您有大概%v分钟进行修复.\n订阅ID: %v\n处理地址: %v\n日志ID: %v,请到后台查看更多。\n错误: %v\n退订此报警请到管理后台-订阅管理中操作。",
		sub.Subscriber_id,
		errorTimes,
		config.GetConfig().MaxRetryTimesOfSendingMessage,
		int(delay)/60,
		sub.Subscription_id,
		sub.Reception_channel,
		logId,
		lastErrorString,
	)
	smsMsg := fmt.Sprintf(
		"订阅者(%v)处理当前消息失败已达%v次，即将超限: %v .\n您有大概%v分钟进行修复.\n订阅ID: %v\n处理地址: %v\n日志ID: %v，请到后台查看更多。\n退订此报警请到管理后台-订阅管理中操作。",
		sub.Subscriber_id,
		errorTimes,
		config.GetConfig().MaxRetryTimesOfSendingMessage,
		int(delay)/60,
		sub.Subscription_id,
		sub.Reception_channel,
		logId,
	)
	alert := Alerter.Alert{
		Subject: "消息中心警报",
	}
	alert.Content = emailMsg
	if em.alerterEmail != nil && subParam.AlerterEmails != "" {
		alert.Recipient = subParam.AlerterEmails
		alert.TemplateName = "MessageSendingFailed.eml"
		em.alerterEmail.Alert(alert)
	}
	alert.Content = smsMsg
	if em.alerterSms != nil && subParam.AlerterPhoneNumbers != "" {
		alert.Recipient = subParam.AlerterPhoneNumbers
		alert.TemplateName = "MessageSendingFailed.sms"
		em.alerterSms.Alert(alert)
	}
}

// Check queued message blocks every 5 seconds.
func (em *errorMonitor) checkQueueBlocks() {
	var (
		br                       broker.Broker
		brConnected              bool
		stats, reQueueStats      map[string]interface{}
		errOfQueue, errOfReQueue error
	)
	if em.alerterEmail == nil && em.alerterSms == nil {
		return
	}
	alertStatistics := map[int32]int64{}
	for {
		subscriptions, err := data.GetAllSubscriptionsWithCache()
		if err != nil {
			logger.GetLogger("WARN").Printf("Failed to get subscriptions: %v", err)
		} else {
			for _, sub := range subscriptions {
				var blockedMessageCount, blockedReQueueMessageCount int
				subParams := NewSubscriptionParams()
				subParams.Load(sub.Subscription_id)
				if !subParams.AlerterEnabled || (subParams.AlerterEmails == "" && subParams.AlerterPhoneNumbers == "") {
					continue
				}
				lastAlertTime, exists := alertStatistics[sub.Subscription_id]
				currentTime := time.Now().Unix()

				if exists && currentTime-lastAlertTime < INTERVAL_OF_ERROR_MONITOR_ALERT {
					continue
				}
				queueName := config.GetChannelName(sub.Class_key, sub.Subscription_id)
				reQueueName := config.GetChannelNameForReSend(sub.Class_key, sub.Subscription_id)
				if !brConnected {
					br, err = broker.GetBrokerWitBlock(INTERVAL_OF_RETRY_ON_CONN_FAIL, shouldExit)
					if err != nil {
						break
					} else {
						brConnected = true
					}
				}
				stats, errOfQueue = br.StatsTopic(queueName)
				reQueueStats, errOfReQueue = br.StatsTopic(reQueueName)
				if errOfQueue != nil && (errOfQueue.Error() == broker.ERROR_CONN_CLOSED || errOfQueue.Error() == broker.ERROR_CONN_BROKEN){
					brConnected = false
				}
				if errOfReQueue != nil && (errOfReQueue.Error() == broker.ERROR_CONN_CLOSED || errOfQueue.Error() == broker.ERROR_CONN_BROKEN) {
					br.Close()
					brConnected = false
				}
				if errOfQueue == nil || errOfReQueue == nil {
					// TODO: assertion failed. if assertion success with ZERO value, the assertion result still holds false (bug of golang?).
					if errOfQueue == nil {
						blockedMessageCount, _ = stats["current-jobs-ready"].(int)
					}

					if errOfQueue == nil {
						blockedReQueueMessageCount, _ = reQueueStats["current-jobs-ready"].(int)
					}

					if blockedMessageCount == 0 && blockedReQueueMessageCount == 0 {
						continue
					}

					if blockedMessageCount >= MESSAGE_BLOCKED_ALERT_THRESHOLD || blockedReQueueMessageCount >= MESSAGE_BLOCKED_ALERT_THRESHOLD {
						alert := Alerter.Alert{
							Subject: "消息中心警报",
							Content: fmt.Sprintf("队列 %v 消息等待数已达%v, 重试队列 %v 消息等待数已达%v, 请到后台订阅管理中调节消息处理速率参数或者优化woker的处理速度。\n订阅ID: %v\n消息处理地址: %v\n当前推送并发数: %v\n推送最小间隔时间: %vms",
								queueName, blockedMessageCount, reQueueName, blockedReQueueMessageCount,
								sub.Subscription_id, sub.Reception_channel, subParams.Concurrency,
								subParams.IntervalOfSending,
							),
						}
						if subParams.AlerterEmails != "" {
							alert.Recipient = subParams.AlerterEmails
							alert.TemplateName = "MessageSendingFailed.eml"
							em.alerterEmail.Alert(alert)
						}

						if subParams.AlerterPhoneNumbers != "" {
							alert.Recipient = subParams.AlerterPhoneNumbers
							alert.TemplateName = "MessageSendingFailed.sms"
							em.alerterSms.Alert(alert)
						}
						alertStatistics[sub.Subscription_id] = currentTime
					}
				}
			}
		}
		time.Sleep(time.Second * 5)
	}
}
