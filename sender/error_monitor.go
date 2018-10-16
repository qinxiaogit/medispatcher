package sender

import (
	"fmt"
	"medispatcher/Alerter"
	_ "medispatcher/Alerter/proxy/AlarmPlatform"
	_ "medispatcher/Alerter/proxy/Email"
	_ "medispatcher/Alerter/proxy/Sms"
	"medispatcher/broker"
	"medispatcher/broker/beanstalk"
	"medispatcher/config"
	"medispatcher/data"
	"medispatcher/logger"
	"strings"
	"sync"
	"time"

	l "github.com/sunreaver/logger"
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
	alerterEmail  *Alerter.Alerter
	alerterSms    *Alerter.Alerter
	alarmPlatform *Alerter.Alerter
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
	alarmPlatform, err := Alerter.New(config.GetConfig().AlarmPlatform)
	if err != nil {
		logger.GetLogger("WARN").Printf("Failed to create platform alerter: %v", err)
	}
	monitor := &errorMonitor{
		alerterEmail:  alerterEmail,
		alerterSms:    alerterSms,
		alarmPlatform: alarmPlatform,
		scLock:        &sync.Mutex{},
		mcLock:        &sync.Mutex{},
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
	if (em.alerterSms == nil || subParam.AlerterPhoneNumbers == "") && (em.alerterEmail == nil || subParam.AlerterEmails == "") && (em.alarmPlatform == nil || subParam.AlerterReceiver == "") {
		// 同时也无法使用默认报警.
		if em.alarmPlatform == nil || config.GetConfig().DefaultAlarmReceiver == "" || config.GetConfig().DefaultAlarmChan == "" {
			l.LoggerByDay.Debugw("ShouldAlert No addSubscriptionCheck",
				"why", "no reciver")
			return
		}
	}
	em.scLock.Lock()
	currentTime := time.Now().Unix()
	sc, ok := em.checkPoints.subscriptions[sub.Subscription_id]
	if !ok {
		sc = errorSubscriptionCheck{
			subscriptionId:      sub.Subscription_id,
			errorSum:            1,
			errorCountStartTime: time.Now().Unix(),
		}
	} else if currentTime-sc.errorCountStartTime > subParam.IntervalOfErrorMonitorAlert {
		// 超出时间窗口
		sc.errorSum = 1
		sc.errorCountStartTime = currentTime
	} else {
		sc.errorSum++
	}

	errorSum := sc.errorSum
	var shouldAlert bool
	if currentTime-sc.lastAlertTime > subParam.AlarmInterval {
		// to many failures in the specified period, should alert.
		if sc.errorSum >= subParam.SubscriptionTotalFailureAlertThreshold {
			shouldAlert = true
		} else {
			l.LoggerByDay.Debugw("ShouldAlert No addSubscriptionCheck",
				"sc.errorSum", sc.errorSum,
				"subParam.SubscriptionTotalFailureAlertThreshold", subParam.SubscriptionTotalFailureAlertThreshold,
				"currentTime-sc.errorCountStartTime", currentTime-sc.errorCountStartTime,
				"subParam.IntervalOfErrorMonitorAlert", subParam.IntervalOfErrorMonitorAlert)
		}
	} else {
		l.LoggerByDay.Debugw("ShouldAlert No addSubscriptionCheck",
			"currentTime-sc.lastAlertTime", currentTime-sc.lastAlertTime,
			"subParam.AlarmInterval", subParam.AlarmInterval,
			"errorSum", sc.errorSum,
			"subParam.SubscriptionTotalFailureAlertThreshold", subParam.SubscriptionTotalFailureAlertThreshold)
	}

	// alert is to be sent, reset the stats.
	if shouldAlert {
		sc.lastAlertTime = currentTime
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
			subParam.IntervalOfErrorMonitorAlert/60, errorSum,
			sub.Subscription_id, sub.Reception_channel,
		),
	}

	// 如果订阅没有配置报警接收人则使用全局默认配置.
	var sentAlarm bool
	if em.alerterEmail != nil && subParam.AlerterEmails != "" {
		alert.Recipient = subParam.AlerterEmails
		alert.TemplateName = "MessageSendingFailed.eml"
		em.alerterEmail.Alert(alert)

		sentAlarm = true
		l.LoggerByDay.Debugw("ShouldAlert addSubscriptionCheck",
			"send AlerterEmails", subParam.AlerterEmails)
	}

	if em.alerterSms != nil && subParam.AlerterPhoneNumbers != "" {
		alert.Recipient = subParam.AlerterPhoneNumbers
		alert.TemplateName = "MessageSendingFailed.sms"
		em.alerterSms.Alert(alert)

		sentAlarm = true
		l.LoggerByDay.Debugw("ShouldAlert addSubscriptionCheck",
			"send AlerterPhoneNumbers", subParam.AlerterPhoneNumbers)
	}

	if em.alarmPlatform != nil && subParam.AlerterReceiver != "" {
		alert.Subject = "消息中心警报(统一告警)"
		alert.Recipient = subParam.AlerterReceiver
		alert.TemplateName = "MessageSendingFailed.sms"
		em.alarmPlatform.Alert(alert)

		sentAlarm = true
		l.LoggerByDay.Debugw("ShouldAlert addSubscriptionCheck",
			"send AlerterReceiver", subParam.AlerterReceiver)
	}

	l.LoggerByDay.Debugw("ShouldAlert addSubscriptionCheck",
		"send", sentAlarm,
		"Subscriber_id", sub.Subscriber_id,
		"Class_key", sub.Class_key,
		"IntervalOfErrorMonitorAlert", subParam.IntervalOfErrorMonitorAlert,
		"errorSum", errorSum,
		"Subscription_id", sub.Subscription_id,
		"AlerterEmails", subParam.AlerterEmails,
		"AlerterPhoneNumbers", subParam.AlerterPhoneNumbers,
		"alarmPlatform", em.alarmPlatform,
		"AlerterReceiver", subParam.AlerterReceiver,
	)

	if em.alarmPlatform != nil && !sentAlarm && config.GetConfig().DefaultAlarmReceiver != "" && config.GetConfig().DefaultAlarmChan != "" {
		alert.Subject = "消息中心警报(统一告警:未设置报警接收人)"
		alert.Content = fmt.Sprintf("***警告:订阅(ID=\"%v\")未设置报警接收人;***\r\n%s", sub.Subscription_id, alert.Content)
		alert.Recipient = config.GetConfig().DefaultAlarmReceiver
		alert.AlarmReceiveChan = config.GetConfig().DefaultAlarmChan
		alert.TemplateName = "MessageSendingFailed.sms"
		em.alarmPlatform.Alert(alert)
	}
}

func (em *errorMonitor) addMessageCheck(sub *data.SubscriptionRecord, subParam SubscriptionParams, logId uint64, lastErrorString string, errorTimes uint16) {
	if errorTimes < subParam.MessageFailureAlertThreshold {
		l.LoggerByDay.Debugw("ShouldAlert No addMessageCheck",
			"errorTimes", errorTimes,
			"subParam.MessageFailureAlertThreshold", subParam.MessageFailureAlertThreshold)
		return
	}
	if (em.alerterSms == nil || subParam.AlerterPhoneNumbers == "") && (em.alerterEmail == nil || subParam.AlerterEmails == "") && (em.alarmPlatform == nil || subParam.AlerterReceiver == "") {
		// 同时也无法使用默认报警.
		if em.alarmPlatform == nil || config.GetConfig().DefaultAlarmReceiver == "" || config.GetConfig().DefaultAlarmChan == "" {
			l.LoggerByDay.Debugw("ShouldAlert No addMessageCheck",
				"why", "no receiver")
			return
		}
	}
	em.mcLock.Lock()
	mc, ok := em.checkPoints.messages[sub.Subscription_id]
	if !ok {
		mc = errorMessageCheck{
			subscriptionId: sub.Subscription_id,
		}
	}
	currentTime := time.Now().Unix()
	if currentTime-mc.lastAlertTime >= subParam.AlarmInterval {
		mc.lastAlertTime = currentTime
		em.checkPoints.messages[sub.Subscription_id] = mc
		em.mcLock.Unlock()
	} else {
		em.mcLock.Unlock()
		l.LoggerByDay.Debugw("ShouldAlert No addMessageCheck",
			"currentTime-mc.lastAlertTime", currentTime-mc.lastAlertTime,
			"subParam.AlarmInterval", subParam.AlarmInterval,
			"errorTimes", errorTimes,
			"subParam.MessageFailureAlertThreshold", subParam.MessageFailureAlertThreshold)
		return
	}
	var delay float64
	maxRetry := config.GetConfig().MaxRetryTimesOfSendingMessage
	coe := config.GetConfig().CoeOfIntervalForRetrySendingMsg
	count := errorTimes + 1
	for count <= maxRetry {
		delay += getRetryDelay(count, coe)
		count++
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

	// 如果订阅没有配置报警接收人则使用全局默认配置.
	var sentAlarm bool
	if em.alerterEmail != nil && subParam.AlerterEmails != "" {
		alert.Recipient = subParam.AlerterEmails
		alert.TemplateName = "MessageSendingFailed.eml"
		em.alerterEmail.Alert(alert)

		sentAlarm = true

		l.LoggerByDay.Debugw("ShouldAlert addMessageCheck",
			"send AlerterEmails", subParam.AlerterEmails)
	}
	alert.Content = smsMsg
	if em.alerterSms != nil && subParam.AlerterPhoneNumbers != "" {
		alert.Recipient = subParam.AlerterPhoneNumbers
		alert.TemplateName = "MessageSendingFailed.sms"
		em.alerterSms.Alert(alert)

		sentAlarm = true
		l.LoggerByDay.Debugw("ShouldAlert addMessageCheck",
			"send AlerterPhoneNumbers", subParam.AlerterPhoneNumbers)
	}

	if em.alarmPlatform != nil && subParam.AlerterReceiver != "" {
		alert.Subject = "消息中心警报(统一告警)"
		alert.Recipient = subParam.AlerterReceiver
		alert.TemplateName = "MessageSendingFailed.sms"
		em.alarmPlatform.Alert(alert)

		sentAlarm = true

		l.LoggerByDay.Debugw("ShouldAlert addMessageCheck",
			"send AlerterReceiver", subParam.AlerterReceiver)
	}

	l.LoggerByDay.Debugw("ShouldAlert addMessageCheck",
		"send", sentAlarm,
		"Subscriber_id", sub.Subscriber_id,
		"Class_key", sub.Class_key,
		"IntervalOfErrorMonitorAlert", subParam.IntervalOfErrorMonitorAlert,
		"errorSum", errorTimes,
		"Subscription_id", sub.Subscription_id,
		"AlerterEmails", subParam.AlerterEmails,
		"AlerterPhoneNumbers", subParam.AlerterPhoneNumbers,
		"alarmPlatform", em.alarmPlatform,
		"AlerterReceiver", subParam.AlerterReceiver,
	)

	if em.alarmPlatform != nil && !sentAlarm && config.GetConfig().DefaultAlarmReceiver != "" && config.GetConfig().DefaultAlarmChan != "" {
		alert.Subject = "消息中心警报(统一告警:未设置报警接收人)"
		alert.Content = fmt.Sprintf("***警告:订阅(ID=\"%v\")未设置报警接收人;***\r\n%s", sub.Subscription_id, alert.Content)
		alert.Recipient = config.GetConfig().DefaultAlarmReceiver
		alert.AlarmReceiveChan = config.GetConfig().DefaultAlarmChan
		alert.TemplateName = "MessageSendingFailed.sms"
		em.alarmPlatform.Alert(alert)
	}
}

// Check queued message blocks every 5 seconds.
func (em *errorMonitor) checkQueueBlocks() {
	var (
		brPool                   *beanstalk.SafeBrokerkPool
		stats, reQueueStats      map[string]map[string]interface{}
		errOfQueue, errOfReQueue error
	)
	if em.alerterEmail == nil && em.alerterSms == nil && em.alarmPlatform == nil {
		return
	}
	brPool = broker.GetBrokerPoolWithBlock(1, 3, shouldExit)
	if brPool == nil {
		return
	}
	alertStatistics := map[int32]int64{}

	// 如果订阅没有配置报警接收人则使用全局默认配置.
	var sentAlarm bool
	for {
		time.Sleep(time.Second * 15)
		subscriptions, err := data.GetAllSubscriptionsWithCache()
		if err != nil {
			logger.GetLogger("WARN").Printf("Failed to get subscriptions: %v", err)
		} else {
			for _, sub := range subscriptions {
				var blockedMessageCount, blockedReQueueMessageCount int
				subParams := NewSubscriptionParams()
				err = subParams.Load(sub.Subscription_id)
				if err != nil {
					logger.GetLogger("WARN").Printf("Failed to load subscription[%v] params: %v", sub.Subscription_id, err)
					continue
				}
				// 当前订阅没有打开报警.
				if !subParams.AlerterEnabled {
					continue
				}

				if subParams.AlerterEmails == "" && subParams.AlerterPhoneNumbers == "" && subParams.AlerterReceiver == "" {
					// 没有任何可用的报警接收方.
					if config.GetConfig().DefaultAlarmReceiver == "" && config.GetConfig().DefaultAlarmChan == "" {
						continue
					}
				}
				lastAlertTime, exists := alertStatistics[sub.Subscription_id]
				currentTime := time.Now().Unix()

				if exists && currentTime-lastAlertTime < subParams.IntervalOfErrorMonitorAlert {
					continue
				}
				queueName := config.GetChannelName(sub.Class_key, sub.Subscription_id)
				reQueueName := config.GetChannelNameForReSend(sub.Class_key, sub.Subscription_id)

				stats, errOfQueue = brPool.StatsTopic(queueName)
				if errOfQueue != nil {
					// TODO: may contains multiple error types
					if strings.Contains(errOfQueue.Error(), broker.ERROR_QUEUE_NOT_FOUND) {
						logger.GetLogger("WARN").Printf("%v ERR: %v", queueName, errOfQueue)
					}
					continue
				}
				reQueueStats, errOfReQueue = brPool.StatsTopic(reQueueName)
				if errOfReQueue != nil {
					// TODO: may contains multiple error types
					if strings.Contains(errOfReQueue.Error(), broker.ERROR_QUEUE_NOT_FOUND) {
						logger.GetLogger("WARN").Printf("%v ERR: %v", reQueueName, reQueueStats)
					}
					continue
				}
				for _, s := range stats {
					n, _ := s["current-jobs-ready"].(int)
					blockedMessageCount += n
				}

				for _, s := range reQueueStats {
					n, _ := s["current-jobs-ready"].(int)
					blockedReQueueMessageCount += n
				}

				if blockedMessageCount == 0 && blockedReQueueMessageCount == 0 {
					continue
				}

				if blockedMessageCount >= int(subParams.MessageBlockedAlertThreshold) || blockedReQueueMessageCount >= int(subParams.MessageBlockedAlertThreshold) {
					alert := Alerter.Alert{
						Subject: "消息中心警报",
						Content: fmt.Sprintf("队列 %v 消息等待数已达%v, 重试队列 %v 消息等待数已达%v, 请到后台订阅管理中调节消息处理速率参数或者优化woker的处理速度。\n订阅ID: %v\n消息处理地址: %v\n当前推送并发数: %v\n推送最小间隔时间: %vms",
							queueName, blockedMessageCount, reQueueName, blockedReQueueMessageCount,
							sub.Subscription_id, sub.Reception_channel, subParams.Concurrency,
							subParams.IntervalOfSending,
						),
					}
					if subParams.AlerterEmails != "" && em.alerterEmail != nil {
						alert.Recipient = subParams.AlerterEmails
						alert.TemplateName = "MessageSendingFailed.eml"
						em.alerterEmail.Alert(alert)

						sentAlarm = true
					}

					if subParams.AlerterPhoneNumbers != "" && em.alerterSms != nil {
						alert.Recipient = subParams.AlerterPhoneNumbers
						alert.TemplateName = "MessageSendingFailed.sms"
						em.alerterSms.Alert(alert)

						sentAlarm = true
					}

					if subParams.AlerterReceiver != "" && em.alarmPlatform != nil {
						alert.Subject = "消息中心警报(统一告警)"
						alert.Recipient = subParams.AlerterReceiver
						alert.TemplateName = "MessageSendingFailed.sms"
						em.alarmPlatform.Alert(alert)

						sentAlarm = true
					}

					if em.alarmPlatform != nil && !sentAlarm && config.GetConfig().DefaultAlarmReceiver != "" && config.GetConfig().DefaultAlarmChan != "" {
						alert.Subject = "消息中心警报(统一告警:未设置报警接收人)"
						alert.Content = fmt.Sprintf("***警告:订阅(ID=\"%v\")未设置报警接收人;***\r\n%s", sub.Subscription_id, alert.Content)
						alert.Recipient = config.GetConfig().DefaultAlarmReceiver
						alert.AlarmReceiveChan = config.GetConfig().DefaultAlarmChan
						alert.TemplateName = "MessageSendingFailed.sms"
						em.alarmPlatform.Alert(alert)
					}

					alertStatistics[sub.Subscription_id] = currentTime
				}
			}
		}
	}
}
