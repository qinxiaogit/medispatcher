package sender

import (
	"errors"
	"fmt"
	"math"
	"medispatcher/config"
	"medispatcher/logger"
)

// Stop stops the senders.
func Stop(returnCh *chan string) {
	close(exitChan)
	procExitWG.Wait()
	*returnCh <- "msgsender"
}

// "true" indicates that the service/process is in exiting stage.
func shouldExit() bool {
	select {
	case <-exitChan:
		return true
	default:
		return false
	}
}

func getRetryDelay(retryTimes uint16, coeOfIntervalForRetrySendingMsg uint16) float64 {
	return math.Pow(float64(retryTimes), float64(2)) * float64(coeOfIntervalForRetrySendingMsg)
}

var subscriptionBenchCfg map[int32]bool = make(map[int32]bool)

// 订阅是否接受压测消息(默认不接受).
func ReceiveBenchMsgs(subscriptionId int32) bool {
	if _, ok := subscriptionBenchCfg[subscriptionId]; !ok {
		subParams := NewSubscriptionParams()
		err := subParams.Load(subscriptionId)
		if err != nil {
			logger.GetLogger("INFO").Printf("Failed to load subscription params: %v, ignore customized subscription performance params.", err)
			return false
		}

		subscriptionBenchCfg[subscriptionId] = subParams.ReceiveBenchMsgs
	}

	return subscriptionBenchCfg[subscriptionId]
}

// SetSubscriptionParams changes the params that affects the sender routine performances.
// This function is not go-routine safe. The invoker should implement go-routine safe calls.
func SetSubscriptionParams(subscriptionId int32, param SubscriptionParams) error {

	routineStatus := senderRoutineStats.getStatus(subscriptionId)
	// No currently running routine for the subscription. Maybe the subscription is in canceled status.
	if routineStatus == nil {
		// lock the  senderRoutineStats, in case a new routine for handling the subscription is created before its parameters are stored.
		senderRoutineStats.lock()
		defer senderRoutineStats.unlock()
	}
	if param.Concurrency > config.GetConfig().MaxSendersPerChannel {
		return errors.New(fmt.Sprintf("Sender number[%v] exceeded max[%v]", param.Concurrency, config.GetConfig().MaxSendersPerChannel))
	}
	if param.ConcurrencyOfRetry > config.GetConfig().MaxSendersPerRetryChannel {
		return errors.New(fmt.Sprintf("Sender(as retry) number[%v] exceeded max[%v]", param.ConcurrencyOfRetry, config.GetConfig().MaxSendersPerRetryChannel))
	}

	if param.ProcessTimeout > config.GetConfig().MaxMessageProcessTime {
		return errors.New(fmt.Sprintf("Message max process time[%v] exceeded max[%v]", param.ProcessTimeout, config.GetConfig().MaxMessageProcessTime))
	} else if param.ProcessTimeout <= 0 {
		param.ProcessTimeout = config.GetConfig().DefaultMaxMessageProcessTime
	}

	if routineStatus == nil {
		err := param.Store(subscriptionId)
		if err != nil {
			logger.GetLogger("WARN").Printf("Failed to save subscription params for subscription: %v: %v", subscriptionId, err)
		}
		return nil
	}

	var err error
	if param.ProcessTimeout != 0 {
		err = routineStatus.SetSubParam("ProcessTimeout", param.ProcessTimeout)
	}

	if err == nil && param.ReceptionUri != "" {
		err = routineStatus.SetSubParam("ReceptionUri", param.ReceptionUri)
	}

	if err == nil {
		err = routineStatus.SetSubParam("AlerterEmails", param.AlerterEmails)
	}

	if err == nil {
		err = routineStatus.SetSubParam("AlerterPhoneNumbers", param.AlerterPhoneNumbers)
	}

	if err == nil {
		err = routineStatus.SetSubParam("AlerterReceiver", param.AlerterReceiver)
	}

	if err == nil {
		err = routineStatus.SetSubParam("ReceiveBenchMsgs", param.ReceiveBenchMsgs)
	}

	if err == nil {
		err = routineStatus.SetSubParam("AlerterEnabled", param.AlerterEnabled)
	}

	if err == nil {
		err = routineStatus.SetSubParam("IntervalOfErrorMonitorAlert", param.IntervalOfErrorMonitorAlert)
	}

	if err == nil {
		err = routineStatus.SetSubParam("SubscriptionTotalFailureAlertThreshold", param.SubscriptionTotalFailureAlertThreshold)
	}

	if err == nil {
		err = routineStatus.SetSubParam("MessageFailureAlertThreshold", param.MessageFailureAlertThreshold)
	}

	if err == nil {
		err = routineStatus.SetSubParam("MessageBlockedAlertThreshold", param.MessageBlockedAlertThreshold)
	}

	if err == nil {
		err = routineStatus.SetSubParam("AlarmInterval", param.AlarmInterval)
	}

	if err == nil {
		err = routineStatus.SetSubParam("DropMessageThreshold", param.DropMessageThreshold)
	}

	if err == nil {
		err = routineStatus.SetSubParam("DropMessageThresholdAction", param.DropMessageThresholdAction)
	}

	if err != nil {
		return err
	}

	var (
		coCount        = routineStatus.GetCoCount()
		coCountOfRetry = routineStatus.GetCoCountOfRetry()
	)

	var sig SubSenderRoutineChanSig
	if param.Concurrency != coCount {
		var diff uint32
		if param.Concurrency > coCount {
			sig = SENDER_ROUTINE_SIG_INCREASE_ROUTINE
			diff = param.Concurrency - coCount
		} else {
			sig = SENDER_ROUTINE_SIG_DECREASE_ROUTINE
			diff = coCount - param.Concurrency
		}

		for ; diff > 0; diff-- {
			go func(ch *chan SubSenderRoutineChanSig, sig SubSenderRoutineChanSig) {
				*ch <- sig
			}(&routineStatus.sigChan, sig)
		}
		routineStatus.SetSubParam("Concurrency", param.Concurrency)
	}

	if param.ConcurrencyOfRetry != coCountOfRetry {
		var diff uint32
		if param.ConcurrencyOfRetry > coCountOfRetry {
			sig = SENDER_ROUTINE_SIG_INCREASE_ROUTINE_FOR_RETRY
			diff = param.ConcurrencyOfRetry - coCountOfRetry
		} else {
			sig = SENDER_ROUTINE_SIG_DECREASE_ROUTINE_FOR_RETRY
			diff = coCountOfRetry - param.ConcurrencyOfRetry
		}

		for ; diff > 0; diff-- {
			go func(ch *chan SubSenderRoutineChanSig, sig SubSenderRoutineChanSig) {
				*ch <- sig
			}(&routineStatus.sigChan, sig)
		}
		routineStatus.SetSubParam("ConcurrencyOfRetry", param.ConcurrencyOfRetry)
	}
	err = routineStatus.SetSubParam("IntervalOfSending", param.IntervalOfSending)
	if err != nil {
		return err
	}
	err = routineStatus.subParams.Store(subscriptionId)
	if err != nil {
		logger.GetLogger("WARN").Printf("Failed to save subscription params for subscription: %v: %v", subscriptionId, err)
	}

	subscriptionBenchCfg[subscriptionId] = param.ReceiveBenchMsgs

	return nil
}
