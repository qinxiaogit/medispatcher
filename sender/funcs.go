package sender

import (
	"errors"
	"fmt"
	"medispatcher/logger"
	"medispatcher/config"
)

// Stop stops the senders.
func Stop(returnCh *chan string) {
	// wait for the exit signal to be checked. taken by shouldExit
	exitChan <- int8(1)
	// wait for the process exit. token by StartAndWait
	exitChan <- int8(1)
	*returnCh <- "msgsender"
}

func shouldExit() bool {
	exitingCheckLock <- 1
	r := exiting
	if !r {
		select {
		case <-exitChan:
			exiting = true
			break
		default:
		}
		r = exiting
	}
	<-exitingCheckLock
	return r
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
	} else if param.ProcessTimeout <=0 {
		param.ProcessTimeout = config.GetConfig().DefaultMaxMessageProcessTime
	}

	if routineStatus == nil{
		err := param.Store(subscriptionId)
		if err != nil {
			logger.GetLogger("WARN").Printf("Failed to save subscription params for subscription: %v: %v", subscriptionId, err)
		}
		return nil
	}

	routineStatus.SetSubParam("ProcessTimeout", param.ProcessTimeout)
	routineStatus.lock()
	var (
		coCount        = routineStatus.coCount
		coCountOfRetry = routineStatus.coCountOfRetry
	)
	routineStatus.unlock()

	var sig SubSenderRoutineChanSig
	if param.Concurrency != coCount {
		var diff uint16
		if param.Concurrency > coCount {
			sig = SENDER_ROUTINE_SIG_INCREASE_ROUTINE
			diff = param.Concurrency - coCount
		} else {
			sig = SENDER_ROUTINE_SIG_DECREASE_ROUTINE
			diff = coCount - param.Concurrency
		}

		for ; diff > 0; diff-- {
			go func(ch *chan SubSenderRoutineChanSig, sig SubSenderRoutineChanSig){
				*ch <- sig
			}(&routineStatus.sigChan, sig)
		}
		routineStatus.SetSubParam("Concurrency", param.Concurrency)
	}

	if param.ConcurrencyOfRetry != coCountOfRetry {
		var diff uint16
		if param.ConcurrencyOfRetry > coCountOfRetry {
			sig = SENDER_ROUTINE_SIG_INCREASE_ROUTINE_FOR_RETRY
			diff = param.ConcurrencyOfRetry - coCountOfRetry
		} else {
			sig = SENDER_ROUTINE_SIG_DECREASE_ROUTINE_FOR_RETRY
			diff = coCountOfRetry - param.ConcurrencyOfRetry
		}

		for ; diff > 0; diff-- {
			go func(ch *chan SubSenderRoutineChanSig, sig SubSenderRoutineChanSig){
				*ch <- sig
			}(&routineStatus.sigChan, sig)
		}
		routineStatus.SetSubParam("ConcurrencyOfRetry", param.ConcurrencyOfRetry)
	}
	err := routineStatus.SetSubParam("IntervalOfSending", param.IntervalOfSending)
	if err != nil {
		return err
	}
	err = routineStatus.subParams.Store(subscriptionId)
	if err != nil {
		logger.GetLogger("WARN").Printf("Failed to save subscription params for subscription: %v: %v", subscriptionId, err)
	}
	return nil
}
