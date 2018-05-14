package sender

import (
	"encoding/json"
	"fmt"
	"medispatcher/config"
	"medispatcher/data"
	"medispatcher/logger"
)

// SubscriptionParams Parameters of the subscription
type SubscriptionParams struct {
	data.SubscriptionParams

	// 错误次数计数间隔，单位: 秒，默认180秒
	IntervalOfErrorMonitorAlert int64
	// 发送失败阈值: 0<n<10，默认7次
	MessageFailureAlertThreshold uint16
	// 失败次数，默认120
	SubscriptionTotalFailureAlertThreshold int32
	// 消息堆积的报警极限，默认5000
	MessageBlockedAlertThreshold int64
}

func NewSubscriptionParams() *SubscriptionParams {
	return &SubscriptionParams{
		SubscriptionParams: data.SubscriptionParams{AlerterEnabled: true,
			Concurrency:        config.GetConfig().SendersPerChannel,
			ConcurrencyOfRetry: config.GetConfig().SendersPerRetryChannel,
			IntervalOfSending:  config.GetConfig().IntervalOfSendingForSendRoutine,
		},
		IntervalOfErrorMonitorAlert:            INTERVAL_OF_ERROR_MONITOR_ALERT,
		MessageFailureAlertThreshold:           MESSAGE_FAILURE_ALERT_THRESHOLD,
		SubscriptionTotalFailureAlertThreshold: SUBSCRIPTION_TOTAL_FAILURE_ALERT_THRESHOLD,
		MessageBlockedAlertThreshold:           MESSAGE_BLOCKED_ALERT_THRESHOLD,
	}
}

func (sp *SubscriptionParams) setAlertOption() {
	if sp.IntervalOfErrorMonitorAlert <= 0 {
		sp.IntervalOfErrorMonitorAlert = INTERVAL_OF_ERROR_MONITOR_ALERT
	}
	if sp.MessageFailureAlertThreshold == 0 {
		sp.MessageFailureAlertThreshold = MESSAGE_FAILURE_ALERT_THRESHOLD
	}
	if sp.SubscriptionTotalFailureAlertThreshold <= 0 {
		sp.SubscriptionTotalFailureAlertThreshold = SUBSCRIPTION_TOTAL_FAILURE_ALERT_THRESHOLD
	}
	if sp.MessageBlockedAlertThreshold <= 0 {
		sp.MessageBlockedAlertThreshold = MESSAGE_BLOCKED_ALERT_THRESHOLD
	}
}

// getFieName returns the name of the file that stores the subscription params.
func (sp *SubscriptionParams) getFileName(subscriptionId int32) string {
	return fmt.Sprintf("subscription_params_%v", subscriptionId)
}

// RefreshAndLoad refresh local caches and load the latest values of the subscription parameters.
func (sp *SubscriptionParams) RefreshAndLoad(subscriptionId int32) error {
	err := sp.LoadFromDb(subscriptionId)
	if err != nil {
		return err
	}
	err = sp.Store(subscriptionId)
	if err != nil {
		logger.GetLogger("WARN").Printf("Failed to store subscription parameters to local storage: %v", err)
	}
	return nil
}

// Load subscription params from local data or database.
func (sp *SubscriptionParams) Load(subscriptionId int32) (err error) {
	defer func() {
		nErr := recover()
		if nErr != nil {
			err = fmt.Errorf("Failed to load params: %v", nErr)
		}
		sp.setAlertOption()
	}()
	sp.SubscriptionId = subscriptionId
	var data []byte
	data, err = config.GetConfigDataFromDisk(sp.getFileName(subscriptionId))
	if err != nil {
		err = sp.LoadFromDb(subscriptionId)
		if err == nil {
			sp.Store(subscriptionId)
		}
	} else {
		e := json.Unmarshal(data, sp)
		if e != nil {
			err = fmt.Errorf("Failed to load params: %v", e)
		}
	}
	return
}

// Store subscription params to local data.
func (sp *SubscriptionParams) Store(subscriptionId int32) error {
	return config.SaveConfig(sp.getFileName(subscriptionId), *sp)
}

func (sp *SubscriptionParams) LoadFromDb(subscriptionId int32) error {
	sub, err := data.GetSubscriptionParamsById(subscriptionId)
	if err != nil {
		return err
	}
	sp.SubscriptionParams = sub
	return nil
}
