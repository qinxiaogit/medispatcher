package handlers

import (
	"medispatcher/config"
	"medispatcher/rpc"
)

type GetDefaultSubscriptionSettings struct {
}

func init() {
	rpc.RegisterHandlerRegister("GetDefaultSubscriptionSettings", GetDefaultSubscriptionSettings{})
}

// Get the default (including maximum values) params that affects the sender routine performances.
// args {}. Do not need any arguments.
// RETURN: {}. currently these params are available:
//     "MaxSendersPerChannel", "MaxSendersPerRetryChannel",IntervalOfSendingForSendRoutine
//     (default) "SendersPerChannel", (default)"SendersPerRetryChannel", (default) "MaxMessageProcessTime",
//     "DefaultMaxMessageProcessTime"
func (i GetDefaultSubscriptionSettings) Process(args map[string]interface{}) (re interface{}, err error) {
	params := map[string]interface{}{
		"MaxSendersPerChannel":      config.GetConfig().MaxSendersPerChannel,
		"MaxSendersPerRetryChannel": config.GetConfig().MaxSendersPerRetryChannel,
		"SendersPerChannel":         config.GetConfig().SendersPerChannel,
		"SendersPerRetryChannel":    config.GetConfig().SendersPerRetryChannel,
		"MaxMessageProcessTime":     config.GetConfig().MaxMessageProcessTime,
		"DefaultMaxMessageProcessTime": config.GetConfig().DefaultMaxMessageProcessTime,
		"IntervalOfSendingForSendRoutine": config.GetConfig().IntervalOfSendingForSendRoutine,
	}

	return params, nil
}
