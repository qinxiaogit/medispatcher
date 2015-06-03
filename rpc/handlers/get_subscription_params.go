package handlers

import (
	"errors"
	"medispatcher/rpc"
	"medispatcher/sender"
)

type GetSubscriptionParams struct {
}

func init() {
	rpc.RegisterHandlerRegister("GetSubscriptionParams", GetSubscriptionParams{})
}

// Get the current customized params that affects the sender routine performances.
// args {"SubscriptionId": 32}
// RETURN: re {"Concurrency": 2, "ConcurrencyOfRetry": 1,  "IntervalOfSending": 211, "ProcessTimeout": 2000}
// ProcessTimeout is in milliseconds.
func (i GetSubscriptionParams) Process(args map[string]interface{}) (re interface{}, err error) {
	var subscriptionId int32
	var ok bool
	if _, ok = args["SubscriptionId"]; !ok {
		return nil, errors.New("argument 'SubscrpitonId' not exists!")
	}
	switch args["SubscriptionId"].(type) {
	case int64:
		subscriptionId = int32(args["SubscriptionId"].(int64))
	case float64:
		subscriptionId = int32(args["SubscriptionId"].(float64))
	case int32:
		subscriptionId = args["SubscriptionId"].(int32)
	default:
		return nil, errors.New("Invalid type of argument 'SubscrpitonId'!")
	}

	subParams := sender.SubscriptionParams{}
	subParams.Load(subscriptionId)
	return subParams, nil
}
