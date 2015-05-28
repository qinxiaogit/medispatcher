package handlers

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"medispatcher/rpc"
	"medispatcher/sender"
	"reflect"
)

type SetSubscriptionParams struct {
	rwLock chan bool
}

func init() {
	rpc.RegisterHandlerRegister("SetSubscriptionParams", SetSubscriptionParams{rwLock: make(chan bool, 1)})
}

func (i SetSubscriptionParams) lock() {
	i.rwLock <- true
}

func (i SetSubscriptionParams) unlok() {
	<-i.rwLock
}

// Set the params that affects the sender routine performances.
// args {"SubscriptionId": 32,
//       "Params": {"Concurrency": 2, "ConcurrencyOfRetry": 1,  "IntervalOfSending": 211}
//      }
func (i SetSubscriptionParams) Process(args map[string]interface{}) (re interface{}, err error) {
	i.lock()
	defer func() {
		i.unlok()
		pErr := recover()
		if pErr != nil {
			err = errors.New(fmt.Sprintf("%v", pErr))
		}
	}()
	var subscriptionId int32
	var newParams map[string]interface{}
	var newParamsI interface{}
	var ok bool
	if _, ok = args["SubscriptionId"]; !ok {
		return nil, errors.New("argument 'SubscrpitonId' not exists!")
	} else if newParamsI, ok = args["Params"]; !ok {
		return nil, errors.New("empty Params!")
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

	if newParams, ok = newParamsI.(map[string]interface{}); !ok {
		return nil, errors.New("Invalid Params. Type map[string]interface{} is required!")
	}

	subParams := sender.SubscriptionParams{}
	rElem := reflect.ValueOf(&subParams).Elem()
	for n, v := range newParams {
		rElemField := rElem.FieldByName(n)
		if rElemField.IsValid() {
			switch n {
			case "ConcurrencyOfRetry", "Concurrency", "IntervalOfSending":
				if vf, ok := v.(float64); !ok {
					err = errors.New(fmt.Sprintf("Param type error: %s: %v", n, v))
					return
				} else {
					v = uint16(vf)
				}
			}
			rElemField.Set(reflect.ValueOf(v))
		}
	}
	sErr := sender.SetSubscriptionParams(subscriptionId, subParams)
	return sErr == nil, sErr
}
