package sender
import (
	"medispatcher/config"
	"errors"
	"fmt"
	"reflect"
	"medispatcher/data"
	"medispatcher/logger"
)

// Parameters of the subscription
type SubscriptionParams struct {
	data.SubscriptionParams
}

func NewSubscriptionParams() *SubscriptionParams {
	return &SubscriptionParams{
		SubscriptionParams: data.SubscriptionParams{AlerterEnabled:     true,
			Concurrency:        config.GetConfig().SendersPerChannel,
			ConcurrencyOfRetry: config.GetConfig().SendersPerRetryChannel,
			IntervalOfSending:  config.GetConfig().IntervalOfSendingForSendRoutine,
		},
	}
}

// getFieName returns the name of the file that stores the subscription params.
func (sp *SubscriptionParams) getFileName(subscriptionId int32) string {
	return fmt.Sprintf("subscription_params_%v", subscriptionId)
}

// RefreshAndLoad refresh local caches and load the latest values of the subscription parameters.
func (sp *SubscriptionParams) RefreshAndLoad(subscriptionId int32) error{
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
	defer func(){
		nErr := recover()
		if nErr != nil {
			err = errors.New(fmt.Sprintf("Failed to load params: %v", nErr))
		}
	}()
	sp.SubscriptionId = subscriptionId
	var data interface{}
	data, err = config.GetConfigFromDisk(sp.getFileName(subscriptionId))
	if err != nil {
		err = sp.LoadFromDb(subscriptionId)
		if err == nil {
			sp.Store(subscriptionId)
		}
		return
	}else {
		params, ok := data.(map[string]interface{})
		if !ok {
			err = errors.New("Failed to load params: type assertion failed.")
			return
		}
		for n, d := range params {
			switch n {
			case "SubscriptionId":
				if vf, ok := d.(float64); !ok {
					err = errors.New(fmt.Sprintf("Invalid subscription parameter data type: ", n))
					return
				} else {
					d = int32(vf)
				}
			case  "ConcurrencyOfRetry", "Concurrency", "ProcessTimeout", "IntervalOfSending":
				if vf, ok := d.(float64); !ok {
					err = errors.New(fmt.Sprintf("Invalid subscription parameter data type: ", n))
					return
				} else {
					d = uint32(vf)
				}
			}
			reflect.ValueOf(sp).Elem().FieldByName(n).Set(reflect.ValueOf(d))
		}
	}
	return
}

// Store subscription params to local data.
func (sp *SubscriptionParams) Store(subscriptionId int32) error {
	return config.SaveConfig(sp.getFileName(subscriptionId), *sp)
}

func (sp *SubscriptionParams) LoadFromDb(subscriptionId int32) error{
	sub, err := data.GetSubscriptionParamsById(subscriptionId)
	if err != nil {
		return err
	}
	sp.SubscriptionParams = sub
	return nil
}