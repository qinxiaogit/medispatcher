package sender
import (
	"medispatcher/config"
	"errors"
	"fmt"
	"reflect"
)

// Parameters of the subscription
type SubscriptionParams struct {
	SubscriptionId     int32
	Concurrency        uint16
	ConcurrencyOfRetry uint16
	IntervalOfSending  uint16
	// Process timeout in milliseconds
	// ProcessTimeout is significant. Many checks relies on it, 0 means it has not a customized params, all params are in default value.
	ProcessTimeout      uint16
	ReceptionUri        string
	AlerterEmails       string
	AlerterPhoneNumbers string
	AlerterEnabled      bool
}

func NewSubscriptionParams() *SubscriptionParams {
	return &SubscriptionParams{
		AlerterEnabled:     true,
		Concurrency:        config.GetConfig().SendersPerChannel,
		ConcurrencyOfRetry: config.GetConfig().SendersPerRetryChannel,
		IntervalOfSending:  config.GetConfig().IntervalOfSendingForSendRoutine,
	}
}

// getFieName returns the name of the file that stores the subscription params.
func (sp *SubscriptionParams) getFileName(subscriptionId int32) string {
	return fmt.Sprintf("subscription_params_%v", subscriptionId)
}

// Load subscription params from local data.
func (sp *SubscriptionParams) Load(subscriptionId int32) (err error) {
	var data interface{}
	data, err = config.GetConfigFromDisk(sp.getFileName(subscriptionId))
	if err == nil {
		params, ok := data.(map[string]interface{})
		if !ok {
			err = errors.New("Failed to load params: type assertion failed.")
			return
		}
		for n, d := range params {
			switch n {
			case "SubscriptionId":
				if vf, ok := d.(float64); !ok {
					err = errors.New(fmt.Sprintf("Failed to load params: %s type assertion failed", n))
					return
				} else {
					d = int32(vf)
				}
				fallthrough
			case "ConcurrencyOfRetry", "Concurrency", "IntervalOfSending", "ProcessTimeout":
				if vf, ok := d.(float64); !ok {
					err = errors.New(fmt.Sprintf("Failed to load params: %s type assertion failed", n))
					return
				} else {
					d = uint16(vf)
				}
				fallthrough
			default:
				reflect.ValueOf(sp).Elem().FieldByName(n).Set(reflect.ValueOf(d))
			}
		}
	}
	return
}

// Store subscription params to local data.
func (sp *SubscriptionParams) Store(subscriptionId int32) error {
	return config.SaveConfig(sp.getFileName(subscriptionId), *sp)
}