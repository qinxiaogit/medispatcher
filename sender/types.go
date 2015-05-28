package sender

import (
	"errors"
	"fmt"
	"medispatcher/config"
	"medispatcher/data"
	"reflect"
)

// Signal of subscription sender routine channel
type SubSenderRoutineChanSig int8

type StatusOfSubSenderRoutine struct {
	subscription *data.SubscriptionRecord
	// count of goroutines/concurrences for sending the subscription.
	coCount uint16
	// count of goroutines/concurrences for sending the subscription as retry.
	coCountOfRetry uint16
	exited         bool
	// 1 increase a sender routine, 2 decrease a sender routine,
	// 3 exit all sender routines, 4 increase a sender for retry routine sender,
	// 5 decrease a sender for retry routine sender.
	sigChan   chan SubSenderRoutineChanSig
	subParams *SubscriptionParams
	rwLock    *chan int8
}

func (s *StatusOfSubSenderRoutine) lock() {
	*(*s).rwLock <- 1
}

func (s *StatusOfSubSenderRoutine) unlock() {
	<-*(*s).rwLock
}

func (s *StatusOfSubSenderRoutine) SetExited() {
	s.lock()
	(*s).exited = true
	s.unlock()
}

func (s *StatusOfSubSenderRoutine) Exited() bool {
	s.lock()
	ex := (*s).exited
	s.unlock()
	return ex
}

func (s *StatusOfSubSenderRoutine) SetCoCount(c uint16) {
	s.lock()
	(*s).coCount = c
	s.unlock()
}

func (s *StatusOfSubSenderRoutine) IncreaseCoCount(c uint16) uint16 {
	s.lock()
	(*s).coCount += c
	nc := (*s).coCount
	s.unlock()
	return nc
}

func (s *StatusOfSubSenderRoutine) DecreaseCoCount(c uint16) uint16 {
	s.lock()
	(*s).coCount -= c
	nc := (*s).coCount
	s.unlock()
	return nc
}

func (s *StatusOfSubSenderRoutine) SetCoCountOfRetry(c uint16) {
	s.lock()
	(*s).coCountOfRetry = c
	s.unlock()
}

func (s *StatusOfSubSenderRoutine) InCoCountOfRetry(c uint16) uint16 {
	s.lock()
	(*s).coCountOfRetry += c
	nc := (*s).coCountOfRetry
	s.unlock()
	return nc
}

func (s *StatusOfSubSenderRoutine) DecreaseCoCountOfRetry(c uint16) uint16 {
	s.lock()
	(*s).coCountOfRetry -= c
	nc := (*s).coCountOfRetry
	s.unlock()
	return nc
}

func (s *StatusOfSubSenderRoutine) GetSubParams() SubscriptionParams {
	s.lock()
	params := *(*s).subParams
	s.unlock()
	return params
}

func (s *StatusOfSubSenderRoutine) SetSubParam(n string, v interface{}) error {
	s.lock()
	defer s.unlock()
	rf := reflect.ValueOf(s.subParams).Elem()
	field := rf.FieldByName(n)
	if !field.IsValid() {
		return errors.New(fmt.Sprintf("invalid param name '%v' for %v.", n, rf.Type().Name()))
	}
	vType := reflect.TypeOf(v)
	if field.Type().Name() != vType.Name() {
		return errors.New(fmt.Sprintf("invalid type '%v' for param '%v', exepcts: %v", vType.Name(), n, field.Type().Name()))
	}
	field.Set(reflect.ValueOf(v))
	return nil
}

// Parameters of the subscription
type SubscriptionParams struct {
	Concurrency        uint16
	ConcurrencyOfRetry uint16
	IntervalOfSending  uint16
	// Process timeout in milliseconds
	ProcessTimeout uint16
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
			case "ConcurrencyOfRetry", "Concurrency", "IntervalOfSending":
				var v uint16
				if vf, ok := d.(float64); !ok {
					err = errors.New(fmt.Sprintf("Failed to load params: %s type assertion failed", n))
					return
				} else {
					v = uint16(vf)
				}
				reflect.ValueOf(sp).Elem().FieldByName(n).Set(reflect.ValueOf(v))
			}
		}
	}
	return
}

// Store subscription params to local data.
func (sp *SubscriptionParams) Store(subscriptionId int32) error {
	return config.SaveConfig(sp.getFileName(subscriptionId), *sp)
}

type SenderRoutineStats struct {
	lockChan      *chan bool
	routineStatus map[int32]*StatusOfSubSenderRoutine
}

func (srs *SenderRoutineStats) lock() {
	if (*srs).lockChan == nil {
		ch := make(chan bool, 1)
		(*srs).lockChan = &ch
	}
	*(*srs).lockChan <- true
}

func (srs *SenderRoutineStats) unlock() {
	<-*(*srs).lockChan
}

// addStatus add a routine status by subscription ID
func (srs *SenderRoutineStats) addStatus(subscriptionId int32, status *StatusOfSubSenderRoutine) error {
	srs.lock()
	defer srs.unlock()
	if _, exists := (*srs).routineStatus[subscriptionId]; exists {
		return errors.New(fmt.Sprintf("Status of subscription(%v) already exists.", subscriptionId))
	}
	(*srs).routineStatus[subscriptionId] = status
	return nil
}

func (srs *SenderRoutineStats) setStatus(subscriptionId int32, status *StatusOfSubSenderRoutine) {
	srs.lock()
	defer srs.unlock()
	(*srs).routineStatus[subscriptionId] = status
}

// statusExists checks if a subscription's sender routine status has been recorded.
func (srs *SenderRoutineStats) statusExists(subscriptionId int32) bool {
	srs.lock()
	defer srs.unlock()
	_, exists := (*srs).routineStatus[subscriptionId]
	return exists
}

func (srs *SenderRoutineStats) getStatus(subscriptionId int32) *StatusOfSubSenderRoutine {
	return (*srs).routineStatus[subscriptionId]
}
