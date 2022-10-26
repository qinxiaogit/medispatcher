package sender

import (
	"errors"
	"fmt"
	"github.com/qinxiaogit/medispatcher/data"
	"reflect"
	"sync"
)

// Signal of subscription sender routine channel
type SubSenderRoutineChanSig int8

type StatusOfSubSenderRoutine struct {
	sync.RWMutex
	subscription *data.SubscriptionRecord
	// count of goroutines/concurrences for sending the subscription.
	coCount uint32
	// count of goroutines/concurrences for sending the subscription as retry.
	coCountOfRetry uint32
	exited         bool
	// 1 increase a sender routine, 2 decrease a sender routine,
	// 3 exit all sender routines, 4 increase a sender for retry routine sender,
	// 5 decrease a sender for retry routine sender.
	sigChan   chan SubSenderRoutineChanSig
	subParams *SubscriptionParams
}

func (s *StatusOfSubSenderRoutine) SetExited() {
	s.Lock()
	s.exited = true
	s.Unlock()
}

func (s *StatusOfSubSenderRoutine) Exited() bool {
	s.Lock()
	ex := s.exited
	s.Unlock()
	return ex
}

func (s *StatusOfSubSenderRoutine) SetCoCount(c uint32) {
	s.Lock()
	s.coCount = c
	s.Unlock()
}

func (s *StatusOfSubSenderRoutine) GetCoCount()uint32 {
	s.Lock()
	defer s.Unlock()
	return s.coCount
}

func (s *StatusOfSubSenderRoutine) IncreaseCoCount(c uint32) uint32 {
	s.Lock()
	s.coCount += c
	nc := s.coCount
	s.Unlock()
	return nc
}

func (s *StatusOfSubSenderRoutine) DecreaseCoCount(c uint32) uint32 {
	s.Lock()
	s.coCount -= c
	nc := s.coCount
	s.Unlock()
	return nc
}

func (s *StatusOfSubSenderRoutine) SetCoCountOfRetry(c uint32) {
	s.Lock()
	s.coCountOfRetry = c
	s.Unlock()
}


func (s *StatusOfSubSenderRoutine) GetCoCountOfRetry()uint32 {
	s.Lock()
	defer s.Unlock()
	return s.coCountOfRetry
}

func (s *StatusOfSubSenderRoutine) InCoCountOfRetry(c uint32) uint32 {
	s.Lock()
	s.coCountOfRetry += c
	nc := s.coCountOfRetry
	s.Unlock()
	return nc
}

func (s *StatusOfSubSenderRoutine) DecreaseCoCountOfRetry(c uint32) uint32 {
	s.Lock()
	s.coCountOfRetry -= c
	nc := s.coCountOfRetry
	s.Unlock()
	return nc
}

func (s *StatusOfSubSenderRoutine) GetSubParams() SubscriptionParams {
	s.Lock()
	params := *s.subParams
	s.Unlock()
	return params
}

func (s *StatusOfSubSenderRoutine) SetSubParam(n string, v interface{}) error {
	s.Lock()
	defer s.Unlock()
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
