package sender

import (
	"errors"
	"fmt"
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
