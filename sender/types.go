package sender
import("medispatcher/data")

// Signal of subscription sender routine channel
type SubSenderRoutineChanSig int8

type StatusOfSubSenderRoutine struct {
	subscription *data.SubscriptionRecord
	// count of goroutines/concurrences for sending the subscription.
	coCount int32
	// count of goroutines/concurrences for sending the subscription as retry.
	coCountOfRetry int32
	exited         bool
	// 1 increase a sender routine, 2 decrease a sender routine,
	// 3 exit all sender routines, 4 increase a sender for retry routine sender,
	// 5 decrease a sender for retry routine sender.
	sigChan chan SubSenderRoutineChanSig

	rwLock  *chan int8
}

func (s *StatusOfSubSenderRoutine) lock(){
	if (*s).rwLock == nil{
		l := make(chan int8, 1)
		(*s).rwLock = &l
	}
	*(*s).rwLock <- 1
}

func (s *StatusOfSubSenderRoutine) unlock(){
	<-*(*s).rwLock
}

func (s *StatusOfSubSenderRoutine) SetExited(){
	s.lock()
	(*s).exited = true
	s.unlock()
}

func (s *StatusOfSubSenderRoutine) Exited()bool{
	s.lock()
	ex := (*s).exited
	s.unlock()
	return ex
}

func (s *StatusOfSubSenderRoutine) SetCoCount(c int32) {
	s.lock()
	(*s).coCount = c
	s.unlock()
}

func (s *StatusOfSubSenderRoutine) IncreaseCoCount(c int32)int32 {
	s.lock()
	(*s).coCount += c
	nc := (*s).coCount
	s.unlock()
	return nc
}

func (s *StatusOfSubSenderRoutine) DecreaseCoCount(c int32)int32 {
	s.lock()
	(*s).coCount -= c
	nc := (*s).coCount
	s.unlock()
	return nc
}

func (s *StatusOfSubSenderRoutine) SetCoCountOfRetry(c int32) {
	s.lock()
	(*s).coCountOfRetry = c
	s.unlock()
}

func (s *StatusOfSubSenderRoutine) InCoCountOfRetry(c int32)int32 {
	s.lock()
	(*s).coCountOfRetry += c
	nc := (*s).coCountOfRetry
	s.unlock()
	return nc
}

func (s *StatusOfSubSenderRoutine) DecreaseCoCountOfRetry(c int32)int32 {
	s.lock()
	(*s).coCountOfRetry -= c
	nc := (*s).coCountOfRetry
	s.unlock()
	return nc
}