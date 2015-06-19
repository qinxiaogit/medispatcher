package sender
import (
	"errors"
	"fmt"
)


// SenderRoutineStats holds the statistics of all sender routines that handles the subscriptions.
type SenderRoutineStats struct {
	lockChan      *chan bool
	routineStatus map[int32]*StatusOfSubSenderRoutine
}

func (srs *SenderRoutineStats) lock() {
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

func (srs *SenderRoutineStats) removeStatus(subscriptionId int32) {
	srs.lock()
	defer srs.unlock()
	delete((*srs).routineStatus, subscriptionId)
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

func (srs *SenderRoutineStats) getHandlingSubscriptionIds() []int32 {
	ids := []int32{}
	for id, _ := range (*srs).routineStatus {
		ids = append(ids, id)
	}
	return ids
}