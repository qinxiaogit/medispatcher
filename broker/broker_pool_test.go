package broker

import (
	"fmt"
	"medispatcher/broker/beanstalk"
	"testing"
	"time"

	"github.com/labstack/gommon/log"
)

func TestPool(t *testing.T) {
	brRs, err := beanstalk.NewSafeBrokerPool("127.0.0.1:11300,127.0.0.1:11301", 1)
	if err != nil {
		t.Error(err)
		return
	}

	brWD, err := beanstalk.NewSafeBrokerPool("127.0.0.1:11300,127.0.0.1:11301", 1)
	if err != nil {
		t.Error(err)
		return
	}

	for n := 10; n > 0; n-- {
		jobId, err := brWD.Pub(Test_Queue_Name_1, []byte(fmt.Sprintf("%v---%v", Test_Message_1, time.Now().UnixNano())), DEFAULT_MSG_PRIORITY, 0, DEFAULT_MSG_TTR)
		if err != nil {
			t.Error(err)
			return
		}
		log.Printf("Pub Job: %v", jobId)
	}
	brRs.Watch(Test_Queue_Name_1)
	timer := time.NewTimer(time.Second * 2)
	msgChan := brRs.Reserve()
	var deletedJobs = map[uint64]bool{}
FM:
	for {
		select {
		case msg := <-msgChan:
			if _, ok := deletedJobs[msg.Id]; ok {
				continue
			}
			timer.Reset(time.Second * 2)
			t.Logf("Fetched Message: ID: %v, SrvAddr: %v, Data: %s", msg.Id, msg.QueueServer, msg.Body)
			err = brWD.KickJob(Test_Queue_Name_1, msg)
			if err != nil {
				t.Errorf("kick msg failed: %+v failed : %v", msg.Id, err)
			} else {
				t.Logf("kicked: %v", msg.Id)
			}
			time.Sleep(time.Millisecond * 10)
			err = brWD.Delete(msg)
			if err != nil {
				t.Errorf("delete msg failed: %v, %v", msg.Id, err)
			} else {
				t.Logf("deleted: %v", msg.Id)
				deletedJobs[msg.Id] = true
			}
		case <-timer.C:
			break FM
		}
	}
	brWD.Close(false)
	brRs.Close(true)

}
