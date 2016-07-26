package broker

import (
	"testing"
	"medispatcher/broker/beanstalk"
	"github.com/labstack/gommon/log"
	"time"
	"fmt"
)

func TestPool(t *testing.T){
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
	timeout := time.After(time.Second * 2)
	msgChan := brRs.Reserve()
	FM:
	for{
		println("ABCD")
		select {
			case msg := <- msgChan:
				t.Logf("Fetched Message: ID: %v, SrvAddr: %v, Data: %s", msg.Id, msg.QueueServer, msg.Body)
				err = brWD.Delete(msg)
				if err != nil {
					t.Error(err)
				}
			case <- timeout:
				break FM
		}
	}
	brWD.Close(false)
	brRs.Close(true)

}