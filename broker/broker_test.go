package broker

import (
	"errors"
	"fmt"
	"testing"
)

const (
	Test_Queue_Name_1 = "medispatcher_test_queue_1"
	Test_Message_1    = "测试消息１."
)

var testConfig = Config{Addr: "127.0.0.1:11300", Type: "beanstalk"}

func newBroker() (Broker, error) {
	br, err := New(testConfig)
	return br, err
}

func pubJob() (err error, br Broker, jobId uint64) {
	br, err = newBroker()
	if err == nil {
		err = br.Use(Test_Queue_Name_1)
		if err == nil {
			jobId, err = br.Pub("", 1025, 0, 30, []byte(Test_Message_1))
		}
	}
	return err, br, jobId
}

func TestNew(re *testing.T) {
	_, err := newBroker()
	if err != nil {
		re.Error(err)
		re.Fail()
	}
}

func TestPub(re *testing.T) {
	err, _, _ := pubJob()
	if err != nil {
		re.Error(err)
		re.Fail()
	}
}

func TestReserve(re *testing.T) {
	var jobId uint64
	var jobBody []byte
	err, br, pJobId := pubJob()
	if err == nil {
		err = br.Watch(Test_Queue_Name_1)
	}

	if err == nil {
		for {
			jobId, jobBody, err = br.ReserveWithTimeout(1)
			if err != nil {
				err = errors.New(fmt.Sprintf("Failed to get job previously putted: %v", err))
				break
			} else if jobId == pJobId {
				re.Logf("DELETING JOB: %v, DATA: %s, ERR: %v", jobId, jobBody, br.Delete(jobId))
				break
			}
		}
	}
	if err != nil {
		re.Error(err)
		re.Fail()
	}
	br.Close()
}

func TestStatsJob(t *testing.T) {
	var jobId uint64
	var stats map[string]interface{}
	err, br, jobId := pubJob()
	if err == nil {
		defer br.Delete(jobId)
		stats, err = br.Stats()
	}
	if err == nil {
		fmt.Printf("Job(%v) stats:\n", jobId)
		for k, v := range stats {
			fmt.Printf("%v: %v \n", k, v)
		}
	}
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	br.Close()
}
