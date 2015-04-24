package broker

import (
	"errors"
	"fmt"
	"log"
	"testing"
)

const (
	Test_Queue_Name_1 = "medispatcher_test_queue_1"
	Test_Message_1    = "测试消息１."
)

var testConfig = Config{Addr: "127.0.0.1:11300", Type: "beanstalk"}

func init() {
	log.SetFlags(log.Lshortfile)
	c, err := newBroker()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize beanstalk client: %v", err))
	}
	for {
		c.Watch(Test_Queue_Name_1)
		rJobId, _, err := c.ReserveWithTimeout(0)
		if err != nil {
			break
		} else {
			fmt.Printf("Deleting previous test job: %v\n", rJobId)
			c.Delete(rJobId)
		}
	}
}

func newBroker() (Broker, error) {
	br, err := New(testConfig)
	return br, err
}

func pubJob() (err error, br Broker, jobId uint64) {
	br, err = newBroker()
	if err == nil {
		err = br.Use(Test_Queue_Name_1)
		if err == nil {
			jobId, err = br.Pub(1025, 0, 30, []byte(Test_Message_1))
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
	err, br, jobId := pubJob()
	if err == nil {
		br.Delete(jobId)
	}
	if err != nil {
		re.Error(err)
		re.Fail()
	}
}

func TestReserve(re *testing.T) {
	var jobId uint64
	err, br, pJobId := pubJob()
	if err == nil {
		err = br.Watch(Test_Queue_Name_1)
	}

	if err == nil {
		for {
			jobId, _, err = br.ReserveWithTimeout(1)
			if err != nil {
				err = errors.New(fmt.Sprintf("Failed to get job previously putted: %v", err))
				break
			} else if jobId == pJobId {
				br.Delete(jobId)
				break
			}
		}
	}
	if err != nil {
		re.Error(err)
		re.Fail()
	}
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
}
