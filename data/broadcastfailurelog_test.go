package data

import (
	"errors"
	"fmt"
	"medispatcher/config"
	"strconv"
	"testing"
	"time"
)

func init() {
	config.Setup()
	db, err := GetDb()
	if err != nil {
		panic(fmt.Sprintf("Failed to get db: %v", err))
	}
	sqlStr := `DELETE FROM %s
	WHERE EXISTS(
		SELECT t2.class_id FROM %s t2 WHERE t2.class_key = ? AND %s.message_class_id=t2.class_id
		 )`
	sqlStr = fmt.Sprintf(sqlStr, DB_TABLE_BROADCAST_FAILURE_LOG, DB_TABLE_MESSAGE_CLASSES, DB_TABLE_BROADCAST_FAILURE_LOG)
	_, err = db.Conn.Exec(sqlStr, "test")
	if err != nil {
		panic(fmt.Sprintf("Failed to init failure log data: %v:\nSQL: %v", err, sqlStr))
	}
}

func TestFailureLogExists(t *testing.T) {
	db, err := GetDb()
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to get db: %v", err))
		return
	}
	defer db.Release()
	testLogId := 999999
	_, err = db.Conn.Exec("DELETE FROM "+DB_TABLE_BROADCAST_FAILURE_LOG+" WHERE log_id=?", testLogId)
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	exists, err := FailureLogExists(uint64(testLogId))
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	if exists {
		t.Error(errors.New(fmt.Sprintf("Not expect log %v to be existing.", testLogId)))
		t.Fail()
		return
	}
}

func TestLogNewFailure(t *testing.T) {
	logId, err := testLogFailure()
	if err == nil {
		if logId < uint64(1) {
			err = errors.New("logId is expected to be larger than 1")
		}
	}
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	return
}

func TestUpdateNewFailureLog(t *testing.T) {
	var (
		logId, newLogId uint64
		err             error
		oldLog          FailureLogRecord
		subscription    SubscriptionRecord
		message         MessageStuct
	)

	logId, err = testLogFailure()
	if err == nil {
		oldLog, err = GetFailureLogInfoById(logId)
	}

	if err == nil {
		message = MessageStuct{
			MsgKey: "test",
			Body:   "{\"test_data\":[2,3]}",
			Sender: "test",
			Time:   float64(time.Now().UnixNano()) * 1E-9,
		}
		subscription, err = GetSubscriptionById(oldLog.Subscription_id)
	}

	if err == nil {
		newLogId, err = LogFailure(message, subscription, oldLog.Last_failure_message, logId, oldLog.Job_id)
		if err == nil {
			if newLogId != logId {
				err = errors.New("Expect newLogId and logId to be identical.")
			}
		}
	}

	if err != nil {
		t.Error(err)
		t.Fail()
	}

}

func testLogFailure() (logId uint64, err error) {
	message := MessageStuct{
		MsgKey:      "test",
		Body:        "{\"test_data\":[2,3]}",
		Sender:      "test",
		Time:        float64(time.Now().UnixNano()) * 1E-9,
		OriginJobId: uint64(9898989),
	}
	subs, err := GetSubscriptionsByTopicFromDb("test")
	if err != nil || len(subs) < 1 {
		err = errors.New(fmt.Sprintf("Failed to get test subscriptions: %v", err))
		return
	}
	sub := subs[0]
	jobId := strconv.Itoa(int(time.Now().Unix())) + strconv.Itoa(int(message.OriginJobId)) + strconv.Itoa(int(sub.Subscription_id))
	logId, err = LogFailure(message, sub, "error for test only.", uint64(0), jobId)
	return
}

func TestSetFinalStatusOfFailureLog(t *testing.T) {
	var log FailureLogRecord
	logId, err := testLogFailure()
	if err == nil {
		err = SetFinalStatusOfFailureLog(logId, uint8(1), uint8(1), 2)
	}
	if err == nil {
		log, err = GetFailureLogInfoById(logId)
		if err != nil {
			err = errors.New(fmt.Sprintf("Failed to get log: %v", err))
		} else if log.Log_id != logId || log.Retry_times != 2 || log.Final_status != uint8(1) || log.Alive != int8(1) {
			err = errors.New("Final status of log incorrect!")
		}
	}
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	return
}
