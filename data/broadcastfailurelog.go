package data

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
	"encoding/json"
)

type FailureLogRecord struct {
	Log_id                uint64
	Message_class_id      int32
	Message_class_key     string
	Message_class_name    string
	Subscription_id       int32
	Subscriber_id         int32
	Time                  int64
	Last_retry_time       int64
	Last_failure_message  string
	Final_status          uint8
	First_failure_message string
	Job_id                string
	Alive                 int8
	Message_time          float32
	Message_body          string
	Retry_times           uint16
	Reception_Channel     string
	Subscriber_name       string
	Subscriber_key        string
}

// Add or update a failure log. Invoke this function when the last sending is failed.
// jobId is unique jobid in format: {timestamp}-{MQ jobid}-{subscription_id}
func LogFailure(message MessageStuct, subscription SubscriptionRecord, errorMessage string, logId uint64, jobId string) (newLogId uint64, err error) {
	if logId >0 {
		_, err = FailureLogExists(logId)
		if err != nil {
			err = errors.New(fmt.Sprintf("Failed to check existing log: %v", err))
			return
		}
	}

	db, err := GetDb()
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to get db: %v", err))
		return
	}
	defer db.Release()

	// new failure log
	if logId < 1 {
		data := map[string]interface{}{}
		data["message_class_id"] = subscription.Message_class_id
		data["subscriber_id"] = subscription.Subscriber_id
		data["subscription_id"] = subscription.Subscription_id
		msgBody, dErr := json.Marshal(message.Body)
		if dErr != nil {
			data["message_body"] = ""
		} else {
			data["message_body"] = string(msgBody)
		}
		data["time"] = time.Now().Unix()
		data["message_time"] = message.Time
		data["last_failure_message"] = errorMessage
		data["first_failure_message"] = errorMessage
		data["job_id"] = jobId
		var re sql.Result
		re, err = db.Insert(DB_TABLE_BROADCAST_FAILURE_LOG, data)
		if err != nil {
			err = errors.New(fmt.Sprintf("Failed to insert new log: %v", err))
			return
		} else {
			var lastLogId int64
			lastLogId, err = re.LastInsertId()
			if err != nil {
				err = errors.New(fmt.Sprintf("Failed to get last log id: %v", err))
				return
			} else {
				logId = uint64(lastLogId)
			}
		}
	} else {
		sqlStr := fmt.Sprintf(`
		UPDATE %s SET
		last_failure_message=?
		WHERE log_id=?`, DB_TABLE_BROADCAST_FAILURE_LOG )
		_, err = db.Exec(sqlStr, errorMessage, logId)
		if err != nil {
			err = errors.New(fmt.Sprintf("Failed to update log: %v : SQL: %v", err, sqlStr))
		}
	}
	newLogId = logId
	return
}

func FailureLogExists(logId uint64) (bool, error) {
	sqlStr := fmt.Sprintf(`SELECT 1 FROM %s WHERE log_id=?`, DB_TABLE_BROADCAST_FAILURE_LOG)
	db, err := GetDb()
	if err != nil {
		return false, err
	}
	defer db.Release()
	rows, err := db.Query(sqlStr, logId)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if rows.Next() {
		var t int
		err = rows.Scan(&t)
		if err != nil {
			return false, err
		}
		return true, nil
	} else {
		return false, nil
	}
}

func GetFailureLogInfoById(logId uint64) (rec FailureLogRecord, err error) {
	var (
		db  *DB
		row *sql.Row
	)
	sqlStr := `
	SELECT a.log_id, a.message_class_id, a.subscriber_id, a.time, a.message_body,
	a.retry_times, a.last_retry_time, a.last_failure_message, a.final_status,
	a.first_failure_message,a.job_id, a.alive, a.message_time,a.subscription_id,
	d.reception_channel,
	b.subscriber_name,b.subscriber_key,
	c.class_name,c.class_key
	FROM %s a
	LEFT JOIN %s b on a.subscriber_id=b.subscriber_id
	LEFT JOIN %s c on a.message_class_id=c.class_id
	LEFT JOIN %s d on b.subscriber_id=d.subscriber_id and c.class_id=d.message_class_id
	WHERE a.log_id=?
	`
	sqlStr = fmt.Sprintf(sqlStr, DB_TABLE_BROADCAST_FAILURE_LOG, DB_TABLE_SUBSCRIBERS, DB_TABLE_MESSAGE_CLASSES, DB_TABLE_SUBSCRIPTIONS)
	db, err = GetDb()
	if err != nil {
		return
	}
	defer db.Release()
	row, err = db.QueryRow(sqlStr, logId)
	if err != nil {
		return
	}

	err = row.Scan(&rec.Log_id, &rec.Message_class_id, &rec.Subscriber_id, &rec.Time,
		&rec.Message_body, &rec.Retry_times, &rec.Last_retry_time, &rec.Last_failure_message,
		&rec.Final_status, &rec.First_failure_message, &rec.Job_id, &rec.Alive,
		&rec.Message_time, &rec.Subscription_id, &rec.Reception_Channel, &rec.Subscriber_name,
		&rec.Subscriber_key, &rec.Message_class_name, &rec.Message_class_key,
	)
	return
}

// Set the final status(include more stats) for a failure log. It's invoked after each sending of the subscription for the message.
// status indicates if the message is successfully handled by the target: failure 0, success 1
func SetFinalStatusOfFailureLog(logId uint64, status uint8, alive uint8, retryTimes uint16) (err error) {
	var db *DB
	sql := fmt.Sprintf(`
		UPDATE %s
		SET final_status=?, alive=?, retry_times=?, last_retry_time=?
		WHERE log_id=?
	`, DB_TABLE_BROADCAST_FAILURE_LOG)
	db, err = GetDb()
	if err != nil {
		return
	}
	defer db.Release()
	var last_retry_time int64
	if retryTimes > 0{
		last_retry_time = time.Now().Unix()
	}
	_, err = db.Exec(sql, status, alive, retryTimes, last_retry_time, logId)
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to set final status for failure log: %v : %v", logId, err))
	}
	return
}
