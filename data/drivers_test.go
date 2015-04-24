package data

import (
	"errors"
	"fmt"
	"medispatcher/config"
	"testing"
	"time"
)

func init() {
	config.Setup()
}
func TestDbQuery(t *testing.T) {
	db, err := GetDb()
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	sql := fmt.Sprintf(`SELECT t2.class_key, t1.* FROM %s t1
	INNER JOIN %s t2
	ON(t1.message_class_id=t2.class_id AND t2.class_key=?)`, DB_TABLE_SUBSCRIPTIONS, DB_TABLE_MESSAGE_CLASSES)
	rows, err := db.Query(sql, "test")
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	defer rows.Close()
	var class_key string
	var message_class_id int64
	var subscription_id int64
	var subscriber_id int64
	var reception_channel string
	var status int8
	var subscribe_time uint
	var timeout uint16
	var result []map[string]interface{}
	for rows.Next() {
		err := rows.Scan(&class_key, &message_class_id, &subscription_id, &subscriber_id,
			&reception_channel, &status, &subscribe_time, &timeout)
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		result = append(result, map[string]interface{}{
			"class_key":         class_key,
			"message_class_id":  message_class_id,
			"subscription_id":   subscription_id,
			"subscriber_id":     subscriber_id,
			"reception_channel": reception_channel,
			"status":            status,
			"subscribe_time":    subscribe_time,
			"timeout":           timeout,
		})
	}

}

func TestInsert(t *testing.T) {
	db, err := GetDb()
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	data := map[string]interface{}{
		"class_name":  "测试消息２",
		"class_key":   "test-class2",
		"comment":     "Juset for test , please delete it later",
		"create_time": time.Now().Unix(),
	}
	c, err := db.Insert(DB_TABLE_MESSAGE_CLASSES, data)
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	lastInsertId, err := c.LastInsertId()
	if err == nil {
		t.Logf("inserted: %v\n", lastInsertId)
		_, err = db.Conn.Exec("DELETE FROM "+DB_TABLE_MESSAGE_CLASSES+" WHERE class_id=?", lastInsertId)
	}
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
}

func TestRedisCommand(t *testing.T) {
	var re interface{}
	redis, err := GetRedis()
	lName := config.GetConfig().MsgQueueFaultToleranceListNamePrefix + ":test2"
	testM := map[string]string{"a1": "a", "b1": "b"}
	re, err = redis.Do("rpush", lName, testM)
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}

	var keys []interface{}
	var ok bool
	if err == nil {
		re, err = redis.Do("keys", config.GetConfig().MsgQueueFaultToleranceListNamePrefix+"*")
		keys, ok = re.([]interface{})
		if !ok {
			t.Error("keys assertion failed")
			t.Fail()
			return
		}
	}
	keys = append(keys, "3")
	re, err = redis.Do("BRPOP", keys...)
	if err == nil {
		_, ok := re.([]interface{})[1].(map[interface{}]interface{})
		if !ok {
			err = errors.New("serialized redis store not success")
		}
	}

	if err != nil {
		t.Error(err)
		t.Fail()
	}

	keyName := "Testkey_Of_" + config.GetConfig().MsgQueueFaultToleranceListNamePrefix
	_, err = redis.Do("set", keyName, testM)
	if err == nil {
		re, err=redis.Do("get", keyName)
		_, ok := re.(map[interface{}]interface{})
		if !ok {
			err = errors.New("serialized redis store not success")
		}
	}
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	return
}
