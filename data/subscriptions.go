package data

import (
	"database/sql"
	"fmt"
	"medispatcher/data/cache"
	"strconv"
)

type SubscriptionRecord struct {
	Class_key         string
	Message_class_id  int32
	Subscription_id   int32
	Subscriber_id     int32
	Reception_channel string
	Status            int8
	Subscribe_time    uint
	Timeout           uint32
}

// GetTopic will 返回mq的topic
func (s *SubscriptionRecord) GetTopic() string {
	if s != nil {
		return s.Class_key
	}
	return ""
}

// GetChannel will 返回mq的channel
func (s *SubscriptionRecord) GetChannel() string {
	if s != nil {
		return strconv.Itoa(int(s.Subscription_id))
	}
	return ""
}

func getSubscriptionsCacheKey(topicName string) string {
	return CACHE_KEY_PREFIX_SUBSCRIPTIONS + topicName
}

//  GetAllSubscriptionsFromDb returns all subscriptions in normal status.
func GetAllSubscriptionsFromDb() ([]SubscriptionRecord, error) {
	sqlStr := fmt.Sprintf(`
	SELECT t2.class_key, t1.message_class_id,
		t1.subscription_id, t1.subscriber_id,
		t1.reception_channel, t1.status,
		t1.subscribe_time,t1.timeout
	FROM %s t1
	INNER JOIN %s t2
	ON(t1.message_class_id=t2.class_id)
	WHERE t1.status=?
	`,
		DB_TABLE_SUBSCRIPTIONS, DB_TABLE_MESSAGE_CLASSES)
	db, err := GetDb()
	if err != nil {
		return nil, err
	}
	defer db.Release()
	rows, err := db.Query(sqlStr, SUBSCRIPTION_NORMAL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var subscriptions []SubscriptionRecord
	for rows.Next() {
		sub := SubscriptionRecord{}
		err := rows.Scan(&sub.Class_key, &sub.Message_class_id,
			&sub.Subscription_id, &sub.Subscriber_id, &sub.Reception_channel,
			&sub.Status, &sub.Subscribe_time, &sub.Timeout)
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, sub)
	}
	return subscriptions, nil
}

func GetAllSubscriptionsFromCache() []SubscriptionRecord {
	subscriptions := cache.Get(getSubscriptionsCacheKey("*"))
	switch subscriptions.(type) {
	case nil:
		return nil
	default:
		return subscriptions.([]SubscriptionRecord)
	}
}

func GetAllSubscriptionsWithCache() ([]SubscriptionRecord, error) {
	subscriptions := GetAllSubscriptionsFromCache()
	if subscriptions != nil {
		return subscriptions, nil
	}

	subscriptions, err := GetAllSubscriptionsFromDb()
	if err == nil {
		cache.Set(getSubscriptionsCacheKey("*"), subscriptions)
	}
	return subscriptions, err
}

// GetSubscriptionsByTopicFromDb returns all subscriptions in normal status of the topicName.
func GetSubscriptionsByTopicFromDb(topicName string) ([]SubscriptionRecord, error) {
	sqlStr := fmt.Sprintf(`
	SELECT t2.class_key, t1.message_class_id,
		t1.subscription_id, t1.subscriber_id,
		t1.reception_channel, t1.status,
		t1.subscribe_time,t1.timeout
	FROM %s t1
	INNER JOIN %s t2
	ON(t1.message_class_id=t2.class_id AND t2.class_key=?)
	WHERE t1.status=?
	`,
		DB_TABLE_SUBSCRIPTIONS, DB_TABLE_MESSAGE_CLASSES)
	db, err := GetDb()
	if err != nil {
		return nil, err
	}
	defer db.Release()
	rows, err := db.Query(sqlStr, topicName, SUBSCRIPTION_NORMAL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var subscriptions []SubscriptionRecord
	for rows.Next() {
		sub := SubscriptionRecord{}
		err := rows.Scan(&sub.Class_key, &sub.Message_class_id,
			&sub.Subscription_id, &sub.Subscriber_id, &sub.Reception_channel,
			&sub.Status, &sub.Subscribe_time, &sub.Timeout)
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, sub)
	}
	return subscriptions, nil
}

func GetSubscriptionsByTopicFromCache(topicName string) []SubscriptionRecord {
	subscriptions := cache.Get(getSubscriptionsCacheKey(topicName))
	switch subscriptions.(type) {
	case nil:
		return nil
	default:
		return subscriptions.([]SubscriptionRecord)
	}
}

func GetSubscriptionsByTopicWithCache(topicName string) ([]SubscriptionRecord, error) {
	subscriptions := GetSubscriptionsByTopicFromCache(topicName)
	if subscriptions != nil {
		return subscriptions, nil
	}

	subscriptions, err := GetSubscriptionsByTopicFromDb(topicName)
	if err == nil {
		cache.Set(getSubscriptionsCacheKey(topicName), subscriptions)
	}
	return subscriptions, err
}

// GetSubscriptionById try to get a subscription according its id.
//	An empty subscription record returned if there's no such subscription.
//	If an error occurred the returned error should not be nil.
func GetSubscriptionById(subscriptionId int32) (sub SubscriptionRecord, err error) {
	var (
		db  *DB
		row *sql.Row
	)
	db, err = GetDb()
	if err != nil {
		return
	}
	defer db.Release()
	sqlStr := fmt.Sprintf(`
	SELECT t2.class_key, t1.message_class_id,
		t1.subscription_id, t1.subscriber_id,
		t1.reception_channel, t1.status,
		t1.subscribe_time,t1.timeout
	FROM %s t1
	INNER JOIN %s t2
	ON(t1.message_class_id=t2.class_id)
	WHERE t1.subscription_id=?`,
		DB_TABLE_SUBSCRIPTIONS, DB_TABLE_MESSAGE_CLASSES)
	row, err = db.QueryRow(sqlStr, subscriptionId)
	if err != nil {
		return
	}
	err = row.Scan(&sub.Class_key, &sub.Message_class_id,
		&sub.Subscription_id, &sub.Subscriber_id, &sub.Reception_channel,
		&sub.Status, &sub.Subscribe_time, &sub.Timeout)
	return
}
