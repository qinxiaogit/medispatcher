package data

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestGetSubscriptionsByTopicWithCache(t *testing.T) {
	topicName := "test"
	subscriptions, err := GetSubscriptionsByTopicWithCache(topicName)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	t.Log("Belowing are fetched from db:")
	t.Log(formatOutput(subscriptions))
	t.Log("Now they should be fetched from cache:")
	subscriptions = GetSubscriptionsByTopicFromCache(topicName)
	if subscriptions == nil {
		t.Error(errors.New("Failed to fetch subscriptions from cache"))
		t.Fail()
		return
	}
	formatOutput(subscriptions)
}

func formatOutput(subscriptions []SubscriptionRecord) string {
	str := "\n"
	for i, sub := range subscriptions {
		r := fmt.Sprintf("%v", sub)
		sep := strings.Repeat("=", len(r)+4)
		str += fmt.Sprintf("%v\n%v: %v\n%v\n", sep, i, r, sep)
	}
	return str
}
