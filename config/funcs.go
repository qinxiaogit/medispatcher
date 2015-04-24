package config

import (
	"strconv"
)

// GetChannelName returns the queue name for subscription.
func GetChannelName(topicName string, subscriptionId int32) string {
	return topicName + "/" + GetConfig().PrefixOfChannelQueue +  strconv.Itoa(int(subscriptionId))
}

func GetChannelNameForReSend(topicName string, subscriptionId int32) string {
	return topicName + "/FAIL/" +GetConfig().PrefixOfChannelQueue +  strconv.Itoa(int(subscriptionId))
}
