package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)

// GetChannelName returns the queue name for subscription.
func GetChannelName(topicName string, subscriptionId int32) string {
	return topicName + "/" + GetConfig().PrefixOfChannelQueue + strconv.Itoa(int(subscriptionId))
}

func GetChannelNameForReSend(topicName string, subscriptionId int32) string {
	return topicName + "/FAIL/" + GetConfig().PrefixOfChannelQueue + strconv.Itoa(int(subscriptionId))
}

// SaveConfig saves config to DATA_DIR as json file.
func SaveConfig(name string, config interface{}) error {
	data, err := json.Marshal(config)
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to serialize config data: %v", err))
		return err
	}
	err = ioutil.WriteFile(GetConfig().DATA_DIR+string(os.PathSeparator)+name+".json", data, os.FileMode(0666))

	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to write config data file: %v", err))
	}
	return err
}

func GetConfigFromDisk(name string) (config interface{}, err error) {
	var data []byte
	data, err = ioutil.ReadFile(GetConfig().DATA_DIR + string(os.PathSeparator) + name + ".json")
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to load config data file: %v", err))
		return
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to decode config: %v", err))
	}
	return
}
