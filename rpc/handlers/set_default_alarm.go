package handlers

import (
	"github.com/qinxiaogit/medispatcher/config"
	"github.com/qinxiaogit/medispatcher/rpc"
	"github.com/qinxiaogit/medispatcher/strutil"
)

type SetDefaultAlarm struct {
}

func init() {
	rpc.RegisterHandlerRegister("SetDefaultAlarm", SetDefaultAlarm{})
}

func (_ SetDefaultAlarm) Process(args map[string]interface{}) (interface{}, error) {
	var exists bool
	var data map[string]interface{} = make(map[string]interface{})
	if _, exists = args["default_alarm_receiver"]; exists {
		config.GetConfigPointer().DefaultAlarmReceiver = args["default_alarm_receiver"].(string)
		data["DefaultAlarmReceiver"] = args["default_alarm_receiver"].(string)
	}

	if _, exists = args["default_alarm_chan"]; exists {
		config.GetConfigPointer().DefaultAlarmChan = args["default_alarm_chan"].(string)
		data["DefaultAlarmChan"] = args["default_alarm_chan"].(string)
	}

	if _, exists = args["global_message_blocked_alert_threshold"]; exists {
		config.GetConfigPointer().GlobalMessageBlockedAlertThreshold = strutil.ToInt(args["global_message_blocked_alert_threshold"])
		data["GlobalMessageBlockedAlertThreshold"] = strutil.ToInt(args["global_message_blocked_alert_threshold"])
	}

	if _, exists = args["global_message_blocked_alarm_interval"]; exists {
		config.GetConfigPointer().GlobalMessageBlockedAlarmInterval = int64(strutil.ToInt(args["global_message_blocked_alarm_interval"]))
		data["GlobalMessageBlockedAlarmInterval"] = int64(strutil.ToInt(args["global_message_blocked_alarm_interval"]))
	}

	if _, exists = args["name_of_main_queue_blocked_alert_threshold"]; exists {
		config.GetConfigPointer().NameOfMainQueueBlockedAlertThreshold = strutil.ToInt(args["name_of_main_queue_blocked_alert_threshold"])
		data["NameOfMainQueueBlockedAlertThreshold"] = strutil.ToInt(args["name_of_main_queue_blocked_alert_threshold"])
	}

	config.SaveConfig("default_alarm", data)

	return nil, nil
}
