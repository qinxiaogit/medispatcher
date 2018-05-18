package handlers

import (
    "medispatcher/rpc"
    "medispatcher/config"
)

type SetDefaultAlarm struct {
}

func init() {
    rpc.RegisterHandlerRegister("SetDefaultAlarm", SetDefaultAlarm{})
}

func (_ SetDefaultAlarm) Process(args map[string]interface{}) (interface{}, error) {
    var exists bool
    var data map[string]string = make(map[string]string)
    if _, exists = args["default_alarm_receiver"]; exists {
        config.GetConfigPointer().DefaultAlarmReceiver = args["default_alarm_receiver"].(string)
        data["DefaultAlarmReceiver"] = args["default_alarm_receiver"].(string);
    }

    if _, exists = args["default_alarm_chan"]; exists {
        config.GetConfigPointer().DefaultAlarmChan = args["default_alarm_chan"].(string)
        data["DefaultAlarmChan"] = args["default_alarm_chan"].(string);
    }

    config.SaveConfig("default_alarm", data)

    return nil, nil
}
