package sender

import (
    "testing"
    "medispatcher/Alerter"
    "medispatcher/config"
)

func init() {
    config.Setup()
}

func TestAlarmPlatform(t *testing.T) {
    em := newErrorMonitor()

    alert := Alerter.Alert{
        Subject: "消息中心警报",
        Content: "test content",
        Recipient: "xianwangs",
    }

    em.alarmPlatform.Alert(alert)
}