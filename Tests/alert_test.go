package Tests

import (
	"medispatcher/Alerter"
	_ "medispatcher/Alerter/proxy/Sms"
	_ "medispatcher/Alerter/proxy/Email"
	"testing"
	"medispatcher/config"
	"time"
//	"fmt"
)

func init(){
	config.Setup()
}

func TestAlertByEmail(t *testing.T) {
 	emailAlerter, err := Alerter.New(config.GetConfig().AlerterEmail)
	if err != nil {
		t.Error(err)
		return
	}
	emailAlerter.Alert(Alerter.Alert{
		Subject: "Test alert mail",
		Content: "abcd",
		Recipient: "chaos@jumei.com",
	})
	time.Sleep(time.Second * 2)
	err = emailAlerter.LastErrorClean()
	if err != nil {
		t.Error(err)
		return
	}
}

func TestAlertBySms(t *testing.T) {
	smsAlerter, err := Alerter.New(config.GetConfig().AlerterSms)
	if err != nil {
		t.Error(err)
		return
	}
	smsAlerter.Alert(Alerter.Alert{
		Subject: "Test alert Sms",
		Content: "abcd",
		Recipient: "15869909200",
	})
	time.Sleep(time.Second * 2)
	err = smsAlerter.LastErrorClean()
	if err != nil {
		t.Error(err)
		return
	}
}