package main

import (
	"medispatcher/Alerter"
	_ "medispatcher/Alerter/proxy/Sms"
	_ "medispatcher/Alerter/proxy/Email"
	"medispatcher/config"
	"time"
	"log"
	"medispatcher/logger"
//	"fmt"
)

func init(){
	err := config.Setup()
	if err != nil {
		log.Print(err)
	}
}

func main() {
	cfg := config.GetConfig().AlerterEmail
	err := cfg.Set("logger", logger.GetLogger("INFO"))
 	emailAlerter, err := Alerter.New(cfg)
	log.SetFlags(log.LstdFlags|log.Lshortfile)
	if err != nil {
		log.Fatal(err)
		return
	}
	emailAlerter.Alert(Alerter.Alert{
		Subject: "Message Center Alert(TEST)",
		Content: "System error, please fix ASASP!",
		Recipient: "suchaoabc@163.com",
		TemplateName: "MessageSendingFailed.eml",
	})
	time.Sleep(time.Second * 2)
	err = emailAlerter.LastErrorClean()
	if err != nil {
		log.Fatal(err)
		return
	}

	smsCfg := config.GetConfig().AlerterSms
	smsCfg.Set("logger", logger.GetLogger("INFO"))
	smsAlerter, err := Alerter.New(smsCfg)
	if err != nil {
		log.Fatal(err)
		return
	}
	smsAlerter.Alert(Alerter.Alert{
		Subject: "Message Center Alert(TEST)",
		Content: "System error, please fix ASASP!",
		Recipient: "15869909200",
		TemplateName: "MessageSendingFailed.sms",
	})
	time.Sleep(time.Second * 2)
	err = smsAlerter.LastErrorClean()
	if err != nil {
		log.Fatal(err)
		return
	}
}
