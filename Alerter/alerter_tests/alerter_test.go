package alerter_test

import (
	"fmt"
	"medispatcher/Alerter"
	_ "medispatcher/Alerter/proxy/Email"
	_ "medispatcher/Alerter/proxy/Sms"
	"testing"
	"time"
)

func TestProxyNames(t *testing.T) {
	names := fmt.Sprintf("%v", Alerter.GetRegistredProxyNames())
	if names != fmt.Sprintf("%v", []string{"Email", "Sms"}) && names != fmt.Sprintf("%v", []string{"Sms", "Email"}) {
		t.Errorf("Expecting [\"Email\", \"Sms\"], but returned: %v", names)
	}
}

func TestEmailProxySend(t *testing.T) {
	cfg := Alerter.Config{
		Gateway:   "http://127.0.0.1",
		User:      "test",
		Password:  "testpwd",
		ProxyType: "Email",
		PostFieldsMap: map[string]string{
			"Recipient": "email_destinations",
			"Subject":   "email_subject",
			"Content":   "email_content",
		},
		TemplateRootPath: "/var/lib/medispatcher/alerter_templates/email/",
	}

	alerter, err := Alerter.New(cfg)
	if err != nil {
		t.Error(err)
		return
	}
	alerter.Alert(Alerter.Alert{
		Content:   "System test alerm message.",
		Recipient: "chaosue@yeah.net",
		Subject:   "TestAlert",
		TemplateName: "MessageSendingFailed.eml",
	})
	time.Sleep(time.Second * 2)
	err = alerter.LastErrorClean()
	if err != nil {
		t.Error(err)
	}
}
