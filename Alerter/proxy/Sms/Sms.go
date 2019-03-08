// Package Sms sends Alerts via http gateways.
package Sms

import (
	"errors"
	"fmt"
	"medispatcher/Alerter"
	transproxy "medispatcher/transproxy/http"
	"regexp"
	"strings"
	"time"

	l "github.com/sunreaver/logger"
)

type Sms struct {
	cfg Alerter.Config
}

func (proxy *Sms) Open() error {
	return nil
}

func (proxy *Sms) Close() error {
	return nil
}

func (proxy *Sms) Config(cfg Alerter.Config) error {
	if !proxy.IsValidGateWay(cfg.Gateway) {
		return errors.New("Invalid gateway string")
	}
	proxy.cfg = cfg
	return nil
}

func (proxy *Sms) GetConfig() *Alerter.Config {
	return &proxy.cfg
}

func (proxy *Sms) IsValidGateWay(gateway string) bool {
	valid, _ := regexp.Match(`(?i)^https?://`, []byte(gateway))
	return valid
}

func (proxy *Sms) IsValidPhoneNumber(phoneNum string) bool {
	valid, _ := regexp.Match(`^\+?\d{1,12}(-\d{1,6}){0,4}`, []byte(phoneNum))
	return valid
}

func (proxy *Sms) Send(alm Alerter.Alert) (err error) {
	defer func() {
		l.GetSugarLogger("alerter.log").Infow("Send sms",
			"proxy", proxy,
			"error", err)
	}()

	recipients := strings.Split(alm.Recipient, ",")
	sErr := []string{}
	for _, recipient := range recipients {
		alm.Recipient = recipient
		if !proxy.IsValidPhoneNumber(recipient) {
			sErr = append(sErr, "Invalid phone number: '"+recipient+"'")
			continue
		}
		httpCode, resp, err := transproxy.Transfer(proxy.cfg.Gateway, proxy.packRequestData(&alm), nil, time.Millisecond*DEFAULT_TRANSPORT_TIMEOUT)

		if err != nil {
			sErr = append(sErr, "Failed to send alert  by sms: "+err.Error())
		} else if httpCode != 200 {
			sErr = append(sErr, fmt.Sprintf("Failed to send alert  by sms: gateway error: %v ", httpCode))
		} else if proxy.cfg.AckStr != string(resp) {
			sErr = append(sErr, "Gateway response '"+string(resp)+"' is not as exepected '"+proxy.cfg.AckStr+"'.")
		}
	}
	if len(sErr) > 0 {
		return errors.New("Error ocurred: " + strings.Join(sErr, "|"))
	}
	return nil
}

func (proxy *Sms) packRequestData(alm *Alerter.Alert) map[string]string {
	data := map[string]string{}
	if user, exists := proxy.cfg.PostFieldsMap["User"]; exists {
		data[user] = proxy.cfg.User
	}

	if password, exists := proxy.cfg.PostFieldsMap["Password"]; exists {
		data[password] = proxy.cfg.Password
	}
	if from, exists := proxy.cfg.PostFieldsMap["From"]; exists {
		data[from] = proxy.cfg.From
	}
	if subject, exists := proxy.cfg.PostFieldsMap["Subject"]; exists {
		data[subject] = alm.Subject
	}
	if content, exists := proxy.cfg.PostFieldsMap["Content"]; exists {
		data[content] = alm.Content
	}
	if recipient, exists := proxy.cfg.PostFieldsMap["Recipient"]; exists {
		data[recipient] = alm.Recipient
	}
	return data
}

func (proxy *Sms) New() Alerter.AlerterProxy {
	return &Sms{}
}

func init() {
	Alerter.RegisterProxy("Sms", &Sms{})
}
