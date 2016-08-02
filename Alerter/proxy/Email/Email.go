// Package Email sends Alerts to email servers via http gateways.
package Email

import (
	"errors"
	"medispatcher/Alerter"
	transproxy "medispatcher/transproxy/http"
	"regexp"
	"strings"
	"time"
	"fmt"
)

type Email struct {
	cfg Alerter.Config
}

func (proxy *Email) Open() error {
	return nil
}

func (proxy *Email) Close() error {
	return nil
}

func (proxy *Email) Config(cfg Alerter.Config) error {
	if !proxy.IsValidGateWay(cfg.Gateway){
		return errors.New("Invalid gateway string")
	}
	proxy.cfg = cfg
	return nil
}

func (proxy *Email) GetConfig()*Alerter.Config{
	return  &proxy.cfg
}

func (proxy *Email) IsValidGateWay(gateway string) bool {
	valid, _ := regexp.Match(`(?i)^https?://`, []byte(gateway))
	return valid
}

func (proxy *Email) IsValidEmail(email string) bool {
	valid, _ := regexp.Match(`^[\da-zA-Z\._]+@[\da-zA-Z_-]+(\.[a-zA-Z]{2,3}){1,4}$`, []byte(email))
	return valid
}

func (proxy *Email) Send(alm Alerter.Alert) error {
	recipients := strings.Split(alm.Recipient, ",")
	sErr := []string{}
	for _, recipient := range recipients {
		if !proxy.IsValidEmail(recipient) {
			sErr = append(sErr, "Invalid email recipient: '"+recipient + "'")
			continue
		}
		alm.Recipient = recipient
		httpCode, resp, err := transproxy.Transfer(proxy.cfg.Gateway, proxy.packRequestData(&alm), time.Millisecond*DEFAULT_TRANSPORT_TIMEOUT)

		if err != nil {
			sErr = append(sErr, "Failed to send alert  by email: "+err.Error())
		} else if httpCode != 200 {
			sErr = append(sErr, fmt.Sprintf("Failed to send alert  by email: gateway error: %v ", httpCode))
		} else if proxy.cfg.AckStr != string(resp) {
			sErr = append(sErr, "Gateway response '"+string(resp)+"' is not as exepected '"+proxy.cfg.AckStr+"'.")
		}
	}
	if len(sErr) > 0 {
		return errors.New("Error ocurred: " + strings.Join(sErr, "|"))
	}
	return nil
}

func (proxy *Email) packRequestData(alm *Alerter.Alert) map[string]string {
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

func (proxy *Email) New() Alerter.AlerterProxy {
	return &Email{}
}

func init() {
	Alerter.RegisterProxy("Email", &Email{})
}