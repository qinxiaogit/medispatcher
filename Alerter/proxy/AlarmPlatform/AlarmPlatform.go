package AlarmPlatform

import (
    "errors"
    "medispatcher/Alerter"
    transproxy "medispatcher/transproxy/http"
    "regexp"
    // "strings"
    "time"
    "fmt"
    "strings"
)

type AlarmPlatform struct {
    cfg Alerter.Config
}

func (proxy *AlarmPlatform) Open() error {
    return nil
}

func (proxy *AlarmPlatform) Close() error {
    return nil
}

func (proxy *AlarmPlatform) Config(cfg Alerter.Config) error {
    if !proxy.IsValidGateWay(cfg.Gateway){
        return errors.New("Invalid gateway string")
    }
    proxy.cfg = cfg
    return nil
}

func (proxy *AlarmPlatform) GetConfig()*Alerter.Config{
    return  &proxy.cfg
}

func (proxy *AlarmPlatform) IsValidGateWay(gateway string) bool {
    valid, _ := regexp.Match(`(?i)^https?://`, []byte(gateway))
    return valid
}

func (proxy *AlarmPlatform) IsValidPhoneNumber(phoneNum string) bool {
    valid, _ := regexp.Match(`^\+?\d{1,12}(-\d{1,6}){0,4}`, []byte(phoneNum))
    return valid
}

func (proxy *AlarmPlatform) Send(alm Alerter.Alert) error {
    var sErr []string = []string{}
    receivers := strings.Split(alm.Recipient, ",")

    for _, receiver := range receivers {
        receiver = strings.TrimSpace(receiver)
        if receiver == "" {
            continue
        }

        alm.Recipient = receiver

        httpCode, resp, err := transproxy.TransferJSON(proxy.cfg.Gateway, proxy.packRequestData(&alm), time.Millisecond*DEFAULT_TRANSPORT_TIMEOUT)
        fmt.Println(httpCode, string(resp))

        if err != nil {
            sErr = append(sErr, "Failed to send alert  by email: "+err.Error())
        } else if httpCode != 200 {
            sErr = append(sErr, fmt.Sprintf("Failed to send alert  by email: gateway error: %v ", httpCode))
        } else if proxy.cfg.AckStr != string(resp) {
            sErr = append(sErr, "Gateway response '"+string(resp)+"' is not as exepected '"+proxy.cfg.AckStr+"'.")
        }
    }

    if len(sErr) > 0 {
        return errors.New("Error ocurred: " + strings.Join(sErr, ";"))
    }

    return nil
}

func (proxy *AlarmPlatform) packRequestData(alm *Alerter.Alert) map[string]string {
    data := map[string]string{}

    if user, exists := proxy.cfg.PostFieldsMap["User"]; exists {
        data[user] = proxy.cfg.User
    }

    if password, exists := proxy.cfg.PostFieldsMap["Password"]; exists {
        data[password] = proxy.cfg.Password
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

    data["project"] = "EventServer"
    if project, exists := proxy.cfg.PostFieldsMap["project"]; exists {
        data["project"] = project
    }

    // 1邮件 2短信 3微信.
    if alm.AlarmReceiveChan != "" {
        data["type"] = alm.AlarmReceiveChan
    } else {
        data["type"] = "00000111"
        if receiver_channel, exists := proxy.cfg.PostFieldsMap["type"]; exists {
            data["type"] = receiver_channel
        }
    }

    return data
}

func (proxy *AlarmPlatform) New() Alerter.AlerterProxy {
    return &AlarmPlatform{}
}

func init() {
    Alerter.RegisterProxy("AlarmPlatform", &AlarmPlatform{})
}