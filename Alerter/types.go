package Alerter

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
	"reflect"
)

type Logger interface  {
	Printf(format string, v ...interface{})
	Print(v ...interface{})
	Println(v ...interface{})

}
type Config struct {
	Gateway  string
	User     string
	Password string
	// From may be used by Email proxy
	From string
	// e.g. Email
	ProxyType string
	// Maps Config fields or params to post fields. e.g.
	//	{"User":"tasker", "Password": "key",
	//	"From": "replyTo", "Subject": "email_subject",
	//  "Recipient": "email_destinations", "Content": email_content"
	//	}
	PostFieldsMap map[string]string
	logger        Logger
	// The string indicates the Gateway successfully received the Alert.
	AckStr string
	// Subject,From, Content will be passed to the templates.
	TemplateRootPath string
}

func (c *Config)Set(name string, value interface{}) error{
	rf := reflect.ValueOf(c)
	field := rf.Elem().FieldByName(name)
	if !field.IsValid(){
		return errors.New(fmt.Sprintf("'%v' is not a valid field.", name))
	}
	if name != "logger" {
		if field.Type().Name() != reflect.TypeOf(value).Name() {
			return errors.New(fmt.Sprintf("Type of '%v' does not match %v.", name, field.Type().Name()))
		}
		field.Set(reflect.ValueOf(value))
	} else {
		lValue, ok := value.(Logger)
		if !ok {
			return errors.New(fmt.Sprintf("Type of '%v' cannot be assigned to logger", reflect.TypeOf(value).Name()))
		}
		c.logger = lValue
	}
	return nil
}

type AlerterLastError struct {
	msg  string
	lock *sync.Mutex
}

func (ale AlerterLastError) Error() string {
	return ale.msg
}

func (ale *AlerterLastError) setMessage(msg string) {
	ale.lock.Lock()
	defer ale.lock.Unlock()
	ale.msg = msg
}

func (ale *AlerterLastError) getMessage() string {
	ale.lock.Lock()
	defer ale.lock.Unlock()
	msg := ale.msg
	return msg
}

type Alert struct {
	Subject      string
	Content      string
	Recipient    string
	TemplateName string
	AlarmReceiveChan string
}

type AlerterProxy interface {
	// Open connection to the gateway.
	Open() error
	// Send Alert content.
	Send(alm Alert) error
	// Set configurations needed by the proxy.
	Config(cfg Config) error
	GetConfig() *Config
	Close() error
	New() AlerterProxy
}

const MAX_Alert_LIST_LENGTH = 1000

type InstancePool struct {
	instances map[string]*Alerter
	lock      *sync.Mutex
}

func (ipl *InstancePool) add(index string, alt *Alerter) {
	ipl.lock.Lock()
	defer ipl.lock.Unlock()
	(*ipl).instances[index] = alt
	return
}

func (ipl *InstancePool) exists(index string) bool {
	ipl.lock.Lock()
	defer ipl.lock.Unlock()
	_, exists := ipl.instances[index]
	return exists
}

func (ipl *InstancePool) get(index string) *Alerter {
	ipl.lock.Lock()
	defer ipl.lock.Unlock()
	return ipl.instances[index]
}

type Alerter struct {
	alertQueue        []Alert
	lock              *sync.Mutex
	newAlertSigalChan chan bool
	newAlertChan      chan Alert
	errorChan         chan error
	lastError         *AlerterLastError
	logger            Logger
	proxy             *AlerterProxy
}

var instancePool = &InstancePool{
	instances: map[string]*Alerter{},
	lock:      &sync.Mutex{},
}

var intanceCreateLock = &sync.Mutex{}

var registeredProxies = map[string]AlerterProxy{}

func RegisterProxy(name string, proxy AlerterProxy) {
	registeredProxies[name] = proxy
}

func GetRegistredProxyNames() []string {
	names := []string{}
	for name := range registeredProxies {
		names = append(names, name)
	}
	return names
}

func New(cfg Config) (*Alerter, error) {
	intanceCreateLock.Lock()
	defer intanceCreateLock.Unlock()
	indexB, err := json.Marshal(cfg)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to encode config: %v", err))
	}
	index := fmt.Sprintf("%x", md5.Sum(indexB))
	eAlerter := instancePool.get(index)
	if eAlerter != nil {
		return eAlerter, nil
	}
	if cfg.Gateway == "" {
		return nil, errors.New("Empty gateway.")
	}
	proxyIf, exists := registeredProxies[cfg.ProxyType]
	if !exists {
		return nil, errors.New(fmt.Sprintf("Alerter proxy[%v] not registered.", cfg.ProxyType))
	}
	proxy := proxyIf.New()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to config proxy: %v", err))
	}
	proxy.Config(cfg)
	eAlerter = &Alerter{
		alertQueue:        []Alert{},
		lock:              &sync.Mutex{},
		logger:            cfg.logger,
		proxy:             &proxy,
		lastError:         &AlerterLastError{lock: &sync.Mutex{}},
		newAlertSigalChan: make(chan bool, 300),
		newAlertChan:      make(chan Alert, 3),
		errorChan:         make(chan error, 3),
	}
	instancePool.add(index, eAlerter)
	go (*eAlerter).startSender()
	go (*eAlerter).collectError()
	return eAlerter, nil
}

func (alt *Alerter) Alert(alm Alert) {
	alt.newAlertChan <- alm
}

// Get the last error for alert sending and clean the last error messages.
// TODO: currently not distinguishes routines.
func (alt *Alerter) LastErrorClean() error {
	errMsg := alt.lastError.getMessage()
	if errMsg == "" {
		return nil
	}
	err := errors.New(errMsg)
	alt.lastError.setMessage("")
	return err
}

func (alt *Alerter) collectError() {
	var err error
	for {
		err = <-alt.errorChan
		alt.lastError.setMessage(err.Error())
	}
}

func (alt *Alerter) sendAlert(alm Alert) error {
	err := (*(alt.proxy)).Open()
	if err == nil {
		defer (*(alt.proxy)).Close()
		// Process alert templates.
		cfg := (*(alt.proxy)).GetConfig()
		if alm.TemplateName != "" && cfg.TemplateRootPath != "" {
			templateFile := cfg.TemplateRootPath + string(os.PathSeparator) + alm.TemplateName
			_, err = os.Stat(templateFile)
			if err == nil {
				var fd *os.File
				fd, err = os.Open(templateFile)
				if err == nil {
					content := []byte{}
					buff := make([]byte, 128)
					for {
						n, e := fd.Read(buff)
						if e != nil {
							if e == io.EOF {
								break
							} else {
								err = errors.New(fmt.Sprintf("Failed to read alert template: %v", e))
								break
							}
						} else {
							content = append(content, buff[:n]...)
						}
					}
					if err == nil {
						contentStr := string(content)
						contentStr = strings.Replace(contentStr, "{@Subject@}", alm.Subject, -1)
						contentStr = strings.Replace(contentStr, "{@From@}", cfg.From, -1)
						contentStr = strings.Replace(contentStr, "{@Content@}", alm.Content, -1)
						alm.Content = contentStr
					}
				}
			}

		}
		if err == nil {
			err = (*(alt.proxy)).Send(alm)
		}
	}
	if err != nil {
		if alt.logger != nil {
			alt.logger.Printf("Failed to send Alert to [%v]: %v", alm.Recipient, err)
		}
		alt.errorChan <- err
	}
	return err
}
func (alt *Alerter) startSender() {
	loopCheckInterval := time.After(time.Second * 5)
	for {
		select {
		case Alert := <-alt.newAlertChan:
			alt.lock.Lock()
			alt.alertQueue = append(alt.alertQueue, Alert)
			alt.lock.Unlock()
			alt.newAlertSigalChan <- true
		// has new Alert
		case <-alt.newAlertSigalChan:
			alt.lock.Lock()
			queueLength := len(alt.alertQueue)
			if queueLength > 0 {
				Alert := alt.alertQueue[0]
				if queueLength == 1 {
					alt.alertQueue = alt.alertQueue[0:0]
				} else {
					alt.alertQueue = alt.alertQueue[1:]
				}
				alt.lock.Unlock()
				go alt.sendAlert(Alert)
			} else {
				alt.lock.Unlock()
			}
		case <-loopCheckInterval:
			if len(alt.alertQueue) > MAX_Alert_LIST_LENGTH {
				alt.lock.Lock()
				alt.alertQueue = alt.alertQueue[:MAX_Alert_LIST_LENGTH-1]
				alt.lock.Unlock()
			}
			loopCheckInterval = time.After(time.Second * 5)
		}
	}
}
