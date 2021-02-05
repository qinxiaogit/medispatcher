// Package config manages the configurations needed by medispatcher.
package config

import (
	"errors"
	"fmt"
	util "medispatcher/osutil"
	"medispatcher/strutil"
	"os"
	"path"
	"reflect"
	"time"
)

var BuildTime = "unknonw"
var GitHash = ""

const (
	VerNo           = "2.5.0"
	MAX_CONNECTIONS = 15000
	// 客户端响应超时
	CLIENT_TIMEOUT  = time.Second * 5
	CLIENT_MAX_IDLE = time.Second * 120
	// 请求处理超时
	PROCESS_TIMEOUT = int64(2)
)

var debug bool

var config = &Config{
	QueueServerType:                      "beanstalk",
	ListenAddr:                           "0.0.0.0:5601",
	DebugAddr:                            ":9898",
	StatisticApiAddr:                     "0.0.0.0:5606",
	PrometheusApiAddr:                    "0.0.0.0:25606",
	NameOfMainQueue:                      "main-incoming-queue",
	PrefixOfChannelQueue:                 "sub-queue/",
	MedisPerMaxConsumerQueueNum:          8,
	QueueServerAddr:                      "127.0.0.1:11300",
	QueueServerPoolListenConnCount:       2,
	QueueServerPoolCmdConnCount:          1,
	ListenersOfMainQueue:                 uint16(1),
	SendersPerChannel:                    uint32(1),
	MaxSendersPerChannel:                 uint32(10),
	IntervalOfSendingForSendRoutine:      uint32(0),
	SendersPerRetryChannel:               uint32(1),
	MaxSendersPerRetryChannel:            uint32(10),
	CoeOfIntervalForRetrySendingMsg:      uint16(10),
	EnableMsgSentLog:                     true,
	SplitLog:                             true,
	MaxRetryTimesOfSendingMessage:        uint16(10),
	MaxMessageProcessTime:                uint32(30000),
	DefaultMaxMessageProcessTime:         uint32(5000),
	MsgQueueFaultToleranceListNamePrefix: "mec_list_of_msg_for_restore_to_queue_server:",
	DATA_DIR:                             "/var/lib/medispatcher/",
	RunAtBench:                           false,
	DropMessageLogDir:                    "/var/lib/medispatcher-drop/",
}

//	TODO: not coroutine safe
func DebugEnabled() bool {
	return debug
}

//	TODO: not coroutine safe
func SetDebug(enable bool) {
	debug = enable
}

//	TODO: not coroutine safe
func SetConfig(name string, val interface{}) {
	reflect.ValueOf(config).Elem().FieldByName(name).Set(reflect.ValueOf(val))
}

func Setup() error {

	var err error
	// 获取并解析配置文件
	config, err = ParseConfig()
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to initialize configs: %s ", err))
	}

	// 加载缺省报警设置.
	result, err := GetConfigFromDisk("default_alarm")
	if err == nil {
		if v, ok := result.(map[string]interface{}); ok {
			if _, exists := v["DefaultAlarmReceiver"]; exists {
				config.DefaultAlarmReceiver = v["DefaultAlarmReceiver"].(string)
			}

			if _, exists := v["DefaultAlarmChan"]; exists {
				config.DefaultAlarmChan = v["DefaultAlarmChan"].(string)
			}

			if _, exists := v["GlobalMessageBlockedAlertThreshold"]; exists {
				config.GlobalMessageBlockedAlertThreshold = strutil.ToInt(v["GlobalMessageBlockedAlertThreshold"])
			}

			if _, exists := v["GlobalMessageBlockedAlarmInterval"]; exists {
				config.GlobalMessageBlockedAlarmInterval = int64(strutil.ToInt(v["GlobalMessageBlockedAlarmInterval"]))
			}
		}
	}

	var daemonUid, daemonGid int
	if config.DAEMON_USER == "" {
		daemonGid = os.Getegid()
		daemonUid = os.Getuid()
	} else {
		daemonUid, daemonGid, err = util.LookupOsUidGid(config.DAEMON_USER)
	}

	if err != nil {
		return err
	}

	config.DAEMON_UID = daemonUid
	config.DAEMON_GID = daemonGid

	config.InstallDir = path.Dir(os.Args[0]) + string(os.PathSeparator)

	if !util.FileExists(config.LOG_DIR) {
		err = os.MkdirAll(config.LOG_DIR, 0755)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to initialize log dir[%s]: %s", config.LOG_DIR, err))
		}
	}
	// 创建丢弃数据日志目录
	if !util.FileExists(config.DropMessageLogDir) {
		err = os.MkdirAll(config.DropMessageLogDir, 0755)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to initialize drop message dir[%s]: %s", config.DropMessageLogDir, err))
		}
	}

	err = util.Rchown(config.LOG_DIR, daemonUid, daemonGid)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to change log dir owner: %s", err))
	}

	if !util.FileExists(config.DATA_DIR) {
		err = os.MkdirAll(config.DATA_DIR, 0755)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to initialize data dir[%s]: %s", config.DATA_DIR, err))
		}
	}

	err = util.Rchown(config.DATA_DIR, daemonUid, daemonGid)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to change data dir owner: %s", err))
	}
	return nil
}
