// Package config manages the configurations needed by medispatcher.
package config

import (
	"errors"
	"fmt"
	util "medispatcher/osutil"
	"os"
	"path"
	"reflect"
	"time"
)

const (
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
	StatisticApiAddr:                     "0.0.0.0:5606",
	NameOfMainQueue:                      "main-incoming-queue",
	PrefixOfChannelQueue:                 "sub-queue/",
	QueueServerAddr:                      "127.0.0.1:11300",
	ListenersOfMainQueue:                 uint16(1),
	SendersPerChannel:                    uint16(1),
	MaxSendersPerChannel:                 uint16(10),
	IntervalOfSendingForSendRoutine:      uint16(1),
	SendersPerRetryChannel:               uint16(1),
	MaxSendersPerRetryChannel:            uint16(10),
	CoeOfIntervalForRetrySendingMsg:      uint16(10),
	EnableMsgSentLog:                     true,
	MaxRetryTimesOfSendingMessage:        uint16(10),
	MaxMessageProcessTime:                uint16(30000),
	DefaultMaxMessageProcessTime:         uint16(5000),
	MsgQueueFaultToleranceListNamePrefix: "mec_list_of_msg_for_restore_to_queue_server:",
	DATA_DIR: "/var/lib/medispatcher/",
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
