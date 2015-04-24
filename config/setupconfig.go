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

//	TODO: not coroutine savfe
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

	config.InstallDir = path.Dir(os.Args[0]) + string(os.PathListSeparator)

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
	return nil
}
