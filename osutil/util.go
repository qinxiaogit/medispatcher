package osutil

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var lastLocalIpUpdateTime = time.Now()
var localIp string = ""
var localIpUpdateLock = new(sync.RWMutex)

func init() {
	go func() {
		tm := time.NewTicker(time.Second * 10)
		for {
			<-tm.C
			localIpUpdateLock.Lock()
			localIp = RGetLocalIp()
			localIpUpdateLock.Unlock()
		}
	}()
}

func FileExists(name string) bool {
	_, err := os.Stat(name)
	return err == nil || os.IsExist(err)
}

// RChmod 递归地修改指定目录及其文件的mode
func Rchmod(dir string, mode os.FileMode) error {
	err := filepath.Walk(dir, filepath.WalkFunc(func(path string, info os.FileInfo, err error) error {
		return os.Chmod(path, mode)
	}))
	if err == nil {
		err = os.Chmod(dir, mode)
	}
	return err
}

// RChown 递归地修改指定目录及其文件的所有者
func Rchown(dir string, uid int, gid int) error {
	err := filepath.Walk(dir, filepath.WalkFunc(func(path string, info os.FileInfo, err error) error {
		return os.Chown(path, uid, gid)
	}))
	if err == nil {
		err = os.Chown(dir, uid, gid)
	}
	return err
}

func LookupOsUidGid(name string) (uid int, gid int, err error) {
	cmd := exec.Command("id", name)
	o := bytes.Buffer{}
	cmd.Stderr = &o
	cmd.Stdout = &o
	err = cmd.Run()
	if err != nil {
		err = errors.New(fmt.Sprintf("Failed to get uid and gid for [%s]: %s:%s", name, err, o.String()))
	} else {
		outstring := strings.Fields(o.String())[:2]
		uid, err = strconv.Atoi(string([]byte(outstring[1])[4:strings.Index(outstring[0], "(")]))
		if err != nil {
			err = errors.New(fmt.Sprintf("Failed to get uid for[%s]: %s", name, err))
		}
		gid, err = strconv.Atoi(string([]byte(outstring[1])[4:strings.Index(outstring[1], "(")]))
		if err != nil {
			err = errors.New(fmt.Sprintf("Failed to get gid for[%s]: %s", name, err))
		}
	}
	return
}

func GetLocalIp() string {
	localIpUpdateLock.RLock()
	defer localIpUpdateLock.RUnlock()
	return localIp
}

func RGetLocalIp() (ip string) {
	hostName, err := os.Hostname()
	if err != nil {
		add, err := net.LookupHost(hostName)
		if err != nil && len(add) > 0 {
			return add[0]
		}
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {

		return
	}
	for _, a := range addrs {
		pts := strings.Split(a.String(), "/")
		if len(pts) < 2 {
			continue
		}
		_ip := pts[0]
		if strings.Index(ip, ":") >= 0 {
			continue
		}
		if strings.Index(_ip, "127.0.") == 0 {
			continue
		}
		ip = _ip
		break
	}
	return
}
