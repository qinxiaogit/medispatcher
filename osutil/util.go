package osutil

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

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
