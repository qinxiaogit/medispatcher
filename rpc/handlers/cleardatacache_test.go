package handlers

import (
	"errors"
	"medispatcher/config"
	"medispatcher/rpclient"
	"testing"
	"fmt"
)

func init() {
	config.Setup()
}

func TestCall(t *testing.T) {
	var err error
	c, err := rpclient.New(config.GetConfig().ListenAddr)
	if err == nil {
		var re interface{}
		re, err = c.Call("ClearDataCache", map[string]interface{}{})
		fmt.Println(re)
		if re != true {
			err = errors.New("Not cleared")
		}
	}
	if err != nil {
		t.Error(err)
		t.Fail()
	}
}
