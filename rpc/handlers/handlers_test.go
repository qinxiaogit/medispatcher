package handlers

import (
	"errors"
	//	"fmt"
	"medispatcher/config"
	"medispatcher/rpclient"
	"testing"
)

func init() {
	config.Setup()
}

func TestRpcSend(t *testing.T) {
	var err error
	c, err := rpclient.New(config.GetConfig().ListenAddr)
	if err == nil {
		defer c.Close()
		var re interface{}
		re, err = c.Call("ClearDataCache", map[string]interface{}{})
		t.Log(re)
		if re != true {
			err = errors.New("Not cleared")
		}
	}
	if err != nil {
		t.Error(err)
		t.Fail()
	}
}

func TestSetSubscriptionParams(t *testing.T) {
	var err error
	c, err := rpclient.New(config.GetConfig().ListenAddr)
	if err == nil {
		defer c.Close()
		var re interface{}
		re, err = c.Call("SetSubscriptionParams", map[string]interface{}{
			"SubscriptionId": 352,
			"Params":         map[string]interface{}{"Concurrency": 1,  "ConcurrencyOfRetry": 1, "IntervalOfSending": 12, "ProcessTimeout": 800},
		})
		t.Log(re)
	}
	if err != nil {
		t.Error(err)
		t.Fail()
	}
}
