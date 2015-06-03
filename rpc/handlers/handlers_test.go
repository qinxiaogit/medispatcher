package handlers

import (
	"errors"
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
			"Params":         map[string]interface{}{"Concurrency": 11,  "ConcurrencyOfRetry": 1, "IntervalOfSending": 92, "ProcessTimeout": 100},
		})
		t.Log(re)
	}
	if err != nil {
		t.Error(err)
		t.Fail()
	}
}

func TestGetSubscriptionParams(t *testing.T) {
	var err error
	c, err := rpclient.New(config.GetConfig().ListenAddr)
	if err == nil {
		defer c.Close()
		var re interface{}
		re, err = c.Call("GetSubscriptionParams", map[string]interface{}{
			"SubscriptionId": 352,
		})
		t.Log(re)
	}
	if err != nil {
		t.Error(err)
		t.Fail()
	}
}

func TestGetDefaultSubscriptionSettings(t *testing.T){
	var err error
	c, err := rpclient.New(config.GetConfig().ListenAddr)
	if err == nil {
		defer c.Close()
		var re interface{}
		re, err = c.Call("GetDefaultSubscriptionSettings", map[string]interface{}{})
		t.Log(re)
	}
	if err != nil {
		t.Error(err)
		t.Fail()
	}
}