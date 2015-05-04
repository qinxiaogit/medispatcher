package data

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
	"time"
)

func TestSerializer(t *testing.T) {
	msg := MessageStuct{
		MsgKey: "test",
		Body: map[interface{}]interface{}{
			"a": 1,
			"b": "fsdfsf",
			123: "fff",
			"e": map[interface{}]interface{}{"a1": 12323},
			"ll": []interface{}{
				map[interface{}]interface{}{
					"a":  123,
					"bb": "123213",
				},
			},
		},
		Sender:      "test",
		Time:        float64(time.Now().UnixNano()) * 1E-9,
		OriginJobId: uint64(9898989),
	}

	var dMsg MessageStuct
	b, err := SerializeMessage(msg)
	if err == nil {
		dMsg, err = UnserializeMessage(b)
	}

	if err != nil {
		t.Error(err)
		t.Fail()
	} else {
		t.Log(dMsg)
	}
	mB, err := json.Marshal(dMsg.Body)
	if err != nil {
		t.Error(err)
		t.Fail()
	} else {
		var mBi interface{}
		err = json.Unmarshal(mB, &mBi)
		if err != nil {
			t.Log(err)
			t.Fail()
		} else {
			t.Log(fmt.Sprintf("%s", mB))
			t.Log(fmt.Sprintf("%+v", mBi))
		}
	}
}

func TestMsgackUnserialization(t *testing.T) {
	s, err := ioutil.ReadFile("serializer_test.data")
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	msg, err := UnserializeMessage([]byte(s))
	if err != nil {
		t.Error(err)
		t.Fail()
	} else {
		t.Logf("Unserialized as: %+v", msg)
		jB, err := json.Marshal(msg)
		if err != nil {
			t.Error(err)
			t.Fail()
		} else {
			t.Logf("json string: %s", jB)
		}
	}
}
