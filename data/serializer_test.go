package data

import (
	"testing"
	"time"
)

func TestSerializer(t *testing.T) {
	msg := MessageStuct{
		MsgKey:      "test",
		Body:        "{\"test_data\":[2,3]}",
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
}
