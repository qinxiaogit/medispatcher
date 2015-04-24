package http

import (
	"fmt"
	"testing"
	"time"
)

func TestTransfer(t *testing.T) {
	code, respData, err := Transfer("http://127.0.0.1/temp/post.php",
		map[string]string{"content": `{"id":12323, "name": "chaos"}`, "time": "123123"},
		time.Second*2,
	)
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	fmt.Printf("httpcode: %v\nresponse data: %s\n", code, respData)
}
