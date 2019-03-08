package filelog

import (
	"medispatcher/config"
	"testing"
)

func Test_FileLog(t *testing.T) {
	config.GetConfigPointer().DropMessageLogDir = "/tmp"
	for i := 0; i < 10; i++ {
		err := Write(1, map[string]interface{}{"hello": 1, "world": "gogogo"})
		if err != nil {
			t.Error(err)
		}
	}
}
