package handlers

import (
	"errors"
	"medispatcher/pushstatistics"
	"medispatcher/rpc"
)

// GetPushStatistics 获取推送统计数据
type GetPushStatistics struct {
}

func init() {
	rpc.RegisterHandlerRegister("GetPushStatistics", GetPushStatistics{})
}

// Process 获取当前推送的统计数据.
// args {}. 不需要.
// RETURN: 三级map结构, topics ==> channels ==> data
//{
//	topic: {
//			channel : {
//				"s": int,
//				"m": int,
//				"h": int,
//				"d": int,
//		}
//	}
//}.
func (g GetPushStatistics) Process(args map[string]interface{}) (re interface{}, err error) {
	data := pushstatistics.ShowData()
	if data == nil {
		return nil, errors.New("no pushstatistics data")
	}
	return data, nil
}
