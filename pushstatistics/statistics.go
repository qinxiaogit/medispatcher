package pushstatistics

import (
	"container/list"
	"strings"
	"time"
)

const (
	allSuffix     = "_"
	failSuffix    = "_fail"
	successSuffix = "_success"
)

var (
	allData     *MQList // 全量数据
	failData    *MQList // 失败数据
	successData *MQList // 失败数据
	statistics  *Statistics
)

// PrometheusStatisticsStart PrometheusStatisticsStart
func PrometheusStatisticsStart(addr string) {
	allData = NewMQList()
	failData = NewMQList()
	successData = NewMQList()
	statistics = &Statistics{
		Len:  86400, // 1day
		Data: map[string]map[string]*Categorys{},
	}
	go run()
	go httpRun(addr)
}

// Add will 添加统计
func Add(mq MessageQueue, count int, success bool) {
	if mq == nil {
		return
	}
	for i := 0; i < count; i++ {
		allData.push(mq)
		if !success {
			failData.push(mq)
		} else {
			successData.push(mq)
		}
	}
}

// ShowData will 返回当前统计数据
func ShowData(sufixs ...string) map[string]map[string]*Categorys {
	out := statistics.ShowCopy()
	for k0, v := range out {
		for k1 := range v {
			needDel := true
			for _, sufix := range sufixs {
				if strings.HasSuffix(k1, sufix) {
					needDel = false
					break
				}
			}
			if needDel {
				v[k1].Recover()
				delete(out[k0], k1)
			}
		}
	}
	return out
}

func run() {
	t := time.NewTicker(time.Second)
	for {
		select {
		case <-t.C:
			last := statistics.Tick()
			data := allData.pop()
			fail := failData.pop()
			success := successData.pop()
			if data.Len() > 0 {
				// data > 0, fail must be > 0
				go func(at uint, all, failSrc, successSrc *list.List) {
					fn := func(l *list.List, sufix string) {
						if l == nil {
							return
						}
						for item := l.Front(); item != nil; item = item.Next() {
							v := item.Value.(MessageQueue)
							ctg := statistics.channel(v.GetTopic(), v.GetChannel()+sufix)
							ctg.AddAt(at, 1)
						}
					}
					fn(all, allSuffix)
					fn(failSrc, failSuffix)
					fn(successSrc, successSuffix)
				}(last, data, fail, success)
			}
		}
	}
}
