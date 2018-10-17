package pushstatistics

import (
	"container/list"
	"time"
)

const (
	allPrefix     = "ALL:"
	failPrefix    = "FAIL:"
	successPrefix = "SUCCESS:"
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
		Len:  600, // 10 minutes
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
func ShowData(prefixes ...string) map[string]map[string]*Categorys {
	return statistics.ShowCopy(prefixes...)
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
					fn := func(l *list.List, prefix string) {
						if l == nil {
							return
						}
						for item := l.Front(); item != nil; item = item.Next() {
							v := item.Value.(MessageQueue)
							ctg := statistics.channel(prefix+v.GetTopic(), v.GetChannel())
							ctg.AddAt(at, 1)
						}
					}
					fn(all, allPrefix)
					fn(failSrc, failPrefix)
					fn(successSrc, successPrefix)
				}(last, data, fail, success)
			}
		}
	}
}
