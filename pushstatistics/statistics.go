package pushstatistics

import (
	"container/list"
	"time"
)

var (
	poolData   *MQList
	statistics *Statistics
)

func init() {
	poolData = NewMQList()
	statistics = &Statistics{
		Len:  86400, // 1day
		Data: map[string]map[string]*Categorys{},
	}
	go run()
	go httpRun()
}

// Add will 添加统计
func Add(mq MessageQueue, count int) {
	if mq == nil {
		return
	}
	for i := 0; i < count; i++ {
		poolData.push(mq)
	}
}

// ShowData will 返回当前统计数据
func ShowData() map[string]map[string]*Categorys {
	return statistics.Show()
}

func run() {
	t := time.NewTicker(time.Second)
	for {
		select {
		case <-t.C:
			last := statistics.Tick()
			data := poolData.pop()
			if data.Len() > 0 {
				go func(at uint, source *list.List) {
					if source == nil {
						return
					}
					for item := source.Front(); item != nil; item = item.Next() {
						v := item.Value.(MessageQueue)
						ctg := statistics.channel(v.GetTopic(), v.GetChannel())
						ctg.AddAt(at, 1)
					}
				}(last, data)
			}
		}
	}
}
