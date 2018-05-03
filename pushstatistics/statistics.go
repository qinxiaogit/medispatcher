package pushstatistics

import (
	"sync"
	"time"
)

var (
	poolData    *MQList
	statistics  *Statistics
	initCurData sync.Once
)

func init() {
	initCurData.Do(func() {
		poolData = NewMQList()
		statistics = &Statistics{
			Data: map[string]map[string]*Categorys{},
		}
		go run()
	})
}

// Add will 添加统计
func Add(mq MessageQueue, count int) {
	if mq == nil {
		return
	}
	poolData.push(mq)
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
			statistics.Tick()
			data := poolData.GetAllData()
			if data.Len() > 0 {
				go func() {
					for item := data.Front(); item != nil; item = item.Next() {
						v := item.Value.(MessageQueue)
						ctg := statistics.channel(v.GetTopic(), v.GetChannel())
						ctg.Add(1)
					}
				}()
			}
		}
	}
}
