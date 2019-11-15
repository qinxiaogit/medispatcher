package pushstatistics

import (
	"container/list"
	"time"
	"sync"
	"medispatcher/broker"
	"medispatcher/logger"
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

// 存放队列阻塞的统计结果
var queueBlocked sync.Map
// 队列阻塞的统计间隔(秒)
var queueBlockedStatsInterval = 10

// PrometheusStatisticsStart PrometheusStatisticsStart
func PrometheusStatisticsStart(addr string) {
	allData = NewMQList()
	failData = NewMQList()
	successData = NewMQList()
	statistics = &Statistics{
		Len:  600, // 10 minutes
		Data: map[string]map[string]*Categorys{},
	}

	go blockedStats()
	go run()
	go httpRun(addr)
}

// blockedStats 统计队列阻塞
func blockedStats() {
	brPool := broker.GetBrokerPoolWithBlock(1, 3, func() bool {
		return false
	})
	if brPool == nil {
		logger.GetLogger("WARN").Printf("blockedStats: Unable to get connection pool object\r\n")
		return
	}

	t := time.NewTicker(time.Second * time.Duration(queueBlockedStatsInterval))
	// var currentTime int64
	for {
		/* if currentTime > 0 {
			logger.GetLogger("WARN").Printf("blockedStats cost: %d\r\n", time.Now().Unix()-currentTime)
		} */

		topicBlocked := map[string]int{}
		select {
		case <-t.C:
			// currentTime = time.Now().Unix()
			// topics[br.addr] = brTopics
			topicsResult, err := brPool.ListTopics()
			if err != nil {
				logger.GetLogger("WARN").Printf("blockedStats error:%v", err)
				continue
			}

			for _, topics := range topicsResult {
				for _, topic := range topics {
					if _, ok := topicBlocked[topic]; !ok {
						statsResult, err := brPool.StatsTopic(topic)
						if err != nil {
							continue
						}

						topicBlocked[topic] = 0

						for _, stats := range statsResult {
							if _, ok := stats["current-jobs-ready"]; !ok {
								continue
							}

							if currentJobsReady, ok := stats["current-jobs-ready"].(int); ok {
								topicBlocked[topic] += currentJobsReady
							}
						}
					}
				}
			}

			for topic, blockedNum := range topicBlocked {
				queueBlocked.Store(topic, blockedNum)
			}

			// 删除不存在的topic
			queueBlocked.Range(func(key, val interface{}) bool {
				if _, ok := topicBlocked[key.(string)]; !ok {
					queueBlocked.Delete(key)
				}
				return true
			})
		}
	}
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
