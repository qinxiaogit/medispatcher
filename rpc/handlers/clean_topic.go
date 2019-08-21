package handlers

import (
	"fmt"
	"medispatcher/rpc"
	"net/http"
	"sync"
	"time"

	"git.oschina.net/chaos.su/beanstalkc"
	"github.com/labstack/gommon/log"
)

var QueueCleanWaitGroup sync.WaitGroup

type CleanQueue struct {
}

func init() {
	rpc.RegisterHandlerRegister("CleanTopic", CleanQueue{})
}

func (self CleanQueue) Callback(url string) {
	if url == "" {
		return
	}

	var counter uint8 = 0
	var res *http.Response
	var err error
	for counter < 3 {
		counter++

		res, err = http.Get(url)
		if err == nil && res.StatusCode == 200 {
			log.Infof("回调完成: %v", url)
			return
		}

		time.Sleep(1 * time.Second)
	}

	if err == nil {
		log.Warnf("回调失败: %v, CODE: %v", url, res.StatusCode)
		return
	}

	log.Warnf("回调失败: %v, ERR: %v", url, err)
}

func (self CleanQueue) Process(args map[string]interface{}) (interface{}, error) {
	var exists bool
	var topic string
	if _, exists = args["servers"]; !exists {
		return nil, fmt.Errorf("servers not found!")
	}

	res := args["servers"].([]interface{})
	if len(res) == 0 {
		return nil, fmt.Errorf("servers is empty!")
	}

	var servers []string
	for _, v := range res {
		servers = append(servers, v.(string))
	}

	if _, exists = args["topic"]; !exists {
		return nil, fmt.Errorf("topic not found!")
	}

	if _, exists = args["topic"].(string); !exists {
		return nil, fmt.Errorf("topic not found!")
	}

	topic = args["topic"].(string)
	if topic == "" {
		return nil, fmt.Errorf("topic is empty")
	}

	callback := ""
	if _, exists = args["callback"]; exists {
		callback = args["callback"].(string)
	}

	// 执行异步清理任务.
	QueueCleanWaitGroup.Add(1)
	go func(servers []string, topic, callback string) {
		defer QueueCleanWaitGroup.Done()

		var waitgroup sync.WaitGroup
		for _, host := range servers {
			log.Infof("开始清除队列: %v ===> %v", host, topic)
			waitgroup.Add(1)
			// 清理特定beanstalk实例上特定的topic.
			go func(host, topic string) {
				defer waitgroup.Done()

				c, err := beanstalkc.Dial(host)
				if err != nil {
					log.Errorf("%v", err)
					return
				}

				defer c.Conn.Close()
				c.Watch(topic)

				d, err := beanstalkc.Dial(host)
				if err != nil {
					log.Errorf("%v", err)
					return
				}

				defer d.Conn.Close()

				// lock := new(sync.Mutex)
				// 检测清理是否完成.
				fin := time.NewTimer(time.Second * 10)
				report := time.NewTimer(time.Second * 5)
				// 清理总数.
				total := 0

				// 将job的状态由delay迁移到ready
				/* go func() {
					for {
						lock.Lock()
						actuallyKicked, err := c.Kick(500)
						lock.Unlock()
						if err != nil {
							log.Warnf("执行kick操作时发生错误: %v ===> %v, ERR: %v", host, topic, err)
							return
						}

						if actuallyKicked == 0 {
							log.Infof("kick操作执行完毕: %v ===> %v", host, topic)
							return
						}
					}
				}() */
				log.Infof("开始执行kick操作: %v ===> %v", host, topic)
				for {
					actuallyKicked, err := c.Kick(500)
					if err != nil {
						log.Warnf("执行kick操作时发生错误: %v ===> %v, ERR: %v", host, topic, err)
						return
					}

					if actuallyKicked == 0 {
						log.Infof("kick操作执行完毕: %v ===> %v", host, topic)
						break
					}
				}

				go func() {
					t := time.NewTimer(time.Minute * 5)
					for {
						select {
						case <-t.C:
							log.Infof("清理操作超时, 终止队列清理操作: %v ===> %v", host, topic)
							return
						default:
						}

						if isServerStopping() {
							log.Infof("进程即将退出, 终止队列清理操作: %v ===> %v", host, topic)
							return
						}

						//lock.Lock()
						id, _, err := c.Reserve()
						//lock.Unlock()
						if err != nil {
							log.Warnf("从队列中获取消息失败: %v ===> %v, ERR: %v", host, topic, err)
							return
						}

						//lock.Lock()
						err = c.Delete(id)
						//lock.Unlock()
						if err != nil {
							log.Warnf("从队列中删除消息失败: %v ===> %v, ERR: %v", host, topic, err)
						} else {
							total++
						}

						fin.Reset(time.Second * 5)
					}
				}()

				for {
					select {
					case <-fin.C:
						log.Infof("队列清除完成: %v ===> %v, 清理任务数: %v", host, topic, total)
						return
					case <-report.C:
						stats, err := d.StatsTube(topic)

						if err == nil {
							log.Infof("队列剩余长度: %v ==> %v: ready: %v, delay: %v", host, topic, stats["current-jobs-ready"], stats["current-jobs-delayed"])
						}

						self.Callback(callback + "&state=process")
						report.Reset(time.Second * 5)
					}
				}

			}(host, topic)
		}

		waitgroup.Wait()
		// 清理完成后执行回调.
		self.Callback(callback + "&state=done")
	}(servers, topic, callback)

	return nil, nil
}
