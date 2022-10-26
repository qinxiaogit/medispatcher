// 修复处于中间状态的消息，如： 将异常buried状态的消息恢复到ready状态，以便继续处理(根据不同状态产生的原因，可能会导致重复消息，订阅者需注意去重)。
package main

import (
	"log"
	"github.com/qinxiaogit/medispatcher/broker/beanstalk"
	"github.com/qinxiaogit/medispatcher/config"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const ErrNotFound = "NOT_FOUND"

func main() {
	log.SetFlags(log.Lshortfile | log.Lshortfile)
	var concurrency uint
	flags := config.GetFlags()
	flags.UintVar(&concurrency, "c", uint(2), "concurrent processing routines.")
	err := config.Setup()
	if err != nil {
		log.Panicf("Config setup err: %v", err)
	}

	ctlSig := make(chan os.Signal)
	signal.Notify(ctlSig)
	wg := new(sync.WaitGroup)
	var runningWorkers int32
	exitSigChan := make(chan bool)
	brokerAddrs := strings.Split(config.GetConfig().QueueServerAddr, ",")
	for i := range brokerAddrs {
		wg.Add(1)
		go func(addr string) {
			atomic.AddInt32(&runningWorkers, 1)
			defer func() {
				e := recover()
				if err != nil {
					log.Printf("mainqueue on: %v, ERR: %v", addr, e)
				}
				wg.Done()
				atomic.AddInt32(&runningWorkers, -1)
			}()
			br, err := beanstalk.New(addr)
			if err != nil {
				log.Panic(err)
			}
			subQueues, err := br.ListTopics()
			if err != nil {
				log.Printf("Failed to list queues on %v.", addr)
				return
			}
			for _, queueName := range subQueues {
				err = kickBuriedMessages(br, addr, queueName)
				if err != nil {
					return
				}
			}

		}(brokerAddrs[i])
	}

WAIT:
	for {
		select {
		case sig := <-ctlSig:
			switch sig {
			case syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT:
				close(exitSigChan)
				wg.Wait()
				break WAIT
			}
		default:
			time.Sleep(time.Millisecond * 400)
			if atomic.LoadInt32(&runningWorkers) < 1 {
				log.Printf("All works done, exit.")
				break WAIT
			}
		}
	}

}

func kickBuriedMessages(br *beanstalk.Broker, brokerAddr, queueName string) (err error) {
	log.Printf("CHECKING on queue[%v]: %v...", queueName, brokerAddr)
	var queueStats map[string]interface{}
	queueStats, err = br.StatsTopic(queueName)
	if err != nil {
		log.Printf("Failed to stats queue[%v] on %v", queueName, brokerAddr)
		return
	}
	watchers, ok := queueStats["current-watching"].(int)
	if !ok {
		log.Printf("ERR: watchers test error on queue[%v] on [%v]!", queueName, brokerAddr)
		return
	}
	if watchers > 0 {
		log.Printf("Queue[%v] on [%v] is under watching[%v], kick action skipped, now exit routine for it.\n", queueName, brokerAddr, watchers)
		return
	}

	buriedMessages, _ := queueStats["current-jobs-buried"].(int)

	if buriedMessages < 1 {
		log.Printf("There's no buried messages on queue[%v], kick action on main queue skipped for %v...\n", queueName, brokerAddr)
	} else {
		log.Printf("Found [%v] buried messages, starting kicking messages on queue[%v] on %v...\n", buriedMessages, queueName, brokerAddr)
		err = br.Use(queueName)
		if err != nil {
			log.Printf("Failed to use queue[%v] on %v, ERR: %v", queueName, brokerAddr, err)
			return
		}
		kicked := 0
		for kicked < buriedMessages {
			log.Printf("Try to kick [%v] messages on queue[%v] for %v...", buriedMessages, queueName, brokerAddr)
			var n uint64
			n, err = br.Kick(buriedMessages)
			if err != nil {
				log.Printf("Unexpected err [%v], when kicking on queue[%v] on %v.", err, queueName, brokerAddr)
				return
			}

			log.Printf("Kicked [%v] messages on queue[%v] on %v.", n, queueName, brokerAddr)
			queueStats, err = br.StatsTopic(queueName)
			if err != nil {
				log.Printf("Failed to stats queue[%v] on %v", queueName, brokerAddr)
				return
			}
			watchers, ok := queueStats["current-watching"].(int)
			if !ok {
				log.Printf("ERR: watchers test error on queue[%v] on [%v]!", queueName, brokerAddr)
				return
			}
			if watchers > 0 {
				log.Printf("Queue[%v] on [%v] is under watching[%v], kick action skipped, now exit routine for it.\n", queueName, brokerAddr, watchers)
				return
			}
			buriedMessages, _ = queueStats["current-jobs-buried"].(int)
			if buriedMessages <= 0 {
				log.Printf("All buried message are kicked on queue[%v] on %v.", queueName, brokerAddr)
				break
			}
		}
	}
	return
}
