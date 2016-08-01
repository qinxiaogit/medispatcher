package main

import (
	"fmt"
	"log"
	"medispatcher/broker"
	"medispatcher/broker/beanstalk"
	"medispatcher/config"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type stats struct {
	c int
	t time.Duration
}
type StatsAll struct {
	TotalMessagesConsumed int
	TotalMessagesProduced int
	TimeElapsed           time.Duration
	ConsumeOPS            string
	ProduceOPS            string
	Seg                   map[int64]int
	SegOrders             []int64
}

var statsAll = StatsAll{
	Seg:       map[int64]int{},
	SegOrders: []int64{},
}
var statsCollectStartTime time.Time

var consumerC = 500
var consumerConnections = uint32(130)
var consumerCounterChan = make(chan int, 100)
var wg = sync.WaitGroup{}
var exitSig = make(chan bool)
var delMsgChan = make(chan *beanstalk.Msg)
var queue = "event_center_messages"

func main() {
	config.Setup()
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	log.Printf("Starting consumers %v...\n", consumerC)
	wg.Add(consumerC)
	statsCollectStartTime = time.Now()
	go monitorStatsAll()
	deleteMessages()
	consume()
	log.Println("Started")
	osSigChan := make(chan os.Signal)
	signal.Notify(osSigChan)
WAIT:
	for {
		sig := <-osSigChan
		switch sig {
		case syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT:
			select {
			case <-exitSig:
			default:
				log.Println("Stopping...")
				close(exitSig)
				break WAIT
			}
		case syscall.SIGHUP:
			showStatsAll()
		default:
			log.Printf("caught signal: %v", sig)
		}
	}
	wg.Wait()
	showStatsAll()
}

func monitorStatsAll() {
	var cC int
	var lastSumTime = time.Now().Unix()
	for {
		select {
		case cC = <-consumerCounterChan:
			statsAll.TotalMessagesConsumed += cC
			sec := time.Now().Unix()
			var index int64
			if sec-lastSumTime < 5 {
				index = lastSumTime
			} else {
				index = sec
				lastSumTime = sec
				statsAll.SegOrders = append(statsAll.SegOrders, index)
			}
			statsAll.Seg[index] += cC
		}
	}
}

func showStatsAll() {
	statsAll.TimeElapsed = time.Now().Sub(statsCollectStartTime)
	fmt.Print("STATS\n------------------------\n")
	fmt.Printf("TotalMessagesConsumed: %v\n", statsAll.TotalMessagesConsumed)
	fmt.Printf("TimeElapsed: %.3fms\n", float64(statsAll.TimeElapsed.Nanoseconds())*1e-6)
	fmt.Printf("Mean ConsumeOPS: %.3f/s\n", float64(statsAll.TotalMessagesConsumed)/(float64(statsAll.TimeElapsed.Nanoseconds())*1e-9))
	for _, n := range statsAll.SegOrders {
		nt, _ := time.Parse("2006/01/02", "1970/01/02")
		fmt.Printf("ConsumeOPS./ %v: %v ops            %.5f ms/op\n", nt.Add(time.Second*time.Duration(n)).Local().Format("15:04:05"), statsAll.Seg[n]/5, float64(5000)/float64(statsAll.Seg[n]))
	}
	fmt.Print("\r\n")
}

func consume() {
	br := broker.GetBrokerPoolWithBlock(consumerConnections, 2, func() bool { return false })
	br.Watch(queue)
	msgChan := br.Reserve()
	for ; consumerC > 0; consumerC-- {
		go worker(msgChan)
	}

}

func deleteMessages() {
	brP := broker.GetBrokerPoolWithBlock(consumerConnections, 2, func() bool { return false })
	for n := consumerC; n > 0; n-- {
		go func() {
			var msg *beanstalk.Msg
			for {
				select {
				case msg = <-delMsgChan:
					err := brP.Delete(msg)
					if err != nil {
						log.Print(err)
						time.Sleep(time.Second * 2)
					}
				case <-exitSig:
					brP.Close(true)
				}

			}
		}()
	}
}

func worker(msgChan chan *beanstalk.Msg) {
	n := 0
	defer func() {
		wg.Done()
	}()
	for {
		select {
		case msg := <-msgChan:
			n += 1
			consumerCounterChan <- 1
			delMsgChan <- msg
		case <-exitSig:
			return
		}
	}
}
