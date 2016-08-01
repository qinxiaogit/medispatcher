package main

import (
	"fmt"
	"log"
	"medispatcher/broker/beanstalk"
	"medispatcher/config"
	"os"
	"os/signal"
	"strings"
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

type TestMsg struct {
	id        uint64
	queueAddr string
}

var consumerC = 260
var consumerConnections = uint32(30)
var consumerCounterChan = make(chan int, 1000)
var wg = sync.WaitGroup{}
var exitSig = make(chan bool)
var delMsgChan = make(chan *TestMsg)
var queue = "event_center_messages"

func main() {
	config.Setup()
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	log.Printf("Starting consumers %v...\n", consumerC)
	deleteMsg()
	wg.Add(consumerC)
	statsCollectStartTime = time.Now()
	go monitorStatsAll()

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
		fmt.Printf("ConsumeOPS %v: %v ops            %.5f ms/op\n", nt.Add(time.Second*time.Duration(n)).Local().Format("15:04:05"), statsAll.Seg[n]/5, float64(5000)/float64(statsAll.Seg[n]))
	}
	fmt.Print("\r\n")
}

func consume() {
	for n := consumerC; n > 0; n-- {
		go worker()
	}

}

func deleteMsg() {
	addrs := strings.Split(config.GetConfig().QueueServerAddr, ",")
	brs := map[string]*beanstalk.Broker{}
	for _, addr := range addrs {
		br, err := beanstalk.New(addr)
		if err != nil {
			log.Println(err)
			return
		}
		brs[addr] = br
	}
	for n := consumerC * 2; n > 0; n-- {
		go func() {
			for {
				select {
				case m := <-delMsgChan:
					brs[m.queueAddr].Delete(m.id)
				case <-exitSig:
					return
				}
			}
		}()
	}
}

func worker() {

	defer func() {
		wg.Done()
	}()
	addrs := strings.Split(config.GetConfig().QueueServerAddr, ",")
	for _, addr := range addrs {
		go func(addr string) {
			br, err := beanstalk.New(addr)
			// br, err := beanstalkc.Dial(addr)
			if err != nil {
				log.Println(err)
				return
			}
			err = br.Watch(queue)
			if err != nil {
				log.Println(err)
				return
			}
			for {

				id, _, e := br.Reserve()

				if e != nil {
					log.Print(e)
					return
				} else {
					// br.Bury(id, 1024)
					br.Bury(id)
					// br.Delete(id)
				}
				consumerCounterChan <- 1
				delMsgChan <- &TestMsg{id: id, queueAddr: addr}
				select {
				case <-exitSig:
					return
				default:
				}
			}
		}(addr)
	}
	<-exitSig
}
