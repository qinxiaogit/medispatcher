package main

import (
	"fmt"
	"log"
	"math/rand"
	"medispatcher/broker/beanstalk"
	"medispatcher/config"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/vmihailenco/msgpack.v2"
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

var consumerC = 10
var consumerConnections = uint32(30)
var consumerCounterChan = make(chan int, 1000)
var wg = sync.WaitGroup{}
var exitSig = make(chan bool)
var delMsgChan = make(chan *beanstalk.Msg)
var queue = "event_center_messages"

func main() {
	config.Setup()
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	log.Printf("Starting producers %v...\n", consumerC)
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
	fmt.Printf("TotalMessagesProduced: %v\n", statsAll.TotalMessagesConsumed)
	fmt.Printf("TimeElapsed: %.3fms\n", float64(statsAll.TimeElapsed.Nanoseconds())*1e-6)
	fmt.Printf("Mean ProduceOPS: %.3f/s\n", float64(statsAll.TotalMessagesConsumed)/(float64(statsAll.TimeElapsed.Nanoseconds())*1e-9))
	for _, n := range statsAll.SegOrders {
		nt, _ := time.Parse("2006/01/02", "1970/01/02")
		fmt.Printf("ProduceOPS %v: %v ops            %.5f ms/op\n", nt.Add(time.Second*time.Duration(n)).Local().Format("15:04:05"), statsAll.Seg[n]/5, float64(5000)/float64(statsAll.Seg[n]))
	}
	fmt.Print("\r\n")
}

func consume() {
	for n := consumerC; n > 0; n-- {
		go worker()
	}

}

func worker() {

	defer func() {
		wg.Done()
	}()
	keys := []string{"test", "test_1"}
	addrs := strings.Split(config.GetConfig().QueueServerAddr, ",")
	for _, addr := range addrs {
		go func(addr string) {
			br, err := beanstalk.New(addr)
			// br, err := beanstalkc.Dial(addr)
			if err != nil {
				log.Println(err)
				return
			}
			err = br.Use(queue)
			if err != nil {
				log.Println(err)
				return
			}
			for {
				nt := time.Now()
				rand.Seed(time.Now().UnixNano())
				key := keys[rand.Intn(len(keys))]
				data := map[string]interface{}{
					"msgKey": key,
					"body": map[string]interface{}{
						"shipping_no": []int64{245132238},
						"order_id":    773372311,
						"uid":         38953296,
					},
					"time":     float64(nt.Unix()) + float64(nt.Nanosecond())*1e-9,
					"sender":   "test",
					"priority": 1024,
					"delay":    0,
				}
				b, e := msgpack.Marshal(data)
				if e != nil {
					log.Println(e)
				}
				_, e = br.Pub(1024, 0, 60, b)

				if e != nil {
					log.Print(e)
					return
				}

				ots := time.Now()
				consumerCounterChan <- 1
				select {
				case <-exitSig:
					return
				default:
				}
				println(fmt.Sprintf("%.5f\n", float64(time.Now().Sub(ots).Nanoseconds())*1e-6))
			}
		}(addr)
	}
	<-exitSig
}
