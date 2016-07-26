package main

import (
	"fmt"
	"time"
	"sync"
	"os/signal"
	"os"
	"syscall"
	"log"
	"medispatcher/broker"
	"medispatcher/config"
	"gopkg.in/vmihailenco/msgpack.v2"
	"math/rand"
)

type stats struct{
	c int
	t time.Duration
}
type StatsAll struct{
	TotalMessagesConsumed int
	TotalMessagesProduced int
	TimeElapsed time.Duration
	ConsumeOPS   string
	ProduceOPS   string
	Seg map[int64]int
	SegOrders []int64
}
var statsAll = StatsAll{
	Seg: map[int64]int{},
	SegOrders: []int64{},
}
var statsCollectStartTime time.Time
var producerC = 130
var host = "10.0.19.231:11304,10.0.19.231:11305"
var produceCounterChan = make(chan int, 100)
var wg = sync.WaitGroup{}
var exitSig = make(chan bool)
//var queues = []string{"CoutuanSucceed","user_info_changed", "crm_event_order_paid"}
var queues = []string{"event_center_messages"}
func main(){
	config.Setup()
	log.SetFlags(log.Lshortfile|log.LstdFlags)
	log.Printf("Starting bench with  %v producers...\n", producerC)
	wg.Add(producerC)
	statsCollectStartTime = time.Now()
	for ; producerC > 0; producerC--{
		go produce()
	}
	go monitorStatsAll()
	log.Println("Started")
	osSigChan := make(chan os.Signal)
	signal.Notify(osSigChan)
	WAIT:
	for{
		sig := <- osSigChan
		switch sig{
		case syscall.SIGTERM, syscall.SIGQUIT,syscall.SIGINT:
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

func monitorStatsAll(){
	var pC int
	var lastSumTime = time.Now().Unix()
	for{
		select{
		case pC = <-produceCounterChan:
			statsAll.TotalMessagesProduced += pC
			sec := time.Now().Unix()
			var index int64
			if sec - lastSumTime < 5 {
				index = lastSumTime
			} else {
				index = sec
				lastSumTime = sec
				statsAll.SegOrders = append(statsAll.SegOrders, index)
			}
			statsAll.Seg[index] += pC
		}
	}
}

func showStatsAll(){
	statsAll.TimeElapsed = time.Now().Sub(statsCollectStartTime)
	fmt.Print("STATS\n------------------------\n")
	fmt.Printf("TotalMessagesProduced: %v\n", statsAll.TotalMessagesProduced)
	fmt.Printf("TimeElapsed: %.3fms\n", float64(statsAll.TimeElapsed.Nanoseconds()) * 1e-6)
	fmt.Printf("Mean ProduceOPS: %.3f/s\n", float64(statsAll.TotalMessagesProduced)/(float64(statsAll.TimeElapsed.Nanoseconds()) * 1e-9))
	for _, n := range statsAll.SegOrders {
		nt, _ := time.Parse("2006/01/02", "1970/01/02")
		fmt.Printf("ProduceOPS %v: %v ops            %.5f ms/op\n", nt.Add(time.Second * time.Duration(n)).Local().Format("15:04:05"), statsAll.Seg[n]/5, float64(5000)/float64(statsAll.Seg[n]))
	}
	fmt.Print("\r\n")
}


func produce(){
	br := broker.GetBrokerPoolWithBlock(1, 2, func()bool{return false})
	var err error
	defer func(){
		wg.Done()
	}()
	queueLen := len(queues)
	keys := []string{"test", "test_1"}
	for{
		select{
		case <-exitSig:
			br.Close(true)
			return
		default:
		       nt := time.Now()
			rand.Seed(time.Now().UnixNano())
			key := keys[rand.Intn(len(keys))]
			data := map[string]interface{}{
				"msgKey": key,
				"body": map[string]interface{}{
					"shipping_no":[]int64{245132238},
					"order_id": 773372311,
					"uid": 38953296,
				},
				"time": float64(nt.Unix()) + float64(nt.Nanosecond()) * 1e-9,
				"sender": "test",
				"priority": 1024,
				"delay": 0,
			}
			b, e := msgpack.Marshal(data)
			if e != nil {
				log.Println(e)
			}
			for _, queue := range queues{
				_, err = br.Pub(queue, b, broker.DEFAULT_MSG_PRIORITY, 0, broker.DEFAULT_MSG_TTR)
				if err != nil {
					log.Print(err)
				}
			}
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(60)))

			produceCounterChan <- queueLen
		}
	}
}