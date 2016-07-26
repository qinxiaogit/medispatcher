package main

import (
	"medispatcher/broker/beanstalk"
	"fmt"
	"time"
	"sync"
	"os/signal"
	"os"
	"syscall"
	"log"
	"medispatcher/broker"
	"medispatcher/config"
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
}
var statsAll = StatsAll{}
var statsCollectStartTime time.Time
var producerC = 130
var consumerC = 500
var consumerConnections = uint32(30)
var host = "127.0.0.1:11300,127.0.0.1:11301"
var produceCounterChan = make(chan int, 100)
var consumerCounterChan = make(chan int, 100)
var wg = sync.WaitGroup{}
var exitSig = make(chan bool)
var delMsgChan = make(chan *beanstalk.Msg)
const (
	Bench_Test_Queue_1 = "Bench_Test_Queue_1"
	Bench_Test_Queue_2 = "Bench_Test_Queue_2"
	Bench_Test_Queue_3 = "Bench_Test_Queue_3"
)

func main(){
	config.Setup()
	log.SetFlags(log.Lshortfile|log.LstdFlags)
	log.Printf("Starting bench with %v consumers and %v producers...\n", consumerC, producerC)
	wg.Add(consumerC)
	wg.Add(producerC)
	statsCollectStartTime = time.Now()
	for ; producerC > 0; producerC--{
		go produce()
	}
	go monitorStatsAll()
	go deleteMessages()
	consume()
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
			case syscall.SIGINFO:
				showStatsAll()
			default:
				log.Printf("caught signal: %v", sig)
		}
	}

	wg.Wait()
	showStatsAll()
}

func monitorStatsAll(){
	var pC, cC int
	for{
		select{
		case pC = <-produceCounterChan:
			statsAll.TotalMessagesProduced += pC
		case cC = <-consumerCounterChan:
			statsAll.TotalMessagesConsumed += cC
		}
	}
}

func showStatsAll(){
	statsAll.TimeElapsed = time.Now().Sub(statsCollectStartTime)
	fmt.Print("STATS\n------------------------\n")
	fmt.Printf("TotalMessagesProduced: %v\n", statsAll.TotalMessagesProduced)
	fmt.Printf("TotalMessagesConsumed: %v\n", statsAll.TotalMessagesConsumed)
	fmt.Printf("TimeElapsed: %.3fms\n", float64(statsAll.TimeElapsed.Nanoseconds()) * 1e-6)
	fmt.Printf("ConsumeOPS: %.3f/s\n", float64(statsAll.TotalMessagesConsumed)/(float64(statsAll.TimeElapsed.Nanoseconds()) * 1e-9))
	fmt.Printf("ProduceOPS: %.3f/s\n", float64(statsAll.TotalMessagesProduced)/(float64(statsAll.TimeElapsed.Nanoseconds()) * 1e-9))
	fmt.Print("\r\n")
}

func consume(){
	br, err := beanstalk.NewSafeBrokerPool(host, consumerConnections)
	if err != nil {
		panic(err)
	}
	br.Watch(Bench_Test_Queue_1)
	br.Watch(Bench_Test_Queue_2)
	br.Watch(Bench_Test_Queue_3)
	msgChan := br.Reserve()
	for ;consumerC > 0; consumerC--{
		go worker(msgChan)
	}

}

func deleteMessages(){
	brP, err := beanstalk.NewSafeBrokerPool(host, consumerConnections)
	if err != nil {
		panic(err)
	}
	var msg *beanstalk.Msg
	for{
		select {
		case msg = <-delMsgChan:
		err = brP.Delete(msg)
		if err != nil {
			log.Print(err)
			time.Sleep(time.Second * 2)
		}
		case <-exitSig:
			brP.Close(true)
		}

	}
}


func worker(msgChan chan *beanstalk.Msg){
	n := 0
	defer func(){
		wg.Done()
	}()
	for{
		select {
		case  msg := <-msgChan:
			n += 1
			consumerCounterChan <- 1
			delMsgChan <- msg
		case <-exitSig:
			return
		}
	}
}

func produce(){
	br, err := beanstalk.NewSafeBrokerPool(host, 1)
	if err != nil {
		log.Print(err)
	}
	n := 0
	defer func(){
		wg.Done()
	}()
	for{
		select{
		case <-exitSig:
			br.Close(true)
		       return
		default:
			_, err = br.Pub(Bench_Test_Queue_1, []byte(fmt.Sprintf("测试数据: %v", time.Now().UnixNano())), broker.DEFAULT_MSG_PRIORITY, 0, broker.DEFAULT_MSG_TTR)
		        if err != nil {
				log.Print(err)
				time.Sleep(time.Second * 2)
				continue
			}
			br.Pub(Bench_Test_Queue_2, []byte(fmt.Sprintf("测试数据: %v", time.Now().UnixNano())), broker.DEFAULT_MSG_PRIORITY, 0, broker.DEFAULT_MSG_TTR)
			br.Pub(Bench_Test_Queue_3, []byte(fmt.Sprintf("测试数据: %v", time.Now().UnixNano())), broker.DEFAULT_MSG_PRIORITY, 0, broker.DEFAULT_MSG_TTR)
			n += 3
			produceCounterChan <- 3
		//time.Sleep(time.Millisecond * 20)
		}
	}
}