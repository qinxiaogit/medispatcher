package main

import (
	"os"
	"os/signal"
	"time"
	"syscall"
	"fmt"
	"git.oschina.net/chaos.su/beanstalkc"
)

const Bench_Test_Queue_1 = "Bench_Test_Queue_2"
var consumerCounterChan = make(chan int, 100)
var c = 0
func main() {
	st := time.Now()
	go produce()
	go m()
	go consume()
	sigC := make(chan os.Signal)
	signal.Notify(sigC)
	for {
		switch <-sigC{
		case syscall.SIGQUIT, syscall.SIGINT, syscall.SIGTERM:
			fmt.Printf("%v, %v\n", time.Now().Sub(st).Seconds(), c)
			fmt.Printf("%.3f\r\n", float64(c)/(float64(time.Now().Sub(st).Nanoseconds()) * 1e-9))
			return
		}
	}
}
func produce(){
	br, err := beanstalkc.Dial("127.0.0.1:11300")
	if err != nil {
		panic(err)
	}
	br.Use(Bench_Test_Queue_1)
	for{
		time.Sleep(time.Millisecond * 20)
		continue
		id, _, _ := br.Put(1024, 0, 60, []byte("测试消息"))
		fmt.Println(id)
	}
}
func consume(){
	br, err := beanstalkc.Dial("127.0.0.1:11300")
	if err != nil {
		panic(err)
	}
	br.Watch(Bench_Test_Queue_1)
	for{

		id, _, _ := br.Reserve()
		br.Delete(id)
		consumerCounterChan <- 1
	}
}
func m(){
	for{
		select {
		case n := <-consumerCounterChan:
			c += n
		}
	}
}