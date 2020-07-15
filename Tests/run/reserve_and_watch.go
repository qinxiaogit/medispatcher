package main

import (
	"log"
	"git.oschina.net/chaos.su/beanstalkc"
	"os"
	"os/signal"
	"syscall"
	"time"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"flag"
)


var queue = "main"
//var br = broker.GetBrokerPoolWithBlock(2, 2, func() bool { return false })
var br0, _ = beanstalkc.Dial("127.0.0.1:11300")
var br1, _ = beanstalkc.Dial("127.0.0.1:11300")
var withTimeout bool
func main() {
	a := flag.NewFlagSet("", flag.PanicOnError)
	a.BoolVar(&withTimeout, "t", false, "reserve with timeout")
	a.Parse(os.Args[1:])
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	osSigChan := make(chan os.Signal)
	signal.Notify(osSigChan)
	go http.ListenAndServe("0.0.0.0:6162", nil)
	br0.Use(queue)
	go pubMessage()
	startWatch()
	go reserveRoutine()
	WAIT:
	for {
		sig := <-osSigChan
		switch sig {
		case syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT:
			log.Println("Stopping...")
			break WAIT
		case syscall.SIGUSR1:
			go startWatch()
		case syscall.SIGUSR2:
			go cancelWatch()
		case syscall.SIGHUP:
			//go pubMessage()
		default:
			log.Printf("caught signal: %v", sig)
		}
	}
}

func pubMessage(){
	for {
		msg := fmt.Sprintf("message %v", time.Now().String())

		br0.Put(1024, 0, 3, []byte(msg), )
		log.Printf("pub message to queue %v, \n %v\n", queue, msg)
		time.Sleep(time.Microsecond * 1000)
	}
}
func startWatch(){
	br1.Watch(queue)
	log.Printf("watch queue %v\n", queue)
}

func cancelWatch(){
	br1.Ignore(queue)
	log.Printf("cancel watch queue %v\n", queue)
}
func reserveRoutine() {
	log.Printf("start receiving messages from %+v\n", queue)
	for{
		if withTimeout {
			id, _, _ := br1.ReserveWithTimeout(1)

			log.Printf("Tget message id: %v", id)
			br1.Delete(id)
		} else {
			id, _, _ := br1.Reserve()
			log.Printf("get message id: %v", id)
			br1.Delete(id)
		}
	}
}
