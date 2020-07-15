package main

import (
	"log"
	"medispatcher/broker"
	"medispatcher/config"
	"os"
	"os/signal"
	"syscall"
	"time"
	"fmt"
	"net/http"
	_ "net/http/pprof"
)


var queue = "main"
var br = broker.GetBrokerPoolWithBlock(2, 2, func() bool { return false })

func main() {
	config.Setup()
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	go reserveRoutine()
	osSigChan := make(chan os.Signal)
	signal.Notify(osSigChan)
	go http.ListenAndServe("0.0.0.0:6162", nil)
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
			go pubMessage()
		default:
			log.Printf("caught signal: %v", sig)
		}
	}
}

func watchAndReserve(withTimeout bool){
	br.Reserve()
}
