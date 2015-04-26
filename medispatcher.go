package main

import (
	"encoding/json"
	"fmt"
	"medispatcher/config"
	"medispatcher/logger"
	"medispatcher/msgredist"
	"medispatcher/recoverwatcher"
	rpcSrv "medispatcher/rpc/connection"
	"medispatcher/sender"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"
)

var exitSigChan = make(chan int8)

func main() {
	err := config.Setup()
	if err != nil {
		fmt.Printf("Failed to setup configs: %v", err)
		os.Exit(1)
	}
	logger.InitDefaultLogger()

	go ProcessSysSignal()
	go rpcSrv.StartServer()
	go recoverwatcher.StartAndWait()
	go msgredist.StartAndWait()
	go sender.StartAndWait()
	<-exitSigChan
}

// 系统信号处理
func ProcessSysSignal() {
	var exitSigSent bool
	// 系统信号监控通道
	osSingalChan := make(chan os.Signal, 10)

	signal.Notify(osSingalChan)

	var sigusr1 = syscall.Signal(0xa)

	// 在此阻塞，监控并处理系统信号
	for {
		osSingal := <-osSingalChan
		go func(sig *os.Signal) {
			switch *sig {
			case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
				if !exitSigSent {
					logger.GetLogger("INFO").Print("Quit signal received! exit...\n")
					exitSigSent = true
				} else {
					logger.GetLogger("INFO").Print("Quit signal already sent! ignore...\n")
					return
				}
				workerExitSigChan := make(chan string)
				go rpcSrv.Stop(&workerExitSigChan)
				go msgredist.Stop(&workerExitSigChan)
				go recoverwatcher.Stop(&workerExitSigChan)
				go sender.Stop(&workerExitSigChan)
				sigReceivedCount := 0
				for sigReceivedCount < 3 {
					select {
					case workerName := <-workerExitSigChan:
						sigReceivedCount += 1
						logger.GetLogger("INFO").Printf("%v worker exited.", workerName)
					default:
						time.Sleep(time.Millisecond * 40)
					}
				}
				exitSigChan <- int8(1)

			case syscall.SIGALRM:
				logger.GetLogger("INFO").Print("Received manual GC signal, starting free OS memory!")
				gcStats := debug.GCStats{}
				debug.ReadGCStats(&gcStats)
				gcStatsOut, _ := json.MarshalIndent(gcStats, "", "")
				fmt.Fprintf(os.Stdout, "%s\r\n", gcStatsOut)
				debug.FreeOSMemory()
			case sigusr1:
				fmt.Printf("Current goroutines: %d\n", runtime.NumGoroutine())
			// case syscall.SIGUSR2:
			// 	fmt.Printf("Configs: %s\n", config.GetConfig())
			case syscall.SIGHUP:
				curDebugMode := !config.DebugEnabled()
				logger.GetLogger("INFO").Printf("Received force toggle DEBUG mode signal, setting DEBUG mode to : %v", curDebugMode)
				config.SetDebug(curDebugMode)
			default:
				if config.DebugEnabled() {
					logger.GetLogger("INFO").Printf("un-expected signal: %s(%d)\n", *sig, *sig)
				}
			}
		}(&osSingal)
	}
}
