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
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	l "github.com/sunreaver/gotools/logger"
)

var exitSigChan = make(chan bool)

func main() {

	err := config.Setup()
	if err != nil {
		fmt.Printf("Failed to setup configs: %v", err)
		os.Exit(1)
	}

	l.InitLogger(config.GetConfig().LOG_DIR, l.DebugLevel, time.FixedZone("Asia/Shanghai", 8*3600))
	l.LoggerByDay.Debugw("Setup", "Config", config.GetConfig())

	go http.ListenAndServe(config.GetConfig().DebugAddr, nil)
	maxProcs := runtime.NumCPU()
	if maxProcs > 1 {
		maxProcs--
	}
	runtime.GOMAXPROCS(maxProcs)

	logger.InitDefaultLogger()
	if !config.GetConfig().SplitLog {
		logger.SetSingleFile()
	}

	go ProcessSysSignal()
	go rpcSrv.StartServer()
	go recoverwatcher.StartAndWait()
	go msgredist.StartAndWait()
	go sender.StartAndWait()
	<-exitSigChan
}

// ProcessSysSignal 系统信号处理
func ProcessSysSignal() {
	var exitSigSent bool
	var exitSigSentLock = make(chan bool, 1)
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
				exitSigSentLock <- true
				if !exitSigSent {
					logger.GetLogger("INFO").Print("Quit signal received! exit...\n")
					exitSigSent = true
					<-exitSigSentLock
				} else {
					logger.GetLogger("INFO").Print("Quit signal already sent! ignore...\n")
					<-exitSigSentLock
					return
				}
				workerExitSigChan := make(chan string)
				go rpcSrv.Stop(&workerExitSigChan)
				go msgredist.Stop(&workerExitSigChan)
				go recoverwatcher.Stop(&workerExitSigChan)
				go sender.Stop(&workerExitSigChan)
				sigReceivedCount := 0
				for sigReceivedCount < 4 {
					select {
					case workerName := <-workerExitSigChan:
						sigReceivedCount++
						logger.GetLogger("INFO").Printf("%v worker exited.", workerName)
					default:
						time.Sleep(time.Millisecond * 40)
					}
				}
				logger.Flush()
				close(exitSigChan)

			case syscall.SIGALRM:
				logger.GetLogger("INFO").Print("Received manual GC signal, starting free OS memory!")
				gcStats := debug.GCStats{}
				debug.ReadGCStats(&gcStats)
				gcStatsOut, _ := json.MarshalIndent(gcStats, "", "")
				fmt.Fprintf(os.Stdout, "%s\r\n", gcStatsOut)
				debug.FreeOSMemory()
			case sigusr1:
				fmt.Printf("Current goroutines: %d\n", runtime.NumGoroutine())
			case syscall.SIGUSR2:
				fmt.Printf("Current goroutines: %d\n", runtime.NumGoroutine())
				//fmt.Printf("Configs: %s\n", config.GetConfig())
			case syscall.SIGHUP:
				curDebugMode := !config.DebugEnabled()
				logger.GetLogger("INFO").Printf("Received force toggle DEBUG mode signal, setting DEBUG mode to : %v", curDebugMode)
				config.SetDebug(curDebugMode)
			default:
				if config.DebugEnabled() {
					logger.GetLogger("INFO").Printf("un-expected signal: %s(%v)\n", *sig, *sig)
				}
			}
		}(&osSingal)
	}
}
