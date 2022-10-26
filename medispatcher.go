package main

import (
	"encoding/json"
	"fmt"
	"github.com/qinxiaogit/medispatcher/balance"
	"github.com/qinxiaogit/medispatcher/config"
	"github.com/qinxiaogit/medispatcher/logger"
	"github.com/qinxiaogit/medispatcher/msgredist"
	"github.com/qinxiaogit/medispatcher/pushstatistics"
	"github.com/qinxiaogit/medispatcher/recoverwatcher"
	rpcSrv "github.com/qinxiaogit/medispatcher/rpc/connection"
	"github.com/qinxiaogit/medispatcher/sender"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	l "github.com/sunreaver/logger"
)

var exitSigChan = make(chan bool)

func main() {

	err := config.Setup()
	if err != nil {
		fmt.Printf("Failed to setup configs: %v", err)
		os.Exit(1)
	}

	l.InitLogger(config.GetConfig().LOG_DIR, l.InfoLevel, time.FixedZone("Asia/Shanghai", 8*3600))
	l.LoggerByDay.Debugw("Setup", "Config", config.GetConfig())

	// 将当前 推送服务实例 按策略加入到 特定的一组队列实例 的消费者列表中去.
	balance.NewBalance().Startup()

	// 启动prometheus统计功能
	pushstatistics.PrometheusStatisticsStart(config.GetConfig().PrometheusApiAddr)

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
	go watchConfigFile()
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
				// 将当前 推送服务实例 从特定的一组队列实例的消费者列表中移除, 以便其他新启动的推送服务实例能及时的加入到这些队列的消费者列表中去.
				balance.NewBalance().Over()

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

// watchConfigFile 监控配置文件所在的目录, 配置文件发生变更的时候, 给当前进程发送SIGQUIT信号
func watchConfigFile() {
	configFile := config.GetConfigPath()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	defer watcher.Close()

	if err := watcher.Add(path.Dir(configFile)); err != nil {
		panic(err)
	}

	for {
		select {
		case ev, ok := <-watcher.Events:
			if !ok {
				break
			}

			if path.Base(ev.Name) != path.Base(configFile) {
				continue
			}

			switch ev.Op {
			case fsnotify.Create, fsnotify.Write, fsnotify.Remove, fsnotify.Rename:
				if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
					logger.GetLogger("INFO").Printf("Configuration file changed(%s), kill -SIGINT\n", configFile)
				}
				return
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				break
			}

			logger.GetLogger("WARN").Printf("watcher error:%v\n", err)
		}
	}

	// TODO 文件监控存在异常的时候, 可以重新添加监控 或 忽略文件监控
}
