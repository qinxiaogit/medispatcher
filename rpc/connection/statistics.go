package connection

import (
	"bytes"
	"compress/gzip"
	"medispatcher/logger"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"sort"
	"strconv"
	"time"
)

type ConnAction int8

type ConnActionStatus int8

func (_ ConnAction) String() {

}

const (
	// 内存保存的最大统计数据天数
	MAX_IN_MEM_STATS_LENGTH  = 30
	GC_IN_MEM_STATS_INTERVAL = time.Minute * 60
	CONNACTION_ACCEPT        = ConnAction(1)
	CONNACTION_RESPONSE      = ConnAction(2)
	CONNACTION_PROCESS_FIN   = ConnAction(3)

	CONNACTION_STATUS_OK     = ConnActionStatus(1)
	CONNACTION_STATUS_FAILED = ConnActionStatus(1)
)

type StatsMessage struct {
	cmd    string
	action ConnAction
	status ConnActionStatus
}

type Stats struct {
	TotalReq    int
	FinishedReq int
	FailedReq   int
}

var statsSegs = map[int64]*map[string]*Stats{}
var currentSegIndex int64

var statsInChanRWLock = make(chan int, 1)
var statsInChan = make(chan StatsMessage, 100)

func getStatsRWLock() {
	statsInChanRWLock <- 1
}

func releaseStatsRWLock() {
	<-statsInChanRWLock
}

// 按分钟分段记录请求统计数据
func receiveStats() {
	calIndex := func() int64 {
		index := time.Now().Unix()
		index = index - index%60
		return index
	}

	go gcStats()

	currentSegIndex = calIndex()

	go func(cal func() int64) {
		for {
			time.Sleep(time.Second * 2)
			currentSegIndex = cal()
		}
	}(calIndex)

	for {
		cStats := <-statsInChan
		if cStats.action != CONNACTION_PROCESS_FIN {
			continue
		}
		newStatsSeg, exists := statsSegs[currentSegIndex]
		if !exists {
			newStatsSeg = &map[string]*Stats{}
		}
		newStats, exists := (*newStatsSeg)[cStats.cmd]
		if !exists {
			newStats = &Stats{0, 0, 0}
		}
		(*newStats).TotalReq += 1
		if cStats.status == CONNACTION_STATUS_OK {
			(*newStats).FinishedReq += 1
		} else {
			(*newStats).FailedReq += 1
		}
		(*newStatsSeg)[cStats.cmd] = newStats
		statsSegs[currentSegIndex] = newStatsSeg
	}
}

func gcStats() {
	for {
		time.Sleep(GC_IN_MEM_STATS_INTERVAL)
		statsSegLength := len(statsSegs)
		delRangeLength := statsSegLength - MAX_IN_MEM_STATS_LENGTH*24*60
		if delRangeLength < 1 {
			continue
		}
		for i := 1; delRangeLength-i >= 0; i++ {
			indexForDel := currentSegIndex - int64((MAX_IN_MEM_STATS_LENGTH*24+i)*60)
			delete(statsSegs, indexForDel)
		}
	}
}

// 启动控制台服务。可通过控制台进行各项调试。Telent 127.0.0.1 5606
func startConsole() {
	server, err := net.Listen("tcp", "0.0.0.0:5606")
	if err != nil {
		logger.GetLogger("ERROR").Printf("Failed to start console server: %s", err)
		return
	}

	defer func() {
		server.Close()
	}()

	for {
		conn, err := server.Accept()
		if err != nil {
			logger.GetLogger("ERROR").Printf("Console server failed to accept connection: %v", err)
		}
		go func() {
			cmd := make([]byte, 51)
			start := 0
			for {
				conn.SetReadDeadline(time.Now().Add(time.Second * 30))
				n, err := conn.Read(cmd[start:])
				if err != nil {
					conn.Close()
					break
				}
				start += n
				var closed bool
				for i := 0; i <= start; i++ {
					if cmd[i] == '\r' && cmd[i+1] == '\n' {
						cmd = bytes.TrimRight(cmd, "\r\n\000")
						switch string(cmd) {
						case "stats":
							sendStatsToConsole(&conn, false)
						case "statsz":
							sendStatsToConsole(&conn, true)
						case "quit", "Quit", "exit", "Exit", "close", "Close":
							conn.Write([]byte("Byte\n"))
							closed = true
							conn.Close()
						default:
							conn.Write([]byte(fmt.Sprintf("Invalid command: %s\nAvailabe commands are 'stats','statsz','quit','close'\n", cmd)))
						}

						start = 0
						cmd = make([]byte, 51)
						break
					}
				}
				if closed {
					break
				}
				if start >= 50 {
					start = 0
					cmd = make([]byte, 51)
				}
			}
		}()
	}
}

// 向控制台统计数据
//	compressed 发送压缩后的统计数据
func sendStatsToConsole(conn *net.Conn, compressed bool) {
	rf := reflect.ValueOf(statsSegs)
	keys := rf.MapKeys()
	keysInt := []int{}
	for _, v := range keys {
		keysInt = append(keysInt, int(v.Int()))
	}
	sort.Ints(keysInt)
	if !compressed {
		if len(keysInt) < 1 {
			(*conn).Write([]byte("no stats at the time.\r\n"))
			return
		}
		for _, indexInt := range keysInt {
			index := int64(indexInt)
			(*conn).Write([]byte(time.Unix(index, 0).Format(time.RFC3339) + "\n"))
			stats := statsSegs[index]
			re, err := json.MarshalIndent(*stats, "", "")
			if err != nil {
				(*conn).Write([]byte(fmt.Sprintf("Failed to format stats: %s\n", err)))
			} else {
				(*conn).Write(re)
				(*conn).Write([]byte("\n\n\n"))
			}
		}
	} else {
		defer (*conn).Close()
		zw, err := gzip.NewWriterLevel((*conn), gzip.BestCompression)
		if err != nil {
			(*conn).Write([]byte("failed"))
			return
		}
		defer zw.Close()
		_, err = zw.Write([]byte{'{'})
		if err != nil {
			return
		}
		kl := len(keysInt) - 1
		for i, indexInt := range keysInt {
			_, err := zw.Write([]byte("\"" + strconv.Itoa(indexInt) + "\": "))
			if err != nil {
				return
			}

			re, err := json.Marshal(*(statsSegs[int64(indexInt)]))
			if err != nil {
				return
			}
			if i < kl {
				_, err = zw.Write(append(re, ','))
			} else {
				_, err = zw.Write(re)
			}

			if err != nil {
				return
			}
		}
		zw.Write([]byte{'}'})
		return
	}
}
