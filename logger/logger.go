// Package logger wrappers core package "logger", provides extra features as co-routine safe, timedate based log file etc.
package logger

import (
	"fmt"
	"log"
	"medispatcher/config"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var bufferSize = 20000
var bufferForceFlushSize = 1000
var loggers = map[string]*CoSafeLogger{}
var logAsSingleFile bool

var loggersWRLock = make(chan int, 1)

var validLevels = []string{"INFO", "ERROR", "WARN", "DEFAULT", "DATA", "DEBUG"}

// DatetimeLogFileWriter log messages into datetime named files.
type DatetimeLogFileWriter struct {
	flushLockCheckChan chan bool
	logDir             string
	sync.Mutex
	fileNameSuffixFormat string //暂时无效
	level                string //INFO ERROR WARN DEFAULT DATA
	msgBuf               chan []byte
}

func Flush() {
	loggersWRLock <- 1
	for _, l := range loggers {
		l.writer.flush(0)
	}
	<-loggersWRLock
}

// 屏蔽输出
type NullWriter struct {
}

func (w *NullWriter) Write(p []byte) (n int, err error) {
	return 0, nil
}

type CoSafeLogger struct {
	sync.Mutex
	writer *DatetimeLogFileWriter
	rl     *log.Logger
	flag   int
}

func (cl *CoSafeLogger) getFormatHeader() string {
	if cl.flag != log.Lshortfile {
		return ""
	}
	_, file, lineno, ok := runtime.Caller(2)
	if !ok {
		file = "???"
		lineno = 0
	}
	return path.Base(file) + ":" + strconv.Itoa(lineno) + ":"
}

func (cl *CoSafeLogger) Printf(format string, v ...interface{}) {
	cl.Lock()
	defer cl.Unlock()
	cl.rl.Printf(cl.getFormatHeader()+format, v...)
}

func (cl *CoSafeLogger) Print(v ...interface{}) {
	cl.Lock()
	defer cl.Unlock()
	s := append([]interface{}{}, cl.getFormatHeader())
	s = append(s, v...)
	cl.rl.Print(s...)
}

func (cl *CoSafeLogger) Println(v ...interface{}) {
	cl.Lock()
	defer cl.Unlock()
	s := append([]interface{}{}, cl.getFormatHeader())
	s = append(s, v...)
	cl.rl.Println(s...)
}

func getLoggerByLevel(level string) *CoSafeLogger {
	loggersWRLock <- 1
	if l, exists := loggers[level]; exists {
		<-loggersWRLock
		return l
	}
	<-loggersWRLock
	return nil

}

func addToLoggers(level string, logger *CoSafeLogger) {
	loggersWRLock <- 1
	loggers[level] = logger
	<-loggersWRLock
}

// InitDefaultLogger 初始化默认logger, 可屏蔽其它组件的日志直接输出
func InitDefaultLogger() {
	//默认writer
	if config.DebugEnabled() {
		log.SetOutput(newLoggerWrite("DEFAULT"))
		log.SetFlags(log.LstdFlags)
	} else {
		log.SetOutput(&NullWriter{})
	}
}

// SetSingleFile 设置将各种类型的日志文件合并成一个
func SetSingleFile() {
	logAsSingleFile = true
}
func newLoggerWrite(level string) *DatetimeLogFileWriter {
	w := &DatetimeLogFileWriter{
		logDir:             config.GetConfig().LOG_DIR,
		level:              level,
		msgBuf:             make(chan []byte, bufferSize),
		flushLockCheckChan: make(chan bool, 1),
	}
	w.tickFlush()
	return w
}

func (w *DatetimeLogFileWriter) Write(p []byte) (n int, err error) {
	nb := make([]byte, len(p))
	copy(nb, p)
	w.msgBuf <- nb
	return len(p), nil
}

// tickFlush flush logs to output every 1 seconds or the buffered log count >= bufferForceFlushSize.
func (w *DatetimeLogFileWriter) tickFlush() {
	go func() {
		tickerCheck := time.NewTicker(time.Millisecond * 100)
		flushInterval := time.Microsecond * 1000
		tickerFlush := time.NewTimer(flushInterval)
		for {
			var err error
			<-tickerCheck.C
			bufLen := len(w.msgBuf)
			select {
			case <-tickerFlush.C:
				if bufLen > 0 && w.getFlushLock() {
					err = w.flush(bufLen)
					w.releaseFlushLock()
				}
				tickerFlush.Reset(flushInterval)
			default:
				if bufLen > bufferForceFlushSize && w.getFlushLock() {
					err = w.flush(bufLen)
					w.releaseFlushLock()
				}
			}
			if err != nil {
				log.Printf("Logger err: %v", err)
			}
		}
	}()
}
func (w *DatetimeLogFileWriter) getFlushLock() bool {
	timeout := time.After(time.Millisecond * 80)
	var r bool
	select {
	case <-timeout:
	case w.flushLockCheckChan <- true:
		r = true
	}
	return r
}

func (w *DatetimeLogFileWriter) releaseFlushLock() {
	<-w.flushLockCheckChan
}

// flush flush messsages to output. if "length" equals 0, then flush all messages, otherwise flush the "length" indicated count of messages.
func (w *DatetimeLogFileWriter) flush(length int) error {
	w.Lock()
	defer w.Unlock()
	suffix := time.Now().Format("2006-01-02")
	pathSep := string(os.PathSeparator)
	var logFilename string
	if logAsSingleFile {
		logFilename = fmt.Sprintf("%s%sall.%s.log", strings.TrimRight(w.logDir, pathSep), pathSep, suffix)
	} else {
		logFilename = fmt.Sprintf("%s%s%s.%s.log", strings.TrimRight(w.logDir, pathSep), pathSep, strings.ToLower(w.level), suffix)
	}
	fd, err := os.OpenFile(logFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("Failed to open log file: %s with error: %v", logFilename, err)
	}

	defer fd.Close()
	limit := length
	checkTimeout := time.NewTimer(time.Second * 2)
	for {
		select {
		case <-checkTimeout.C:
			return nil
		case d := <-w.msgBuf:
			_, err = fd.Write(d)
			if err != nil {
				return err
			}
			limit--
			if length != 0 {
				if limit <= 0 {
					return nil
				}
			}

			checkTimeout.Reset(time.Second * 2)
		}
	}
}

func CreateLogger(level string) *CoSafeLogger {
	writer := newLoggerWrite(level)
	return &CoSafeLogger{writer: writer, rl: log.New(writer, "", log.LstdFlags), flag: log.Lshortfile}
}

// GetLogger returns a logger according to level.
// Valid levels are "INFO", "ERROR", "WARN", "DEFAULT"
func GetLogger(level string) *CoSafeLogger {
	if !func(n string) bool {
		for _, dLevel := range validLevels {
			if level == dLevel {
				return true
			}
		}
		return false
	}(level) {
		panic(fmt.Sprintf("Invalid logger level(%s), please refer %v", level, validLevels))
	}
	var logger *CoSafeLogger
	if logger = getLoggerByLevel(level); logger == nil {
		logger = CreateLogger(level)
		addToLoggers(level, logger)
	}
	return logger
}
