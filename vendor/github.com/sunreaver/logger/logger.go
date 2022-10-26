package logger

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Logger Logger.
type Logger interface {
	Debugw(msg string, kv ...interface{})
	Infow(msg string, kv ...interface{})
	Warnw(msg string, kv ...interface{})
	Errorw(msg string, kv ...interface{})
	Panicw(msg string, kv ...interface{})
}

// Empty empty logger.
var (
	Empty        = &emptyLogger{}
	goroutineMap *sync.Map
)

type emptyLogger struct{}

// Debugw Debugw.
func (e *emptyLogger) Debugw(_ string, _ ...interface{}) {
}

// Infow Infow.
func (e *emptyLogger) Infow(_ string, _ ...interface{}) {
}

// Warnw Warnw.
func (e *emptyLogger) Warnw(_ string, _ ...interface{}) {
}

// Errorw Errorw.
func (e *emptyLogger) Errorw(_ string, _ ...interface{}) {
}

// Panicw Panicw.
func (e *emptyLogger) Panicw(msg string, _ ...interface{}) {
	panic(msg)
}

// Config logger config.
type Config struct {
	Path     string
	Loglevel LevelString
	// MaxSize 单文件最大存储，单位MB
	MaxSize int
}

// InitLoggerWithConfig 使用config初始化logger.
func InitLoggerWithConfig(cfg Config, location *time.Location, gid *sync.Map) error {
	if len(cfg.Path) == 0 {
		return errors.New("path empty")
	}
	if e := exists(cfg.Path); e != nil {
		return e
	} else if cfg.MaxSize <= 0 {
		return errors.New("MaxSize must be large than zero")
	}
	config = cfg
	goroutineMap = gid

	// Fix time offset for Local
	// lt := time.FixedZone("Asia/Shanghai", 8*60*60)
	if location != nil {
		time.Local = location
	}

	lastFile := time.Now().Format(loggerByDayFormat)
	LoggerByDay = GetSugarLogger(lastFile)
	go func() {
		for {
			now := time.Now()
			if lastFile != now.Format(loggerByDayFormat) {
				go func(name string) {
					if e := loggers.Close(name); e != nil {
						fmt.Println("writer.Close error", e.Error(), "File", name)
					}
				}(lastFile)

				lastFile = now.Format(loggerByDayFormat)
				LoggerByDay = GetSugarLogger(lastFile)
			}
			time.Sleep(ToEarlyMorningTimeDuration(now))
		}
	}()

	return nil
}

// InitLoggerWithLevel 使用String格式的level初始化logger.
// path 输出路径, 默认当前路径.
// logLevel 日志级别: debug,info,warn.
// location 日志文件名所属时区.
func InitLoggerWithLevel(path string, logLevel LevelString, location *time.Location, gid *sync.Map) error {
	return InitLogger(path, logLevel.toLevel(), location, gid)
}

// InitLogger 初始化.
// path 输出路径, 默认当前路径.
// logLevel 日志级别.
// location 日志文件名所属时区.
func InitLogger(path string, logLevel Level, location *time.Location, gid *sync.Map) error {
	return InitLoggerWithConfig(Config{
		Path:     path,
		Loglevel: logLevel.toLevelString(),
		MaxSize:  1024,
	}, location, gid)
}

// GetLogger to get logger.
func GetLogger(name string) *zap.Logger {
	return loggers.Get(name)
}

// GetSugarLogger to get SugaredLogger.
func GetSugarLogger(name string) Logger {
	return &GIDContext{l: GetLogger(name).Sugar()}
}

// FlushAndCloseLogger flush and close logger.
func FlushAndCloseLogger(name string) error {
	return loggers.Close(name)
}

func exists(path string) error {
	stat, err := os.Stat(path)
	if err == nil {
		return nil
	} else if os.IsNotExist(err) {
		return errors.New("path is not exists: " + path)
	} else if stat != nil && !stat.IsDir() {
		return errors.New("path is not directory: " + path)
	} else if stat == nil {
		return errors.New("not directory: " + path)
	}

	return err
}
