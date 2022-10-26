package logger

import (
	"bytes"
	"runtime"
	"strconv"

	"go.uber.org/zap"
)

type GIDContext struct {
	l *zap.SugaredLogger
}

func (g *GIDContext) reqid() *zap.SugaredLogger {
	if goroutineMap != nil {
		if reqid, ok := goroutineMap.Load(GetGID()); ok {
			return g.l.With("req_id", reqid)
		}
	}
	return g.l
}

func (g *GIDContext) Debugw(msg string, kv ...interface{}) {
	g.reqid().Debugw(msg, kv...)
}

func (g *GIDContext) Infow(msg string, kv ...interface{}) {
	g.reqid().Infow(msg, kv...)
}

func (g *GIDContext) Warnw(msg string, kv ...interface{}) {
	g.reqid().Warnw(msg, kv...)
}

func (g *GIDContext) Errorw(msg string, kv ...interface{}) {
	g.reqid().Errorw(msg, kv...)
}

func (g *GIDContext) Panicw(msg string, kv ...interface{}) {
	g.reqid().Panicw(msg, kv...)
}

func GetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
