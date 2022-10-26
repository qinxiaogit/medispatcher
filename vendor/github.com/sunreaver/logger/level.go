package logger

import (
	"strings"

	"go.uber.org/zap/zapcore"
)

// Level logger level.
type Level int8

const (
	// DebugLevel 输出debug、info、warn、error级别.
	// 开发中用.
	DebugLevel Level = iota - 1
	// InfoLevel 输出info、warn、error级别.
	InfoLevel
	// WarnLevel 输出warn、error级别.
	WarnLevel
	// ErrorLevel 输出error级别.
	ErrorLevel
)

func (l Level) toZapcoreLevel() zapcore.LevelEnabler {
	if l < DebugLevel || l > ErrorLevel {
		return zapcore.DebugLevel
	}

	return zapcore.Level(l)
}

func (l Level) toLevelString() LevelString {
	switch l {
	case DebugLevel:
		return DebugStringLevel
	case InfoLevel:
		return InfoStringLevel
	case WarnLevel:
		return WarnStringLevel
	case ErrorLevel:
		return ErrorStringLevel
	}

	return ErrorStringLevel
}

// LevelString 字符串格式的Level.
type LevelString string

const (
	// DebugStringLevel debug.
	DebugStringLevel LevelString = "debug"
	// InfoStringLevel info.
	InfoStringLevel LevelString = "info"
	// WarnStringLevel warn.
	WarnStringLevel LevelString = "warn"
	// ErrorStringLevel error.
	ErrorStringLevel LevelString = "error"
)

func (l LevelString) toLevel() Level {
	switch LevelString(strings.ToLower(string(l))) {
	case DebugStringLevel:
		return DebugLevel
	case InfoStringLevel:
		return InfoLevel
	case WarnStringLevel:
		return WarnLevel
	case ErrorStringLevel:
		return ErrorLevel
	}

	return DebugLevel
}

func (l LevelString) toZapcoreLevel() zapcore.LevelEnabler {
	return l.toLevel().toZapcoreLevel()
}
