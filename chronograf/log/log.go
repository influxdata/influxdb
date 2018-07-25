package log

import (
	"io"
	"os"

	"github.com/influxdata/platform/chronograf"
	"github.com/sirupsen/logrus"
)

// Level type
type Level uint8

// These are the different logging levels.
const (
	// PanicLevel level, highest level of severity. Logs and then calls panic with the
	// message passed to Debug, Info, ...
	PanicLevel Level = iota
	// FatalLevel level. Logs and then calls `os.Exit(1)`. It will exit even if the
	// logging level is set to Panic.
	FatalLevel
	// ErrorLevel level. Logs. Used for errors that should definitely be noted.
	// Commonly used for hooks to send errors to an error tracking service.
	ErrorLevel
	// WarnLevel level. Non-critical entries that deserve eyes.
	WarnLevel
	// InfoLevel level. General operational entries about what's going on inside the
	// application.
	InfoLevel
	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	DebugLevel
)

// ParseLevel takes a string level and returns the Logrus log level constant.
func ParseLevel(lvl string) Level {
	switch lvl {
	case "panic":
		return PanicLevel
	case "fatal":
		return FatalLevel
	case "error":
		return ErrorLevel
	case "warn":
		return WarnLevel
	case "info":
		return InfoLevel
	default:
		return DebugLevel
	}
}

// LogrusLogger is a chronograf.Logger that uses logrus to process logs
type logrusLogger struct {
	l *logrus.Entry
}

func (ll *logrusLogger) Debug(items ...interface{}) {
	ll.l.Debug(items...)
}

func (ll *logrusLogger) Info(items ...interface{}) {
	ll.l.Info(items...)
}

func (ll *logrusLogger) Warn(items ...interface{}) {
	ll.l.Warn(items...)
}

func (ll *logrusLogger) Error(items ...interface{}) {
	ll.l.Error(items...)
}

func (ll *logrusLogger) Fatal(items ...interface{}) {
	ll.l.Fatal(items...)
}

func (ll *logrusLogger) Panic(items ...interface{}) {
	ll.l.Panic(items...)
}

func (ll *logrusLogger) WithField(key string, value interface{}) chronograf.Logger {
	return &logrusLogger{ll.l.WithField(key, value)}
}

func (ll *logrusLogger) Writer() *io.PipeWriter {
	return ll.l.Logger.WriterLevel(logrus.ErrorLevel)
}

// New wraps a logrus Logger
func New(l Level) chronograf.Logger {
	logger := &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.TextFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.Level(l),
	}

	return &logrusLogger{
		l: logrus.NewEntry(logger),
	}
}
