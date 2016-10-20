package log

import (
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/influxdata/chronograf"
)

// LogrusLogger is a chronograf.Logger that uses logrus to process logs
type logrusLogger struct {
	l *logrus.Entry
}

func (ll *logrusLogger) Info(items ...interface{}) {
	ll.l.Info(items...)
}

func (ll *logrusLogger) Warn(items ...interface{}) {
	ll.l.Warn(items...)
}

func (ll *logrusLogger) Debug(items ...interface{}) {
	ll.l.Debug(items...)
}

func (ll *logrusLogger) Panic(items ...interface{}) {
	ll.l.Panic(items...)
}

func (ll *logrusLogger) Error(items ...interface{}) {
	ll.l.Error(items...)
}

func (ll *logrusLogger) WithField(key string, value interface{}) chronograf.Logger {
	return &logrusLogger{ll.l.WithField(key, value)}
}

func New() chronograf.Logger {
	logger := &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.TextFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}

	return &logrusLogger{
		l: logrus.NewEntry(logger),
	}
}
