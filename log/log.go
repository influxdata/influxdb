package log

import (
	"os"

	"github.com/Sirupsen/logrus"
)

type Logger interface {
	Info(...interface{})
	Warn(...interface{})
	Debug(...interface{})
	Panic(...interface{})
	Error(...interface{})

	WithField(string, interface{}) *logrus.Entry
}

func New() Logger {
	return &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.TextFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}
}
