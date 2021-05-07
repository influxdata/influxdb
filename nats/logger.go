package nats

import (
	"fmt"

	natsserver "github.com/nats-io/gnatsd/server"
	"go.uber.org/zap"
)

var _ natsserver.Logger = (*zapLoggerAdapter)(nil)

// zapLogger
type zapLoggerAdapter struct {
	log *zap.Logger
}

func (z *zapLoggerAdapter) Noticef(format string, v ...interface{}) {
	z.log.Debug(fmt.Sprintf(format, v...), zap.String("nats_level", "notice"))
}

func (z *zapLoggerAdapter) Debugf(format string, v ...interface{}) {
	z.log.Debug(fmt.Sprintf(format, v...), zap.String("nats_level", "debug"))
}

func (z *zapLoggerAdapter) Tracef(format string, v ...interface{}) {
	z.log.Debug(fmt.Sprintf(format, v...), zap.String("nats_level", "trace"))
}

func (z *zapLoggerAdapter) Fatalf(format string, v ...interface{}) {
	z.log.Fatal(fmt.Sprintf(format, v...), zap.String("nats_level", "fatal"))
}

func (z *zapLoggerAdapter) Errorf(format string, v ...interface{}) {
	z.log.Error(fmt.Sprintf(format, v...), zap.String("nats_level", "error"))
}
