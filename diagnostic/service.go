package diagnostic

import (
	"io"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var nullLogger = zap.NewNop()

type Service struct {
	l *zap.Logger
}

func New(w io.Writer) *Service {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = func(ts time.Time, encoder zapcore.PrimitiveArrayEncoder) {
		encoder.AppendString(ts.UTC().Format(time.RFC3339))
	}
	logger := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(config),
		zapcore.AddSync(w),
		zapcore.DebugLevel,
	))
	return &Service{l: logger}
}

func (s *Service) Logger() *zap.Logger {
	if s == nil {
		return nullLogger
	}
	return s.l
}
