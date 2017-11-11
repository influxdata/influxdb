package logger

import (
	"io"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(w io.Writer) *zap.Logger {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = func(ts time.Time, encoder zapcore.PrimitiveArrayEncoder) {
		encoder.AppendString(ts.UTC().Format(time.RFC3339))
	}
	return zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(config),
		zapcore.AddSync(w),
		zapcore.DebugLevel,
	))
}
