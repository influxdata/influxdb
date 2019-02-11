package logger

import (
	"context"

	"go.uber.org/zap"
)

type loggerContextKey struct{}

// NewContextWithLogger returns a new context with log added.
func NewContextWithLogger(ctx context.Context, log *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerContextKey{}, log)
}

// LoggerFromContext returns the zap.Logger associated with ctx or nil if no logger has been assigned.
func FromContext(ctx context.Context) *zap.Logger {
	l, _ := ctx.Value(loggerContextKey{}).(*zap.Logger)
	return l
}
