package telemetry

import (
	"context"

	"go.uber.org/zap"
)

// Store records usage data.
type Store interface {
	// WriteMessage stores data into the store.
	WriteMessage(ctx context.Context, data []byte) error
}

var _ Store = (*LogStore)(nil)

// LogStore logs data written to the store.
type LogStore struct {
	Logger *zap.Logger
}

// WriteMessage logs data at Info level.
func (s *LogStore) WriteMessage(ctx context.Context, data []byte) error {
	s.Logger.Info("write", zap.String("data", string(data)))
	return nil
}
