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
	log *zap.Logger
}

func NewLogStore(log *zap.Logger) *LogStore {
	return &LogStore{
		log: log,
	}
}

// WriteMessage logs data at Info level.
func (s *LogStore) WriteMessage(ctx context.Context, data []byte) error {
	s.log.Info("Write", zap.String("data", string(data)))
	return nil
}
