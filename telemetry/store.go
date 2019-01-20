package telemetry

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/prometheus"
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
	buf := bytes.NewBuffer(data)
	mfs, err := prometheus.DecodeJSON(buf)
	if err != nil {
		s.Logger.Error("error decoding metrics", zap.Error(err))
		return err
	}
	b, err := json.Marshal(mfs)
	if err != nil {
		s.Logger.Error("error marshaling metrics", zap.Error(err))
		return err
	}
	s.Logger.Info("write", zap.String("data", string(b)))
	return nil
}
