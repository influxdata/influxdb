package gather

import (
	"encoding/json"

	"github.com/influxdata/influxdb/tsdb"

	"github.com/influxdata/influxdb/nats"
	"github.com/influxdata/influxdb/storage"
	"go.uber.org/zap"
)

// PointWriter will use the storage.PointWriter interface to record metrics.
type PointWriter struct {
	Writer storage.PointsWriter
}

// Record the metrics and write using storage.PointWriter interface.
func (s PointWriter) Record(collected MetricsCollection) error {
	ps, err := collected.MetricsSlice.Points()
	if err != nil {
		return err
	}
	ps, err = tsdb.ExplodePoints(collected.OrgID, collected.BucketID, ps)
	if err != nil {
		return err
	}
	return s.Writer.WritePoints(ps)
}

// Recorder record the metrics of a time based.
type Recorder interface {
	//Subscriber nats.Subscriber
	Record(collected MetricsCollection) error
}

// RecorderHandler implements nats.Handler interface.
type RecorderHandler struct {
	Recorder Recorder
	Logger   *zap.Logger
}

// Process consumes job queue, and use recorder to record.
func (h *RecorderHandler) Process(s nats.Subscription, m nats.Message) {
	defer m.Ack()
	collected := new(MetricsCollection)
	err := json.Unmarshal(m.Data(), &collected)
	if err != nil {
		h.Logger.Error("recorder handler error", zap.Error(err))
		return
	}
	err = h.Recorder.Record(*collected)
	if err != nil {
		h.Logger.Error("recorder handler error", zap.Error(err))
	}
}
