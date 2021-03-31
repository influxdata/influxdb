package http

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2/http/metric"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
)

func NewWriteUsageRecorder(w *kithttp.StatusResponseWriter, recorder metric.EventRecorder) *WriteUsageRecorder {
	return &WriteUsageRecorder{
		Writer:        w,
		EventRecorder: recorder,
	}
}

type WriteUsageRecorder struct {
	Writer        *kithttp.StatusResponseWriter
	EventRecorder metric.EventRecorder
}

func (w *WriteUsageRecorder) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w *WriteUsageRecorder) Record(ctx context.Context, requestBytes int, orgID platform.ID, endpoint string) {
	w.EventRecorder.Record(ctx, metric.Event{
		OrgID:         orgID,
		Endpoint:      endpoint,
		RequestBytes:  requestBytes,
		ResponseBytes: w.Writer.ResponseBytes(),
		Status:        w.Writer.Code(),
	})
}
