package legacy

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2/http/metric"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
)

func newWriteUsageRecorder(w *kithttp.StatusResponseWriter, recorder metric.EventRecorder) *writeUsageRecorder {
	return &writeUsageRecorder{
		Writer:        w,
		EventRecorder: recorder,
	}
}

type writeUsageRecorder struct {
	Writer        *kithttp.StatusResponseWriter
	EventRecorder metric.EventRecorder
}

func (w *writeUsageRecorder) Record(ctx context.Context, requestBytes int, orgID platform.ID, endpoint string) {
	w.EventRecorder.Record(ctx, metric.Event{
		OrgID:         orgID,
		Endpoint:      endpoint,
		RequestBytes:  requestBytes,
		ResponseBytes: w.Writer.ResponseBytes(),
		Status:        w.Writer.Code(),
	})
}
