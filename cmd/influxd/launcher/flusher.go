package launcher

import (
	"context"

	"github.com/influxdata/influxdb/v2/http"
)

type flushers []http.Flusher

func (f flushers) Flush(ctx context.Context) {
	for _, flusher := range []http.Flusher(f) {
		flusher.Flush(ctx)
	}
}
