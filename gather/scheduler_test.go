package gather

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/models"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestScheduler(t *testing.T) {
	totalGatherJobs := 20

	// Create top level logger
	logger := zaptest.NewLogger(t)
	ts := httptest.NewServer(&mockHTTPHandler{
		responseMap: map[string]string{
			"/metrics": sampleRespSmall,
		},
	})
	defer ts.Close()

	storage := &mockStorage{
		Metrics: make(map[time.Time]Metrics),
		Targets: []influxdb.ScraperTarget{
			{
				ID:       influxdbtesting.MustIDBase16("3a0d0a6365646120"),
				Type:     influxdb.PrometheusScraperType,
				URL:      ts.URL + "/metrics",
				OrgID:    *orgID,
				BucketID: *bucketID,
			},
		},
	}

	gatherJobs := make(chan []models.Point)
	done := make(chan struct{})
	writer := &mock.PointsWriter{}
	writer.WritePointsFn = func(ctx context.Context, orgID platform.ID, bucketID platform.ID, points []models.Point) error {
		select {
		case gatherJobs <- points:
		case <-done:
		}
		return nil
	}

	scheduler, err := NewScheduler(logger, 10, 2, storage, writer, 1*time.Millisecond)
	require.NoError(t, err)
	defer scheduler.Close()
	defer close(done) //don't block the points writer forever

	// make sure all jobs are done
	pointWrites := [][]models.Point{}
	for i := 0; i < totalGatherJobs; i++ {
		newWrite := <-gatherJobs
		pointWrites = append(pointWrites, newWrite)
		assert.Equal(t, 1, len(newWrite))
		newWrite[0].SetTime(time.Unix(0, 0)) // zero out the time so we don't have to compare it
		assert.Equal(t, "go_goroutines gauge=36 0", newWrite[0].String())
	}

	if len(pointWrites) < totalGatherJobs {
		t.Fatalf("metrics stored less than expected, got len %d", len(storage.Metrics))
	}
}

const sampleRespSmall = `
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 36
`
