package tsi1_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/tsi1"
)

func TestSQLIndexExporter_ExportIndex(t *testing.T) {
	idx := MustOpenIndex(1, tsi1.NewConfig())
	defer idx.Close()

	// Add series to index.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: tsdb.EncodeNameSlice(1, 2), Tags: models.NewTags(map[string]string{"region": "east", "status": "ok"})},
		{Name: tsdb.EncodeNameSlice(1, 2), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: tsdb.EncodeNameSlice(3, 4), Tags: models.NewTags(map[string]string{"region": "east"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Expected output.
	want := `
BEGIN TRANSACTION;
INSERT INTO measurement_series (org_id, bucket_id, series_id) VALUES (1, 2, 1);
INSERT INTO measurement_series (org_id, bucket_id, series_id) VALUES (1, 2, 5);
INSERT INTO tag_value_series (org_id, bucket_id, key, value, series_id) VALUES (1, 2, 'region', 'east', 1);
INSERT INTO tag_value_series (org_id, bucket_id, key, value, series_id) VALUES (1, 2, 'region', 'west', 5);
INSERT INTO tag_value_series (org_id, bucket_id, key, value, series_id) VALUES (1, 2, 'status', 'ok', 1);
INSERT INTO measurement_series (org_id, bucket_id, series_id) VALUES (3, 4, 2);
INSERT INTO tag_value_series (org_id, bucket_id, key, value, series_id) VALUES (3, 4, 'region', 'east', 2);
COMMIT;
`[1:]

	// Export file to SQL.
	var buf bytes.Buffer
	e := tsi1.NewSQLIndexExporter(&buf)
	e.ShowSchema = false
	e.Logger = logger.New(os.Stderr)
	if err := e.ExportIndex(idx.Index); err != nil {
		t.Fatal(err)
	} else if err := e.Close(); err != nil {
		t.Fatal(err)
	} else if got := buf.String(); got != want {
		t.Fatalf("unexpected output:\ngot=%s\n--\nwant=%s", got, want)
	}
}
