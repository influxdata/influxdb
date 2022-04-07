package tsi1_test

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"go.uber.org/zap/zaptest"
)

func TestSQLIndexExporter_ExportIndex(t *testing.T) {
	idx := MustOpenIndex(t, 1)
	defer idx.Close()

	// Add series to index.
	if err := idx.CreateSeriesSliceIfNotExists([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east", "status": "ok"})},
		{Name: []byte("disk"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("memory"), Tags: models.NewTags(map[string]string{"region": "east"})},
	}); err != nil {
		t.Fatal(err)
	}

	// Expected output.
	want := `
BEGIN TRANSACTION;
INSERT INTO measurement_series (name, series_id) VALUES ('cpu', 3);
INSERT INTO tag_value_series (name, key, value, series_id) VALUES ('cpu', 'region', 'east', 3);
INSERT INTO tag_value_series (name, key, value, series_id) VALUES ('cpu', 'status', 'ok', 3);
INSERT INTO measurement_series (name, series_id) VALUES ('disk', 7);
INSERT INTO tag_value_series (name, key, value, series_id) VALUES ('disk', 'region', 'west', 7);
INSERT INTO measurement_series (name, series_id) VALUES ('memory', 8);
INSERT INTO tag_value_series (name, key, value, series_id) VALUES ('memory', 'region', 'east', 8);
COMMIT;
`[1:]

	// Export file to SQL.
	var buf bytes.Buffer
	e := tsi1.NewSQLIndexExporter(&buf)
	e.ShowSchema = false
	e.Logger = zaptest.NewLogger(t)
	if err := e.ExportIndex(idx.Index); err != nil {
		t.Fatal(err)
	} else if err := e.Close(); err != nil {
		t.Fatal(err)
	} else if got := buf.String(); got != want {
		t.Fatalf("unexpected output:\ngot=%s\n--\nwant=%s", got, want)
	}
}
