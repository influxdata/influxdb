package tsm1

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb"
)

// Ensure an engine containing cached values responds correctly to queries.
func Test_DevEngineCacheQueryAscending(t *testing.T) {
	// Generate temporary file.
	f, _ := ioutil.TempFile("", "tsm1dev")
	f.Close()
	os.Remove(f.Name())
	walPath := filepath.Join(f.Name(), "wal")
	os.MkdirAll(walPath, 0777)
	defer os.RemoveAll(f.Name())

	// Create a few points.
	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=A value=1.2 2000000000")
	p3 := parsePoint("cpu,host=A value=1.3 3000000000")

	// Write those points to the engine.
	e := NewDevEngine(f.Name(), walPath, tsdb.NewEngineOptions())
	if err := e.Open(); err != nil {
		t.Fatalf("failed to open tsm1dev engine: %s", err.Error())
	}
	if err := e.WritePoints([]models.Point{p1, p2, p3}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// Start a query transactions and get a cursor.
	tx := devTx{engine: e.(*DevEngine)}
	ascCursor := tx.Cursor("cpu,host=A", []string{"value"}, nil, true)

	k, v := ascCursor.SeekTo(1)
	if k != 1000000000 {
		t.Fatalf("failed to seek to before first key: %v %v", k, v)
	}

	k, v = ascCursor.SeekTo(1000000000)
	if k != 1000000000 {
		t.Fatalf("failed to seek to first key: %v %v", k, v)
	}

	k, v = ascCursor.Next()
	if k != 2000000000 {
		t.Fatalf("failed to get next key: %v %v", k, v)
	}

	k, v = ascCursor.Next()
	if k != 3000000000 {
		t.Fatalf("failed to get next key: %v %v", k, v)
	}

	k, v = ascCursor.Next()
	if k != -1 {
		t.Fatalf("failed to get next key: %v %v", k, v)
	}

	k, v = ascCursor.SeekTo(4000000000)
	if k != -1 {
		t.Fatalf("failed to seek to past last key: %v %v", k, v)
	}
}

func parsePoints(buf string) []models.Point {
	points, err := models.ParsePointsString(buf)
	if err != nil {
		panic(fmt.Sprintf("couldn't parse points: %s", err.Error()))
	}
	return points
}

func parsePoint(buf string) models.Point {
	return parsePoints(buf)[0]
}
