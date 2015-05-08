package influxdb

import (
	"reflect"
	"testing"
	"time"
)

func TestShardWrite(t *testing.T) {
	// Enable when shard can convert a WritePointsRequest to stored data.
	// Needs filed encoding/types saved on the shard
	t.Skip("not implemented yet")

	sh := &Shard{ID: 1}

	pt := Point{
		Name:      "cpu",
		Tags:      map[string]string{"host": "server"},
		Timestamp: time.Unix(1, 2),
		Fields:    map[string]interface{}{"value": 1.0},
	}
	pr := &WritePointsRequest{
		Database:        "foo",
		RetentionPolicy: "default",
		Points: []Point{
			pt},
	}

	if err := sh.Write(pr); err != nil {
		t.Errorf("LocalWriter.Write() failed: %v", err)
	}

	p, err := sh.Read(pt.Timestamp)
	if err != nil {
		t.Fatalf("LocalWriter.Read() failed: %v", err)
	}

	if exp := 1; len(p) != exp {
		t.Fatalf("LocalWriter.Read() points len mismatch. got %v, exp %v", len(p), exp)
	}

	if !reflect.DeepEqual(p[0], pt) {
		t.Fatalf("LocalWriter.Read() point mismatch. got %v, exp %v", p[0], pt)
	}
}
