package cluster_test

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/tsdb"
)

func Test_WriteShardRequestSuccess(t *testing.T) {
	var (
		ts = newTestServer(writeShardSuccess)
		s  = cluster.NewServer(ts, "127.0.0.1:0")
	)
	e := s.Open()
	if e != nil {
		t.Fatalf("err does not match.  expected %v, got %v", nil, e)
	}
	// Close the server
	defer s.Close()

	writer := cluster.NewWriter(&metaStore{host: s.Addr().String()}, time.Minute)

	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var points []tsdb.Point
	points = append(points, tsdb.NewPoint(
		"cpu", tsdb.Tags{"host": "server01"}, map[string]interface{}{"value": int64(100)}, now,
	))

	if err := writer.Write(shardID, ownerID, points); err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	responses, err := ts.ResponseN(1)
	if err != nil {
		t.Fatal(err)
	}

	response := responses[0]

	if shardID != response.shardID {
		t.Fatalf("unexpected shardID.  exp: %d, got %d", shardID, response.shardID)
	}

	got := response.points[0]
	exp := points[0]
	t.Log("got: ", spew.Sdump(got))
	t.Log("exp: ", spew.Sdump(exp))

	if got.Name() != exp.Name() {
		t.Fatal("unexpected name")
	}

	if got.Fields()["value"] != exp.Fields()["value"] {
		t.Fatal("unexpected fields")
	}

	if got.Tags()["host"] != exp.Tags()["host"] {
		t.Fatal("unexpected tags")
	}

	if got.Time().UnixNano() != exp.Time().UnixNano() {
		t.Fatal("unexpected time")
	}
}

func Test_WriteShardRequestMultipleSuccess(t *testing.T) {
	var (
		ts = newTestServer(writeShardSuccess)
		s  = cluster.NewServer(ts, "127.0.0.1:0")
	)
	// Start on a random port
	if e := s.Open(); e != nil {
		t.Fatalf("err does not match.  expected %v, got %v", nil, e)
	}
	// Close the server
	defer s.Close()

	writer := cluster.NewWriter(&metaStore{host: s.Addr().String()}, time.Minute)

	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var points []tsdb.Point
	points = append(points, tsdb.NewPoint(
		"cpu", tsdb.Tags{"host": "server01"}, map[string]interface{}{"value": int64(100)}, now,
	))

	if err := writer.Write(shardID, ownerID, points); err != nil {
		t.Fatal(err)
	}

	now = time.Now()

	points = append(points, tsdb.NewPoint(
		"cpu", tsdb.Tags{"host": "server01"}, map[string]interface{}{"value": int64(100)}, now,
	))

	if err := writer.Write(shardID, ownerID, points[1:]); err != nil {
		t.Fatal(err)
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	responses, err := ts.ResponseN(1)
	if err != nil {
		t.Fatal(err)
	}

	response := responses[0]

	if shardID != response.shardID {
		t.Fatalf("unexpected shardID.  exp: %d, got %d", shardID, response.shardID)
	}

	got := response.points[0]
	exp := points[0]
	t.Log("got: ", spew.Sdump(got))
	t.Log("exp: ", spew.Sdump(exp))

	if got.Name() != exp.Name() {
		t.Fatal("unexpected name")
	}

	if got.Fields()["value"] != exp.Fields()["value"] {
		t.Fatal("unexpected fields")
	}

	if got.Tags()["host"] != exp.Tags()["host"] {
		t.Fatal("unexpected tags")
	}

	if got.Time().UnixNano() != exp.Time().UnixNano() {
		t.Fatal("unexpected time")
	}
}
func Test_WriteShardRequestFail(t *testing.T) {
	var (
		ts = newTestServer(writeShardFail)
		s  = cluster.NewServer(ts, "127.0.0.1:0")
	)
	// Start on a random port
	if e := s.Open(); e != nil {
		t.Fatalf("err does not match.  expected %v, got %v", nil, e)
	}
	// Close the server
	defer s.Close()

	writer := cluster.NewWriter(&metaStore{host: s.Addr().String()}, time.Minute)
	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var points []tsdb.Point
	points = append(points, tsdb.NewPoint(
		"cpu", tsdb.Tags{"host": "server01"}, map[string]interface{}{"value": int64(100)}, now,
	))

	if err, exp := writer.Write(shardID, ownerID, points), "error code 1: failed to write"; err == nil || err.Error() != exp {
		t.Fatalf("expected error %s, got %v", exp, err)
	}
}

func Test_WriterDialTimeout(t *testing.T) {
	var (
		ts = newTestServer(writeShardSuccess)
		s  = cluster.NewServer(ts, "127.0.0.1:0")
	)
	// Start on a random port
	if e := s.Open(); e != nil {
		t.Fatalf("err does not match.  expected %v, got %v", nil, e)
	}
	// Close the server
	defer s.Close()

	writer := cluster.NewWriter(&metaStore{host: s.Addr().String()}, time.Nanosecond)
	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var points []tsdb.Point
	points = append(points, tsdb.NewPoint(
		"cpu", tsdb.Tags{"host": "server01"}, map[string]interface{}{"value": int64(100)}, now,
	))

	if err, exp := writer.Write(shardID, ownerID, points), "i/o timeout"; err == nil || !strings.Contains(err.Error(), exp) {
		t.Fatalf("expected error %v, to contain %s", err, exp)
	}
}

func Test_WriterReadTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	writer := cluster.NewWriter(&metaStore{host: ln.Addr().String()}, time.Millisecond)
	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var points []tsdb.Point
	points = append(points, tsdb.NewPoint(
		"cpu", tsdb.Tags{"host": "server01"}, map[string]interface{}{"value": int64(100)}, now,
	))

	err = writer.Write(shardID, ownerID, points)
	if err == nil {
		t.Fatal("expected read io timeout error")
	}
	if exp := fmt.Sprintf("read tcp %s: i/o timeout", ln.Addr().String()); exp != err.Error() {
		t.Fatalf("expected error %s, got %v", exp, err)
	}
}
