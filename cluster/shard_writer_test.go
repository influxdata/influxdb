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

// Ensure the shard writer can successful write a single request.
func TestShardWriter_WriteShard_Success(t *testing.T) {
	ts := newTestServer(writeShardSuccess)
	s := cluster.NewServer(ts, "127.0.0.1:0")
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	w := cluster.NewShardWriter(time.Minute)
	w.MetaStore = &metaStore{host: s.Addr().String()}

	// Build a single point.
	now := time.Now()
	shardID, ownerID := uint64(1), uint64(2)
	var points []tsdb.Point
	points = append(points, tsdb.NewPoint("cpu", tsdb.Tags{"host": "server01"}, map[string]interface{}{"value": int64(100)}, now))

	// Write to shard and close.
	if err := w.WriteShard(shardID, ownerID, points); err != nil {
		t.Fatal(err)
	} else if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Validate response.
	responses, err := ts.ResponseN(1)
	if err != nil {
		t.Fatal(err)
	} else if responses[0].shardID != 1 {
		t.Fatalf("unexpected shard id: %d", responses[0].shardID)
	}

	// Validate point.
	if p := responses[0].points[0]; p.Name() != "cpu" {
		t.Fatalf("unexpected name: %s", p.Name())
	} else if p.Fields()["value"] != int64(100) {
		t.Fatalf("unexpected 'value' field: %d", p.Fields()["value"])
	} else if p.Tags()["host"] != "server01" {
		t.Fatalf("unexpected 'host' tag: %s", p.Tags()["host"])
	} else if p.Time().UnixNano() != now.UnixNano() {
		t.Fatalf("unexpected time: %s", p.Time())
	}
}

// Ensure the shard writer can successful write multiple requests.
func TestShardWriter_WriteShard_MultipleSuccess(t *testing.T) {
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

	w := cluster.NewShardWriter(time.Minute)
	w.MetaStore = &metaStore{host: s.Addr().String()}

	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var points []tsdb.Point
	points = append(points, tsdb.NewPoint(
		"cpu", tsdb.Tags{"host": "server01"}, map[string]interface{}{"value": int64(100)}, now,
	))

	if err := w.WriteShard(shardID, ownerID, points); err != nil {
		t.Fatal(err)
	}

	now = time.Now()

	points = append(points, tsdb.NewPoint(
		"cpu", tsdb.Tags{"host": "server01"}, map[string]interface{}{"value": int64(100)}, now,
	))

	if err := w.WriteShard(shardID, ownerID, points[1:]); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
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

// Ensure the shard writer returns an error when the server fails to accept the write.
func TestShardWriter_WriteShard_Error(t *testing.T) {
	ts := newTestServer(writeShardFail)
	s := cluster.NewServer(ts, "127.0.0.1:0")
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	w := cluster.NewShardWriter(time.Minute)
	w.MetaStore = &metaStore{host: s.Addr().String()}
	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var points []tsdb.Point
	points = append(points, tsdb.NewPoint(
		"cpu", tsdb.Tags{"host": "server01"}, map[string]interface{}{"value": int64(100)}, now,
	))

	if err, exp := w.WriteShard(shardID, ownerID, points), "error code 1: failed to write"; err == nil || err.Error() != exp {
		t.Fatalf("expected error %s, got %v", exp, err)
	}
}

// Ensure the shard writer returns an error when dialing times out.
func TestShardWriter_Write_ErrDialTimeout(t *testing.T) {
	ts := newTestServer(writeShardSuccess)
	s := cluster.NewServer(ts, "127.0.0.1:0")
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	w := cluster.NewShardWriter(time.Nanosecond)
	w.MetaStore = &metaStore{host: s.Addr().String()}
	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var points []tsdb.Point
	points = append(points, tsdb.NewPoint(
		"cpu", tsdb.Tags{"host": "server01"}, map[string]interface{}{"value": int64(100)}, now,
	))

	if err, exp := w.WriteShard(shardID, ownerID, points), "i/o timeout"; err == nil || !strings.Contains(err.Error(), exp) {
		t.Fatalf("expected error %v, to contain %s", err, exp)
	}
}

// Ensure the shard writer returns an error when reading times out.
func TestShardWriter_Write_ErrReadTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	w := cluster.NewShardWriter(time.Millisecond)
	w.MetaStore = &metaStore{host: ln.Addr().String()}
	now := time.Now()

	shardID := uint64(1)
	ownerID := uint64(2)
	var points []tsdb.Point
	points = append(points, tsdb.NewPoint(
		"cpu", tsdb.Tags{"host": "server01"}, map[string]interface{}{"value": int64(100)}, now,
	))

	err = w.WriteShard(shardID, ownerID, points)
	if err == nil {
		t.Fatal("expected read io timeout error")
	}
	if exp := fmt.Sprintf("read tcp %s: i/o timeout", ln.Addr().String()); exp != err.Error() {
		t.Fatalf("expected error %s, got %v", exp, err)
	}
}
