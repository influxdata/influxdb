package tcp_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/data"
	"github.com/influxdb/influxdb/tcp"
)

type testServer string
type serverResponses []serverResponse
type serverResponse struct {
	shardID uint64
	points  []data.Point
}

func (testServer) WriteShard(shardID uint64, points []data.Point) (int, error) {
	responses <- &serverResponse{
		shardID: shardID,
		points:  points,
	}
	return 0, nil
}

func (testServer) ResponseN(n int) ([]*serverResponse, error) {
	var a []*serverResponse
	for {
		select {
		case r := <-responses:
			a = append(a, r)
			if len(a) == n {
				return a, nil
			}
		case <-time.After(time.Second):
			return a, fmt.Errorf("unexpected response count: expected: %d, actual: %d", n, len(a))
		}
	}
}

var responses = make(chan *serverResponse, 1024)

func TestServer_Close_ErrServerClosed(t *testing.T) {
	var (
		ts testServer
		s  = tcp.NewServer(ts)
	)

	_, e := s.ListenAndServe("127.0.0.1:0")
	if e != nil {
		t.Fatalf("err does not match.  expected %v, got %v", nil, e)
	}
	// Close the server
	s.Close()

	// Try to close it again
	if err := s.Close(); err != tcp.ErrServerClosed {
		t.Fatalf("expected an error, got %v", err)
	}
}

func TestServer_WriteShardRequestSuccess(t *testing.T) {
	var (
		ts testServer
		s  = tcp.NewServer(ts)
	)

	host, e := s.ListenAndServe("127.0.0.1:0")
	if e != nil {
		t.Fatalf("err does not match.  expected %v, got %v", nil, e)
	}

	client := tcp.NewClient()
	err := client.Dial(host)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()

	shardID := uint64(1)
	var points []data.Point
	points = append(points, data.Point{
		Name:   "cpu",
		Time:   now,
		Tags:   data.Tags{"host": "server01"},
		Fields: map[string]interface{}{"value": int64(100)},
	})

	if err := client.WriteShardRequest(shardID, points); err != nil {
		t.Fatal(err)
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	// Close the server
	s.Close()

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

	if got.Name != exp.Name {
		t.Fatal("unexpected name")
	}

	if got.Fields["value"] != exp.Fields["value"] {
		t.Fatal("unexpected fields")
	}

	if got.Tags["host"] != exp.Tags["host"] {
		t.Fatal("unexpected tags")
	}

	if got.Time.UnixNano() != exp.Time.UnixNano() {
		t.Fatal("unexpected time")
	}
}
