package raft_test

import (
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/raft"
)

// Ensure a node can join a cluster over HTTP.
func TestHTTPHandler_HandleJoin(t *testing.T) {
	n := NewInitNode()
	defer n.Close()

	// Send request to join cluster.
	go func() { n.Clock().Add(n.Log.ApplyInterval) }()
	resp, err := http.Get(n.Server.URL + "/join?url=" + url.QueryEscape("http://localhost:1000"))
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d: %s", resp.StatusCode, resp.Header.Get("X-Raft-Error"))
	}
	if s := resp.Header.Get("X-Raft-Error"); s != "" {
		t.Fatalf("unexpected raft error: %s", s)
	}
	if s := resp.Header.Get("X-Raft-ID"); s != "2" {
		t.Fatalf("unexpected raft id: %s", s)
	}
}

// Ensure a heartbeat can be sent over HTTP.
func TestHTTPHandler_HandleHeartbeat(t *testing.T) {
	n := NewInitNode()
	defer n.Close()

	// Send heartbeat.
	resp, err := http.Get(n.Server.URL + "/heartbeat?term=1&commitIndex=0&leaderID=1")
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	if s := resp.Header.Get("X-Raft-Error"); s != "" {
		t.Fatalf("unexpected raft error: %s", s)
	}
	if s := resp.Header.Get("X-Raft-Index"); s != "1" {
		t.Fatalf("unexpected raft index: %s", s)
	}
	if s := resp.Header.Get("X-Raft-Term"); s != "1" {
		t.Fatalf("unexpected raft term: %s", s)
	}
}

// Ensure that sending a heartbeat with an invalid term returns an error.
func TestHTTPHandler_HandleHeartbeat_Error(t *testing.T) {
	var tests = []struct {
		query string
		err   string
	}{
		{query: `term=XXX&commitIndex=0&leaderID=1`, err: `invalid term`},
		{query: `term=1&commitIndex=XXX&leaderID=1`, err: `invalid commit index`},
		{query: `term=1&commitIndex=0&leaderID=XXX`, err: `invalid leader id`},
	}
	for i, tt := range tests {
		func() {
			n := NewInitNode()
			defer n.Close()

			// Send heartbeat.
			resp, err := http.Get(n.Server.URL + "/heartbeat?" + tt.query)
			defer resp.Body.Close()
			if err != nil {
				t.Fatalf("%d. unexpected error: %s", i, err)
			} else if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("%d. unexpected status: %d", i, resp.StatusCode)
			}
			if s := resp.Header.Get("X-Raft-Error"); s != tt.err {
				t.Fatalf("%d. unexpected raft error: %s", i, s)
			}
		}()
	}
}

// Ensure that sending a heartbeat to a closed log returns an error.
func TestHTTPHandler_HandleHeartbeat_ErrClosed(t *testing.T) {
	n := NewInitNode()
	n.Log.Close()
	defer n.Close()

	// Send heartbeat.
	resp, err := http.Get(n.Server.URL + "/heartbeat?term=1&commitIndex=0&leaderID=1")
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	if s := resp.Header.Get("X-Raft-Error"); s != "log closed" {
		t.Fatalf("unexpected raft error: %s", s)
	}
}

// Ensure a stream can be retrieved over HTTP.
func TestHTTPHandler_HandleStream(t *testing.T) {
	n := NewInitNode()
	defer n.Close()

	// Connect to stream.
	resp, err := http.Get(n.Server.URL + "/stream?id=1&term=1")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	defer resp.Body.Close()

	// Ensure the stream is connected before applying a command.
	time.Sleep(10 * time.Millisecond)

	// Add an entry.
	if _, err := n.Log.Apply([]byte("xyz")); err != nil {
		t.Fatal(err)
	}

	// Move log's clock ahead & flush data.
	n.Log.Clock.Add(n.Log.HeartbeatInterval)
	n.Log.Flush()

	// Read entries from stream.
	var e raft.LogEntry
	dec := raft.NewLogEntryDecoder(resp.Body)

	// First entry should be the configuration.
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if e.Type != 0xFE {
		t.Fatalf("expected configuration type: %d", e.Type)
	}

	// Next entry should be the snapshot.
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !reflect.DeepEqual(&e, &raft.LogEntry{Type: 0xFF, Data: nil}) {
		t.Fatalf("expected snapshot type: %d", e.Type)
	}

	// Read off the snapshot.
	var fsm FSM
	if err := fsm.Restore(resp.Body); err != nil {
		t.Fatalf("restore: %s", err)
	}

	// Next entry should be the command.
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if !reflect.DeepEqual(&e, &raft.LogEntry{Index: 2, Term: 1, Data: []byte("xyz")}) {
		t.Fatalf("unexpected entry: %#v", &e)
	}
}

// Ensure that requesting a stream with an invalid term will return an error.
func TestHTTPHandler_HandleStream_Error(t *testing.T) {
	var tests = []struct {
		query string
		code  int
		err   string
	}{
		{query: `id=1&term=XXX&index=0`, code: http.StatusBadRequest, err: `invalid term`},
		{query: `id=1&term=1&index=XXX`, code: http.StatusBadRequest, err: `invalid index`},
		{query: `id=XXX&term=1&index=XXX`, code: http.StatusBadRequest, err: `invalid id`},
		{query: `id=1&term=2&index=0`, code: http.StatusInternalServerError, err: `not leader`},
	}
	for i, tt := range tests {
		func() {
			n := NewInitNode()
			defer n.Close()

			// Connect to stream.
			resp, err := http.Get(n.Server.URL + "/stream?" + tt.query)
			defer resp.Body.Close()
			if err != nil {
				t.Fatalf("%d. unexpected error: %s", i, err)
			} else if resp.StatusCode != tt.code {
				t.Fatalf("%d. unexpected status: %d", i, resp.StatusCode)
			}
			if s := resp.Header.Get("X-Raft-Error"); s != tt.err {
				t.Fatalf("%d. unexpected raft error: %s", i, s)
			}
		}()
	}
}

// Ensure a vote request can be sent over HTTP.
func TestHTTPHandler_HandleRequestVote(t *testing.T) {
	n := NewInitNode()
	defer n.Close()

	// Send vote request.
	resp, err := http.Get(n.Server.URL + "/vote?term=5&candidateID=2&lastLogIndex=3&lastLogTerm=4")
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	if s := resp.Header.Get("X-Raft-Error"); s != "" {
		t.Fatalf("unexpected raft error: %s", s)
	}
	if s := resp.Header.Get("X-Raft-Term"); s != "1" {
		t.Fatalf("unexpected raft term: %s", s)
	}
}

// Ensure sending invalid parameters in a vote request returns an error.
func TestHTTPHandler_HandleRequestVote_Error(t *testing.T) {
	var tests = []struct {
		query string
		code  int
		err   string
	}{
		{query: `term=XXX&candidateID=2&lastLogIndex=3&lastLogTerm=4`, code: http.StatusBadRequest, err: `invalid term`},
		{query: `term=5&candidateID=XXX&lastLogIndex=3&lastLogTerm=4`, code: http.StatusBadRequest, err: `invalid candidate id`},
		{query: `term=5&candidateID=2&lastLogIndex=XXX&lastLogTerm=4`, code: http.StatusBadRequest, err: `invalid last log index`},
		{query: `term=5&candidateID=2&lastLogIndex=3&lastLogTerm=XXX`, code: http.StatusBadRequest, err: `invalid last log term`},
		{query: `term=0&candidateID=2&lastLogIndex=0&lastLogTerm=0`, code: http.StatusInternalServerError, err: `stale term`},
	}
	for i, tt := range tests {
		func() {
			n := NewInitNode()
			defer n.Close()

			// Send vote request.
			resp, err := http.Get(n.Server.URL + "/vote?" + tt.query)
			defer resp.Body.Close()
			if err != nil {
				t.Fatalf("%d. unexpected error: %s", i, err)
			} else if resp.StatusCode != tt.code {
				t.Fatalf("%d. unexpected status: %d", i, resp.StatusCode)
			}
			if s := resp.Header.Get("X-Raft-Error"); s != tt.err {
				t.Fatalf("%d. unexpected raft error: %s", i, s)
			}
		}()
	}
}

// Ensure an invalid path returns a 404.
func TestHTTPHandler_NotFound(t *testing.T) {
	n := NewInitNode()
	defer n.Close()

	// Send vote request.
	resp, err := http.Get(n.Server.URL + "/aaaaahhhhh")
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
}
