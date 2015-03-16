package raft_test

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/influxdb/influxdb/raft"
)

func init() {
	// Ensure Log implements the Handler.Log interface.
	_ = raft.Handler{Log: raft.NewLog()}
}

// Ensure a node can join a cluster over HTTP.
func TestHandler_HandleJoin(t *testing.T) {
	h := NewHandler()
	h.AddPeerFunc = func(u url.URL) (uint64, uint64, *raft.Config, error) {
		if u.String() != "http://localhost:1000" {
			t.Fatalf("unexpected url: %s", u)
		}
		return 2, 3, &raft.Config{}, nil
	}
	s := httptest.NewServer(h)
	defer s.Close()

	// Send request to join cluster.
	resp, err := http.Get(s.URL + "/join?url=" + url.QueryEscape("http://localhost:1000"))
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d: %s", resp.StatusCode, resp.Header.Get("X-Raft-Error"))
	} else if s := resp.Header.Get("X-Raft-Error"); s != "" {
		t.Fatalf("unexpected raft error: %s", s)
	} else if s = resp.Header.Get("X-Raft-ID"); s != "2" {
		t.Fatalf("unexpected raft id: %s", s)
	} else if s = resp.Header.Get("X-Raft-Leader-ID"); s != "3" {
		t.Fatalf("unexpected raft leader id: %s", s)
	}
}

// Ensure that joining with an invalid query string with return an error.
func TestHandler_HandleJoin_Error(t *testing.T) {
	h := NewHandler()
	h.AddPeerFunc = func(u url.URL) (uint64, uint64, *raft.Config, error) {
		return 0, 0, nil, raft.ErrClosed
	}
	s := httptest.NewServer(h)
	defer s.Close()

	for i, tt := range []struct {
		query string
		code  int
		err   string
	}{
		{query: ``, code: http.StatusBadRequest, err: `url required`},
		{query: `url=//foo%23%252`, code: http.StatusBadRequest, err: `invalid url`},
		{query: `url=http%3A//localhost%3A1000`, code: http.StatusInternalServerError, err: `log closed`},
	} {
		resp, err := http.Get(s.URL + "/join?" + tt.query)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("%d. unexpected error: %s", i, err)
		} else if resp.StatusCode != tt.code {
			t.Fatalf("%d. unexpected status: %d", i, resp.StatusCode)
		} else if s := resp.Header.Get("X-Raft-Error"); s != tt.err {
			t.Fatalf("%d. unexpected raft error: %s", i, s)
		}
	}
}

// Ensure a node can leave a cluster over HTTP.
func TestHandler_HandleLeave(t *testing.T) {
	h := NewHandler()
	h.RemovePeerFunc = func(id uint64) error {
		if id != 1 {
			t.Fatalf("unexpected id: %d", id)
		}
		return nil
	}
	s := httptest.NewServer(h)
	defer s.Close()

	// Send request to join cluster.
	resp, err := http.Get(s.URL + "/leave?id=1")
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d: %s", resp.StatusCode, resp.Header.Get("X-Raft-Error"))
	} else if s := resp.Header.Get("X-Raft-Error"); s != "" {
		t.Fatalf("unexpected raft error: %s", s)
	}
}

// Ensure that leaving with an invalid query string with return an error.
func TestHandler_HandleLeave_Error(t *testing.T) {
	h := NewHandler()
	h.RemovePeerFunc = func(id uint64) error {
		return raft.ErrClosed
	}
	s := httptest.NewServer(h)
	defer s.Close()

	for i, tt := range []struct {
		query string
		code  int
		err   string
	}{
		{query: `id=xxx`, code: http.StatusBadRequest, err: `invalid raft id`},
		{query: `id=1`, code: http.StatusInternalServerError, err: `log closed`},
	} {
		resp, err := http.Get(s.URL + "/leave?" + tt.query)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("%d. unexpected error: %s", i, err)
		} else if resp.StatusCode != tt.code {
			t.Fatalf("%d. unexpected status: %d", i, resp.StatusCode)
		} else if s := resp.Header.Get("X-Raft-Error"); s != tt.err {
			t.Fatalf("%d. unexpected raft error: %s", i, s)
		}
	}
}

// Ensure a heartbeat can be sent over HTTP.
func TestHandler_HandleHeartbeat(t *testing.T) {
	h := NewHandler()
	h.HeartbeatFunc = func(term, commitIndex, leaderID uint64) (currentIndex uint64, err error) {
		if term != 1 {
			t.Fatalf("unexpected term: %d", term)
		} else if commitIndex != 2 {
			t.Fatalf("unexpected commit index: %d", commitIndex)
		} else if leaderID != 3 {
			t.Fatalf("unexpected leader id: %d", leaderID)
		}
		return 4, nil
	}
	s := httptest.NewServer(h)
	defer s.Close()

	// Send heartbeat.
	resp, err := http.Get(s.URL + "/heartbeat?term=1&commitIndex=2&leaderID=3")
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if s := resp.Header.Get("X-Raft-Error"); s != "" {
		t.Fatalf("unexpected raft error: %s", s)
	} else if s = resp.Header.Get("X-Raft-Index"); s != "4" {
		t.Fatalf("unexpected raft index: %s", s)
	}
}

// Ensure that sending a heartbeat with an invalid term returns an error.
func TestHandler_HandleHeartbeat_Error(t *testing.T) {
	h := NewHandler()
	s := httptest.NewServer(h)
	defer s.Close()

	var tests = []struct {
		query string
		err   string
	}{
		{query: `term=XXX&commitIndex=0&leaderID=1`, err: `invalid term`},
		{query: `term=1&commitIndex=XXX&leaderID=1`, err: `invalid commit index`},
		{query: `term=1&commitIndex=0&leaderID=XXX`, err: `invalid leader id`},
	}
	for i, tt := range tests {
		resp, err := http.Get(s.URL + "/heartbeat?" + tt.query)
		resp.Body.Close()
		if err != nil {
			t.Errorf("%d. unexpected error: %s", i, err)
		} else if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%d. unexpected status: %d", i, resp.StatusCode)
		} else if s := resp.Header.Get("X-Raft-Error"); s != tt.err {
			t.Errorf("%d. unexpected raft error: %s", i, s)
		}
	}
}

// Ensure that sending a heartbeat to a closed log returns an error.
func TestHandler_HandleHeartbeat_ErrClosed(t *testing.T) {
	h := NewHandler()
	h.HeartbeatFunc = func(term, commitIndex, leaderID uint64) (currentIndex uint64, err error) {
		return 0, raft.ErrClosed
	}
	s := httptest.NewServer(h)
	defer s.Close()

	// Send heartbeat.
	resp, err := http.Get(s.URL + "/heartbeat?term=0&commitIndex=0&leaderID=0")
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if s := resp.Header.Get("X-Raft-Error"); s != "log closed" {
		t.Fatalf("unexpected raft error: %s", s)
	}
}

// Ensure a stream can be retrieved over HTTP.
func TestHandler_HandleStream(t *testing.T) {
	h := NewHandler()
	h.WriteEntriesToFunc = func(w io.Writer, id, term, index uint64) error {
		if w == nil {
			t.Fatalf("expected writer")
		} else if id != 1 {
			t.Fatalf("unexpected id: %d", id)
		} else if term != 2 {
			t.Fatalf("unexpected term: %d", term)
		}

		w.Write([]byte("ok"))
		return nil
	}
	s := httptest.NewServer(h)
	defer s.Close()

	// Connect to stream.
	resp, err := http.Get(s.URL + "/stream?id=1&term=2")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	defer resp.Body.Close()

	// Read entries from stream.
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if string(b) != "ok" {
		t.Fatalf("unexpected body: %s", b)
	}
}

// Ensure that requesting a stream with an invalid term will return an error.
func TestHandler_HandleStream_Error(t *testing.T) {
	h := NewHandler()
	h.WriteEntriesToFunc = func(w io.Writer, id, term, index uint64) error {
		return raft.ErrNotLeader
	}
	s := httptest.NewServer(h)
	defer s.Close()

	var tests = []struct {
		query string
		code  int
		err   string
	}{
		{query: `id=1&term=XXX&index=0`, code: http.StatusBadRequest, err: `invalid term`},
		{query: `id=1&term=1&index=XXX`, code: http.StatusBadRequest, err: `invalid index`},
		{query: `id=XXX&term=1&index=XXX`, code: http.StatusBadRequest, err: `invalid id`},
		{query: `id=0&term=1&index=2`, code: http.StatusInternalServerError, err: `not leader`},
	}
	for i, tt := range tests {
		resp, err := http.Get(s.URL + "/stream?" + tt.query)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("%d. unexpected error: %s", i, err)
		} else if resp.StatusCode != tt.code {
			t.Fatalf("%d. unexpected status: %d", i, resp.StatusCode)
		} else if s := resp.Header.Get("X-Raft-Error"); s != tt.err {
			t.Fatalf("%d. unexpected raft error: %s", i, s)
		}
	}
}

// Ensure a vote request can be sent over HTTP.
func TestHandler_HandleRequestVote(t *testing.T) {
	h := NewHandler()
	h.RequestVoteFunc = func(term, candidateID, lastLogIndex, lastLogTerm uint64) error {
		if term != 1 {
			t.Fatalf("unexpected term: %d", term)
		} else if candidateID != 2 {
			t.Fatalf("unexpected candidate id: %d", candidateID)
		} else if lastLogIndex != 3 {
			t.Fatalf("unexpected last log index: %d", lastLogIndex)
		} else if lastLogTerm != 4 {
			t.Fatalf("unexpected last log term: %d", lastLogTerm)
		}
		return nil
	}
	s := httptest.NewServer(h)
	defer s.Close()

	// Send vote request.
	resp, err := http.Get(s.URL + "/vote?term=1&candidateID=2&lastLogIndex=3&lastLogTerm=4")
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if s := resp.Header.Get("X-Raft-Error"); s != "" {
		t.Fatalf("unexpected raft error: %s", s)
	}
}

// Ensure sending invalid parameters in a vote request returns an error.
func TestHandler_HandleRequestVote_Error(t *testing.T) {
	h := NewHandler()
	h.RequestVoteFunc = func(term, candidateID, lastLogIndex, lastLogTerm uint64) error {
		return raft.ErrStaleTerm
	}
	s := httptest.NewServer(h)
	defer s.Close()

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
		resp, err := http.Get(s.URL + "/vote?" + tt.query)
		defer resp.Body.Close()
		if err != nil {
			t.Fatalf("%d. unexpected error: %s", i, err)
		} else if resp.StatusCode != tt.code {
			t.Fatalf("%d. unexpected status: %d", i, resp.StatusCode)
		} else if s := resp.Header.Get("X-Raft-Error"); s != tt.err {
			t.Fatalf("%d. unexpected raft error: %s", i, s)
		}
	}
}

// Ensure an invalid path returns a 404.
func TestHandler_NotFound(t *testing.T) {
	s := httptest.NewServer(NewHandler())
	defer s.Close()

	// Send vote request.
	resp, err := http.Get(s.URL + "/aaaaahhhhh")
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
}

// Ensure a ping returns a 200 OK.
func TestHandler_Ping(t *testing.T) {
	s := httptest.NewServer(NewHandler())
	defer s.Close()

	// Send vote request.
	resp, err := http.Get(s.URL + "/ping")
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
}

// Handler represents a test wrapper for the raft.Handler.
type Handler struct {
	*raft.Handler
	AddPeerFunc        func(u url.URL) (uint64, uint64, *raft.Config, error)
	RemovePeerFunc     func(id uint64) error
	HeartbeatFunc      func(term, commitIndex, leaderID uint64) (currentIndex uint64, err error)
	WriteEntriesToFunc func(w io.Writer, id, term, index uint64) error
	RequestVoteFunc    func(term, candidateID, lastLogIndex, lastLogTerm uint64) error
}

// NewHandler returns a new instance of Handler.
func NewHandler() *Handler {
	h := &Handler{Handler: &raft.Handler{}}
	h.Handler.Log = h
	return h
}

func (h *Handler) AddPeer(u url.URL) (uint64, uint64, *raft.Config, error) { return h.AddPeerFunc(u) }
func (h *Handler) RemovePeer(id uint64) error                              { return h.RemovePeerFunc(id) }

func (h *Handler) Heartbeat(term, commitIndex, leaderID uint64) (currentIndex uint64, err error) {
	return h.HeartbeatFunc(term, commitIndex, leaderID)
}

func (h *Handler) WriteEntriesTo(w io.Writer, id, term, index uint64) error {
	return h.WriteEntriesToFunc(w, id, term, index)
}

func (h *Handler) RequestVote(term, candidateID, lastLogIndex, lastLogTerm uint64) error {
	return h.RequestVoteFunc(term, candidateID, lastLogIndex, lastLogTerm)
}
