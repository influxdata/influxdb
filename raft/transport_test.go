package raft_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdb/influxdb/raft"
)

// Ensure a join over HTTP can be read and responded to.
func TestHTTPTransport_Join(t *testing.T) {
	// Start mock HTTP server.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if path := r.URL.Path; path != `/raft/join` {
			t.Fatalf("unexpected path: %q", path)
		}
		if s := r.FormValue("url"); s != `//local` {
			t.Fatalf("unexpected term: %q", s)
		}
		w.Header().Set("X-Raft-ID", "1")
		w.Header().Set("X-Raft-Leader-ID", "2")
		w.Write([]byte(`{}`))
	}))
	defer s.Close()

	// Execute join against test server.
	u, _ := url.Parse(s.URL)
	id, leaderID, config, err := (&raft.HTTPTransport{}).Join(*u, url.URL{Host: "local"})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if id != 1 {
		t.Fatalf("unexpected id: %d", id)
	} else if leaderID != 2 {
		t.Fatalf("unexpected leader id: %d", leaderID)
	} else if config == nil {
		t.Fatalf("unexpected config")
	}
}

// Ensure that joining a server that doesn't exist returns an error.
func TestHTTPTransport_Join_ErrConnectionRefused(t *testing.T) {
	_, _, _, err := (&raft.HTTPTransport{}).Join(url.URL{Scheme: "http", Host: "localhost:27322"}, url.URL{Host: "local"})
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the response from a join contains a valid id.
func TestHTTPTransport_Join_ErrInvalidID(t *testing.T) {
	// Start mock HTTP server.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Raft-ID", "xxx")
	}))
	defer s.Close()

	// Execute join against test server.
	u, _ := url.Parse(s.URL)
	_, _, _, err := (&raft.HTTPTransport{}).Join(*u, url.URL{Host: "local"})
	if err == nil || err.Error() != `invalid id: "xxx"` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the response from a join contains a valid config.
func TestHTTPTransport_Join_ErrInvalidConfig(t *testing.T) {
	// Start mock HTTP server.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Raft-ID", "1")
		w.Header().Set("X-Raft-Leader-ID", "1")
		w.Write([]byte(`{`))
	}))
	defer s.Close()

	// Execute join against test server.
	u, _ := url.Parse(s.URL)
	_, _, _, err := (&raft.HTTPTransport{}).Join(*u, url.URL{Host: "local"})
	if err == nil || err.Error() != `config unmarshal: unexpected EOF` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the errors returned from a join are passed through.
func TestHTTPTransport_Join_Err(t *testing.T) {
	// Start mock HTTP server.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Raft-ID", "")
		w.Header().Set("X-Raft-Error", "oh no")
	}))
	defer s.Close()

	// Execute join against test server.
	u, _ := url.Parse(s.URL)
	_, _, _, err := (&raft.HTTPTransport{}).Join(*u, url.URL{Host: "local"})
	if err == nil || err.Error() != `oh no` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure a leave over HTTP can be read and responded to.
func TestHTTPTransport_Leave(t *testing.T) {
	// Start mock HTTP server.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if path := r.URL.Path; path != `/raft/leave` {
			t.Fatalf("unexpected path: %q", path)
		} else if id := r.FormValue("id"); id != `1` {
			t.Fatalf("unexpected id: %q", id)
		}
	}))
	defer s.Close()

	// Execute leave against test server.
	u, _ := url.Parse(s.URL)
	if err := (&raft.HTTPTransport{}).Leave(*u, 1); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that leaving a server that doesn't exist returns an error.
func TestHTTPTransport_Leave_ErrConnectionRefused(t *testing.T) {
	err := (&raft.HTTPTransport{}).Leave(url.URL{Scheme: "http", Host: "localhost:27322"}, 1)
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the errors returned from a leave are passed through.
func TestHTTPTransport_Leave_Err(t *testing.T) {
	// Start mock HTTP server.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Raft-Error", "oh no")
	}))
	defer s.Close()

	// Execute leave against test server.
	u, _ := url.Parse(s.URL)
	err := (&raft.HTTPTransport{}).Leave(*u, 1)
	if err == nil || err.Error() != `oh no` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure a heartbeat over HTTP can be read and responded to.
func TestHTTPTransport_Heartbeat(t *testing.T) {
	// Start mock HTTP server.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if path := r.URL.Path; path != `/raft/heartbeat` {
			t.Fatalf("unexpected path: %q", path)
		}
		if term := r.FormValue("term"); term != `1` {
			t.Fatalf("unexpected term: %q", term)
		}
		if commitIndex := r.FormValue("commitIndex"); commitIndex != `2` {
			t.Fatalf("unexpected commit index: %q", commitIndex)
		}
		if leaderID := r.FormValue("leaderID"); leaderID != `3` {
			t.Fatalf("unexpected leader id: %q", leaderID)
		}
		w.Header().Set("X-Raft-Index", "4")
		w.WriteHeader(http.StatusOK)
	}))
	defer s.Close()

	// Execute heartbeat against test server.
	u, _ := url.Parse(s.URL)
	newIndex, err := (&raft.HTTPTransport{}).Heartbeat(*u, 1, 2, 3)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if newIndex != 4 {
		t.Fatalf("unexpected new index: %d", newIndex)
	}
}

// Ensure HTTP heartbeats return correct errors.
func TestHTTPTransport_Heartbeat_Err(t *testing.T) {
	var tests = []struct {
		index  string
		errstr string
		err    string
	}{
		{index: "", err: `invalid index: ""`},
		{index: "1", errstr: "bad heartbeat", err: `bad heartbeat`},
	}
	for i, tt := range tests {
		// Start mock HTTP server.
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Raft-Index", tt.index)
			w.Header().Set("X-Raft-Error", tt.errstr)
			w.WriteHeader(http.StatusOK)
		}))

		u, _ := url.Parse(s.URL)
		_, err := (&raft.HTTPTransport{}).Heartbeat(*u, 1, 2, 3)
		if err == nil {
			t.Errorf("%d. expected error", i)
		} else if tt.err != err.Error() {
			t.Errorf("%d. error:\n\nexp: %s\n\ngot: %s", i, tt.err, err.Error())
		}
		s.Close()
	}
}

// Ensure an HTTP heartbeat to a stopped server returns an error.
func TestHTTPTransport_Heartbeat_ErrConnectionRefused(t *testing.T) {
	u, _ := url.Parse("http://localhost:41932")
	_, err := (&raft.HTTPTransport{}).Heartbeat(*u, 0, 0, 0)
	if err == nil {
		t.Fatal("expected error")
	} else if !strings.Contains(err.Error(), `connection refused`) {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the log can be streamed over HTTP.
func TestHTTPTransport_ReadFrom(t *testing.T) {
	// Start mock HTTP server.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if path := r.URL.Path; path != `/raft/stream` {
			t.Fatalf("unexpected path: %q", path)
		}
		if term := r.FormValue("id"); term != `1` {
			t.Fatalf("unexpected term: %q", term)
		}
		if term := r.FormValue("term"); term != `2` {
			t.Fatalf("unexpected term: %q", term)
		}
		if index := r.FormValue("index"); index != `3` {
			t.Fatalf("unexpected index: %q", index)
		}
		w.Write([]byte("test123"))
	}))
	defer s.Close()

	// Execute stream against test server.
	u, _ := url.Parse(s.URL)
	r, err := (&raft.HTTPTransport{}).ReadFrom(*u, 1, 2, 3)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	b, _ := ioutil.ReadAll(r)
	if string(b) != `test123` {
		t.Fatalf("unexpected stream: %q", string(b))
	}
}

// Ensure a stream can return an error.
func TestHTTPTransport_ReadFrom_Err(t *testing.T) {
	// Start mock HTTP server.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Raft-Error", `bad stream`)
	}))
	defer s.Close()

	// Execute stream against test server.
	u, _ := url.Parse(s.URL)
	r, err := (&raft.HTTPTransport{}).ReadFrom(*u, 0, 0, 0)
	if err == nil {
		t.Fatalf("expected error")
	} else if err.Error() != `bad stream` {
		t.Fatalf("unexpected error: %s", err)
	} else if r != nil {
		t.Fatal("unexpected reader")
	}
}

// Ensure an streaming over HTTP to a stopped server returns an error.
func TestHTTPTransport_ReadFrom_ErrConnectionRefused(t *testing.T) {
	u, _ := url.Parse("http://localhost:41932")
	_, err := (&raft.HTTPTransport{}).ReadFrom(*u, 0, 0, 0)
	if err == nil {
		t.Fatal("expected error")
	} else if !strings.Contains(err.Error(), `connection refused`) {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that requesting over HTTP can be read and responded to.
func TestHTTPTransport_RequestVote(t *testing.T) {
	// Start mock HTTP server.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if path := r.URL.Path; path != `/raft/vote` {
			t.Fatalf("unexpected path: %s", path)
		}
		if term := r.FormValue("term"); term != `1` {
			t.Fatalf("unexpected term: %v", term)
		}
		if candidateID := r.FormValue("candidateID"); candidateID != `2` {
			t.Fatalf("unexpected candidate id: %v", candidateID)
		}
		if lastLogIndex := r.FormValue("lastLogIndex"); lastLogIndex != `3` {
			t.Fatalf("unexpected last log index: %v", lastLogIndex)
		}
		if lastLogTerm := r.FormValue("lastLogTerm"); lastLogTerm != `4` {
			t.Fatalf("unexpected last log term: %v", lastLogTerm)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer s.Close()

	// Execute heartbeat against test server.
	u, _ := url.Parse(s.URL)
	if err := (&raft.HTTPTransport{}).RequestVote(*u, 1, 2, 3, 4); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that a returned vote with an error message returns that error.
func TestHTTPTransport_RequestVote_Error(t *testing.T) {
	// Start mock HTTP server.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Raft-Term", `1`)
		w.Header().Set("X-Raft-Error", `already voted`)
	}))
	defer s.Close()

	u, _ := url.Parse(s.URL)
	if err := (&raft.HTTPTransport{}).RequestVote(*u, 0, 0, 0, 0); err == nil {
		t.Errorf("expected error")
	} else if err.Error() != `already voted` {
		t.Errorf("unexpected error: %s", err)
	}
}

// Ensure that requesting a vote over HTTP to a stopped server returns an error.
func TestHTTPTransport_RequestVote_ErrConnectionRefused(t *testing.T) {
	u, _ := url.Parse("http://localhost:41932")
	if err := (&raft.HTTPTransport{}).RequestVote(*u, 0, 0, 0, 0); err == nil {
		t.Fatal("expected error")
	} else if !strings.Contains(err.Error(), `connection refused`) {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Transport represents a test transport that directly calls another log.
// Logs are looked up by hostname only.
type Transport struct {
	logs map[string]*raft.Log // logs by host
}

// NewTransport returns a new instance of Transport.
func NewTransport() *Transport {
	return &Transport{logs: make(map[string]*raft.Log)}
}

// register registers a log by hostname.
func (t *Transport) register(l *raft.Log) {
	t.logs[l.URL().Host] = l
}

// log returns a log registered by hostname.
func (t *Transport) log(u url.URL) (*raft.Log, error) {
	if l := t.logs[u.Host]; l != nil {
		return l, nil
	}
	return nil, fmt.Errorf("log not found: %s", u.String())
}

// Join calls the AddPeer method on the target log.
func (t *Transport) Join(u url.URL, nodeURL url.URL) (uint64, uint64, *raft.Config, error) {
	l, err := t.log(u)
	if err != nil {
		return 0, 0, nil, err
	}
	return l.AddPeer(nodeURL)
}

// Leave calls the RemovePeer method on the target log.
func (t *Transport) Leave(u url.URL, id uint64) error {
	l, err := t.log(u)
	if err != nil {
		return err
	}
	return l.RemovePeer(id)
}

// Heartbeat calls the Heartbeat method on the target log.
func (t *Transport) Heartbeat(u url.URL, term, commitIndex, leaderID uint64) (lastIndex uint64, err error) {
	l, err := t.log(u)
	if err != nil {
		return 0, err
	}
	return l.Heartbeat(term, commitIndex, leaderID)
}

// ReadFrom streams entries from the target log.
func (t *Transport) ReadFrom(u url.URL, id, term, index uint64) (io.ReadCloser, error) {
	l, err := t.log(u)
	if err != nil {
		return nil, err
	}

	// Create a streaming buffer that will hang until Close() is called.
	buf := newStreamingBuffer()
	go func() {
		if err := l.WriteEntriesTo(buf, id, term, index); err != nil {
			warnf("Transport.ReadFrom: error: %s", err)
		}
		_ = buf.Close()
	}()
	return buf, nil
}

// RequestVote calls RequestVote() on the target log.
func (t *Transport) RequestVote(u url.URL, term, candidateID, lastLogIndex, lastLogTerm uint64) error {
	l, err := t.log(u)
	if err != nil {
		return err
	}
	return l.RequestVote(term, candidateID, lastLogIndex, lastLogTerm)
}

// streamingBuffer implements a streaming bytes buffer.
// This will hang during reads until there is data available or the streamer is closed.
type streamingBuffer struct {
	mu     sync.Mutex
	buf    *bytes.Buffer
	closed bool
}

// newStreamingBuffer returns a new streamingBuffer.
func newStreamingBuffer() *streamingBuffer {
	return &streamingBuffer{buf: bytes.NewBuffer(nil)}
}

// Close marks the buffer as closed.
func (b *streamingBuffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

// Closed returns true if Close() has been called.
func (b *streamingBuffer) Closed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

func (b *streamingBuffer) Read(p []byte) (n int, err error) {
	for {
		b.mu.Lock()
		n, err = b.buf.Read(p)
		b.mu.Unlock()

		if err == io.EOF && n > 0 { // hit EOF, read data
			return n, nil
		} else if err == io.EOF { // hit EOF, no data
			// If closed then return EOF.
			if b.Closed() {
				return n, err
			}

			// If not closed then wait a bit and try again.
			time.Sleep(1 * time.Millisecond)
			continue
		}

		// If we've read data or we've hit a non-EOF error then return.
		return n, err
	}
}

func (b *streamingBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// Ensure the streaming buffer will continue to stream data, if available, after it's closed.
// This is primarily a santity check to make sure our test buffer isn't causing problems.
func TestStreamingBuffer(t *testing.T) {
	// Write some data to buffer.
	buf := newStreamingBuffer()
	buf.buf.WriteString("foo")

	// Read all data out in separate goroutine.
	start := make(chan struct{}, 0)
	ch := make(chan string, 0)
	go func() {
		close(start)
		b, err := ioutil.ReadAll(buf)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		ch <- string(b)
	}()

	// Wait for reader to kick in.
	<-start

	// Write some more data and then close.
	buf.buf.WriteString("bar")
	buf.Close()

	// Verify all data was read.
	if s := <-ch; s != "foobar" {
		t.Fatalf("unexpected output: %s", s)
	}
}
