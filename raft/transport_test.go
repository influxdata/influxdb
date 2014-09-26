package raft_test

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/influxdb/influxdb/raft"
)

// Ensure a join on an unsupported scheme returns an error.
func TestTransportMux_Join_ErrUnsupportedScheme(t *testing.T) {
	u, _ := url.Parse("foo://bar")
	_, _, err := raft.DefaultTransport.Join(u, nil)
	if err == nil || err.Error() != `transport scheme not supported: foo` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure a heartbeat on an unsupported scheme returns an error.
func TestTransportMux_Heartbeat_ErrUnsupportedScheme(t *testing.T) {
	u, _ := url.Parse("foo://bar")
	_, _, err := raft.DefaultTransport.Heartbeat(u, 0, 0, 0)
	if err == nil || err.Error() != `transport scheme not supported: foo` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure a stream on an unsupported scheme returns an error.
func TestTransportMux_ReadFrom_ErrUnsupportedScheme(t *testing.T) {
	u, _ := url.Parse("foo://bar")
	_, err := raft.DefaultTransport.ReadFrom(u, 0, 0, 0)
	if err == nil || err.Error() != `transport scheme not supported: foo` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure a stream on an unsupported scheme returns an error.
func TestTransportMux_RequestVote_ErrUnsupportedScheme(t *testing.T) {
	u, _ := url.Parse("foo://bar")
	_, err := raft.DefaultTransport.RequestVote(u, 0, 0, 0, 0)
	if err == nil || err.Error() != `transport scheme not supported: foo` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure a heartbeat over HTTP can be read and responded to.
func TestHTTPTransport_Heartbeat(t *testing.T) {
	// Start mock HTTP server.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if path := r.URL.Path; path != `/heartbeat` {
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
		w.Header().Set("X-Raft-Term", "5")
		w.WriteHeader(http.StatusOK)
	}))
	defer s.Close()

	// Execute heartbeat against test server.
	u, _ := url.Parse(s.URL)
	newIndex, newTerm, err := raft.DefaultTransport.Heartbeat(u, 1, 2, 3)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if newIndex != 4 {
		t.Fatalf("unexpected new index: %d", newIndex)
	} else if newTerm != 5 {
		t.Fatalf("unexpected new term: %d", newTerm)
	}
}

// Ensure HTTP heartbeats return correct errors.
func TestHTTPTransport_Heartbeat_Err(t *testing.T) {
	var tests = []struct {
		index  string
		term   string
		errstr string
		err    string
	}{
		{index: "", term: "", err: `invalid index: ""`},
		{index: "1000", term: "", err: `invalid term: ""`},
		{index: "1", term: "2", errstr: "bad heartbeat", err: `bad heartbeat`},
	}
	for i, tt := range tests {
		// Start mock HTTP server.
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Raft-Index", tt.index)
			w.Header().Set("X-Raft-Term", tt.term)
			w.Header().Set("X-Raft-Error", tt.errstr)
			w.WriteHeader(http.StatusOK)
		}))

		u, _ := url.Parse(s.URL)
		_, _, err := raft.DefaultTransport.Heartbeat(u, 1, 2, 3)
		if err == nil {
			t.Errorf("%d. expected error")
		} else if tt.err != err.Error() {
			t.Errorf("%d. error:\n\nexp: %s\n\ngot: %s", i, tt.err, err.Error())
		}
		s.Close()
	}
}

// Ensure an HTTP heartbeat to a stopped server returns an error.
func TestHTTPTransport_Heartbeat_ErrConnectionRefused(t *testing.T) {
	u, _ := url.Parse("http://localhost:41932")
	_, _, err := raft.DefaultTransport.Heartbeat(u, 0, 0, 0)
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
		if path := r.URL.Path; path != `/stream` {
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
	r, err := raft.DefaultTransport.ReadFrom(u, 1, 2, 3)
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
	r, err := raft.DefaultTransport.ReadFrom(u, 0, 0, 0)
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
	_, err := raft.DefaultTransport.ReadFrom(u, 0, 0, 0)
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
		if path := r.URL.Path; path != `/vote` {
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
		w.Header().Set("X-Raft-Term", "5")
		w.WriteHeader(http.StatusOK)
	}))
	defer s.Close()

	// Execute heartbeat against test server.
	u, _ := url.Parse(s.URL)
	newTerm, err := raft.DefaultTransport.RequestVote(u, 1, 2, 3, 4)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if newTerm != 5 {
		t.Fatalf("unexpected new term: %d", newTerm)
	}
}

// Ensure that a returned vote with an invalid term returns an error.
func TestHTTPTransport_RequestVote_ErrInvalidTerm(t *testing.T) {
	// Start mock HTTP server.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Raft-Term", `xxx`)
	}))
	defer s.Close()

	u, _ := url.Parse(s.URL)
	_, err := raft.DefaultTransport.RequestVote(u, 0, 0, 0, 0)
	if err == nil {
		t.Errorf("expected error")
	} else if err.Error() != `invalid term: "xxx"` {
		t.Errorf("unexpected error: %s", err)
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
	_, err := raft.DefaultTransport.RequestVote(u, 0, 0, 0, 0)
	if err == nil {
		t.Errorf("expected error")
	} else if err.Error() != `already voted` {
		t.Errorf("unexpected error: %s", err)
	}
}

// Ensure that requesting a vote over HTTP to a stopped server returns an error.
func TestHTTPTransport_RequestVote_ErrConnectionRefused(t *testing.T) {
	u, _ := url.Parse("http://localhost:41932")
	_, err := raft.DefaultTransport.RequestVote(u, 0, 0, 0, 0)
	if err == nil {
		t.Fatal("expected error")
	} else if !strings.Contains(err.Error(), `connection refused`) {
		t.Fatalf("unexpected error: %s", err)
	}
}
