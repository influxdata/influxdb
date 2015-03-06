package messaging_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/influxdb/influxdb/messaging"
)

// Ensure a topic can be streamed from an index.
func TestHandler_getMessages(t *testing.T) {
	s := NewServer()
	defer s.Close()

	// Send request to stream the replica.
	resp, err := http.Get(s.URL + `/messaging/messages?topicID=2000`)
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d: %s", resp.StatusCode, resp.Header.Get("X-Broker-Error"))
	}
	time.Sleep(10 * time.Millisecond)

	// TODO: Decode from body.
}

// Ensure an error is returned when requesting a stream with the wrong HTTP method.
func TestHandler_stream_ErrMethodNotAllowed(t *testing.T) {
	s := NewServer()
	defer s.Close()

	resp, _ := http.Head(s.URL + `/messaging/messages`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
}

// Ensure a handler can receive a message.
func TestHandler_postMessages(t *testing.T) {
	t.Skip("pending")
	/*
		s := NewServer()
		defer s.Close()

		// Send request to the broker.
		resp, _ := http.Post(s.URL+`/messaging/messages?type=100&topicID=200`, "application/octet-stream", strings.NewReader(`abc`))
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected status: %d: %s", resp.StatusCode, resp.Header.Get("X-Broker-Error"))
		}
		s.Handler.Broker().Sync(4)

		// Check if the last message received is our new message.
		time.Sleep(10 * time.Millisecond)
		if !reflect.DeepEqual(&m, &messaging.Message{Type: 100, Index: 4, TopicID: 200, Data: []byte("abc")}) {
			t.Fatalf("unexpected message: %#v", &m)
		}
	*/
}

// Ensure a handler returns an error when publishing a message without a type.
func TestHandler_publish_ErrMessageTypeRequired(t *testing.T) {
	t.Skip("pending")
	/*
		s := NewServer()
		defer s.Close()

		// Send request to the broker.
		resp, _ := http.Post(s.URL+`/messaging/messages?topicID=200`, "application/octet-stream", strings.NewReader(`foo`))
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("unexpected status: %d", resp.StatusCode)
		} else if resp.Header.Get("X-Broker-Error") != "message type required" {
			t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
		}
	*/
}

// Ensure a handler returns an error when publishing a message without a topic.
func TestHandler_publish_ErrTopicRequired(t *testing.T) {
	t.Skip("pending")

	/*
		s := NewServer()
		defer s.Close()

		// Send request to the broker.
		resp, _ := http.Post(s.URL+`/messaging/messages?type=100`, "application/octet-stream", strings.NewReader(`foo`))
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("unexpected status: %d", resp.StatusCode)
		} else if resp.Header.Get("X-Broker-Error") != "topic required" {
			t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
		}
	*/
}

// Ensure the handler routes raft requests to the raft handler.
func TestHandler_raft(t *testing.T) {
	s := NewServer()
	defer s.Close()
	resp, _ := http.Get(s.URL + `/raft/ping`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
}

// Ensure the handler returns an error for an invalid path.
func TestHandler_ErrNotFound(t *testing.T) {
	s := NewServer()
	defer s.Close()
	resp, _ := http.Get(s.URL + `/no_such_path`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
}

// Server is an test HTTP server that wraps a handler and broker.
type Server struct {
	*httptest.Server
	Handler *messaging.Handler
}

// NewServer returns a test server.
func NewServer() *Server {
	h := messaging.NewHandler(nil)
	s := httptest.NewServer(h)
	h.SetBroker(NewBroker(MustParseURL(s.URL)).Broker)
	return &Server{s, h}
}

// NewUninitializedServer returns a test server with an uninitialized broker.
func NewUninitializedServer() *Server {
	h := messaging.NewHandler(nil)
	s := httptest.NewServer(h)
	h.SetBroker(NewUninitializedBroker(MustParseURL(s.URL)).Broker)
	return &Server{s, h}
}

// Close stops the server and broker and removes all temp data.
func (s *Server) Close() {
	s.Broker().Close()
	s.Server.Close()
}

// Broker returns a reference to the broker attached to the handler.
func (s *Server) Broker() *Broker { return &Broker{s.Handler.Broker()} }

// MustParseURL parses a string into a URL. Panic on error.
func MustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err.Error())
	}
	return u
}
