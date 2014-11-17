package messaging_test

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/messaging"
)

// Ensure a replica can connect and stream messages.
func TestHandler_stream(t *testing.T) {
	s := NewServer()
	defer s.Close()

	// Create replica.
	s.Handler.Broker().CreateReplica("foo")

	// Send request to stream the replica.
	resp, err := http.Get(s.URL + `/messages?name=foo`)
	defer resp.Body.Close()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d: %s", resp.StatusCode, resp.Header.Get("X-Broker-Error"))
	}
	time.Sleep(10 * time.Millisecond)

	// Decode from body.
	var m messaging.Message
	dec := messaging.NewMessageDecoder(resp.Body)
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("decode error: %s", err)
	} else if m.Index != 2 && m.Type != messaging.CreateReplicaMessageType {
		t.Fatalf("unexpected index/type: %d / %x", m.Index, m.Type)
	}
}

// Ensure an error is returned when requesting a stream without a replica name.
func TestHandler_stream_ErrReplicaNameRequired(t *testing.T) {
	s := NewServer()
	defer s.Close()

	resp, _ := http.Get(s.URL + `/messages`)
	defer resp.Body.Close()
	if msg := resp.Header.Get("X-Broker-Error"); resp.StatusCode != http.StatusBadRequest || msg != "replica name required" {
		t.Fatalf("unexpected status/error: %d/%s", resp.StatusCode, msg)
	}
}

// Ensure an error is returned when requesting a stream for a non-existent replica.
func TestHandler_stream_ErrReplicaNotFound(t *testing.T) {
	s := NewServer()
	defer s.Close()

	resp, _ := http.Get(s.URL + `/messages?name=no_such_replica`)
	defer resp.Body.Close()
	if msg := resp.Header.Get("X-Broker-Error"); resp.StatusCode != http.StatusNotFound || msg != "replica not found" {
		t.Fatalf("unexpected status/error: %d/%s", resp.StatusCode, msg)
	}
}

// Ensure an error is returned when requesting a stream with the wrong HTTP method.
func TestHandler_stream_ErrMethodNotAllowed(t *testing.T) {
	s := NewServer()
	defer s.Close()

	resp, _ := http.Head(s.URL + `/messages`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
}

// Ensure a handler can publish a message.
func TestHandler_publish(t *testing.T) {
	s := NewServer()
	defer s.Close()

	// Stream subscription for a replica.
	var m messaging.Message
	s.Handler.Broker().CreateReplica("replica0")
	s.Handler.Broker().Subscribe("replica0", 200)
	go func() {
		resp, _ := http.Get(s.URL + `/messages?name=replica0`)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected response code: %d", resp.StatusCode)
		}

		dec := messaging.NewMessageDecoder(resp.Body)
		for {
			if err := dec.Decode(&m); err != nil {
				return
			}
		}
	}()

	// Send request to the broker.
	resp, _ := http.Post(s.URL+`/messages?type=100&topicID=200`, "application/octet-stream", strings.NewReader(`abc`))
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
}

// Ensure a handler returns an error when publishing a message without a type.
func TestHandler_publish_ErrMessageTypeRequired(t *testing.T) {
	s := NewServer()
	defer s.Close()

	// Send request to the broker.
	resp, _ := http.Post(s.URL+`/messages?topicID=200`, "application/octet-stream", strings.NewReader(`foo`))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "message type required" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
}

// Ensure a handler returns an error when publishing a message without a topic.
func TestHandler_publish_ErrTopicRequired(t *testing.T) {
	s := NewServer()
	defer s.Close()

	// Send request to the broker.
	resp, _ := http.Post(s.URL+`/messages?type=100`, "application/octet-stream", strings.NewReader(`foo`))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "topic required" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
}

// Ensure a handler returns an error when publishing to a closed broker.
func TestHandler_publish_ErrClosed(t *testing.T) {
	s := NewServer()
	s.Handler.Broker().Close()
	defer s.Close()

	// Send request to the broker.
	resp, _ := http.Post(s.URL+`/messages?type=100&topicID=200`, "application/octet-stream", strings.NewReader(`foo`))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "log closed" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
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
	h := messaging.NewHandler(NewBroker().Broker)
	return &Server{httptest.NewServer(h), h}
}

// Close stops the server and broker and removes all temp data.
func (s *Server) Close() {
	(&Broker{s.Handler.Broker()}).Close()
	s.Server.Close()
}
