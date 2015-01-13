package messaging_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
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
	s.Handler.Broker().CreateReplica(2000)

	// Send request to stream the replica.
	resp, err := http.Get(s.URL + `/messaging/messages?replicaID=2000`)
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

// Ensure an error is returned when requesting a stream without a replica id.
func TestHandler_stream_ErrReplicaIDRequired(t *testing.T) {
	s := NewServer()
	defer s.Close()

	resp, _ := http.Get(s.URL + `/messaging/messages`)
	defer resp.Body.Close()
	if msg := resp.Header.Get("X-Broker-Error"); resp.StatusCode != http.StatusBadRequest || msg != "replica id required" {
		t.Fatalf("unexpected status/error: %d/%s", resp.StatusCode, msg)
	}
}

// Ensure an error is returned when requesting a stream for a non-existent replica.
func TestHandler_stream_ErrReplicaNotFound(t *testing.T) {
	s := NewServer()
	defer s.Close()

	resp, _ := http.Get(s.URL + `/messaging/messages?replicaID=0`)
	defer resp.Body.Close()
	if msg := resp.Header.Get("X-Broker-Error"); resp.StatusCode != http.StatusNotFound || msg != "replica not found" {
		t.Fatalf("unexpected status/error: %d/%s", resp.StatusCode, msg)
	}
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

// Ensure a handler can publish a message.
func TestHandler_publish(t *testing.T) {
	s := NewServer()
	defer s.Close()

	// Stream subscription for a replica.
	var m messaging.Message
	s.Handler.Broker().CreateReplica(2000)
	s.Handler.Broker().Subscribe(2000, 200)
	go func() {
		resp, _ := http.Get(s.URL + `/messaging/messages?replicaID=2000`)
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
}

// Ensure a handler returns an error when publishing a message without a type.
func TestHandler_publish_ErrMessageTypeRequired(t *testing.T) {
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
}

// Ensure a handler returns an error when publishing a message without a topic.
func TestHandler_publish_ErrTopicRequired(t *testing.T) {
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
}

// Ensure a handler returns an error when publishing to a closed broker.
func TestHandler_publish_ErrClosed(t *testing.T) {
	s := NewServer()
	s.Handler.Broker().Close()
	defer s.Close()

	// Send request to the broker.
	resp, _ := http.Post(s.URL+`/messaging/messages?type=100&topicID=200`, "application/octet-stream", strings.NewReader(`foo`))
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

// Ensure a handler can correctly create a replica.
func TestHandler_createReplica(t *testing.T) {
	s := NewServer()
	defer s.Close()

	// Send request to the broker.
	resp, _ := http.Post(s.URL+`/messaging/replicas?id=200`, "application/octet-stream", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}

	// Verify replica was created.
	if r := s.Handler.Broker().Replica(200); r == nil {
		t.Fatalf("replica not created")
	}
}

// Ensure a handler returns an error when creating a replica without an id.
func TestHandler_createReplica_ErrReplicaIDRequired(t *testing.T) {
	s := NewServer()
	defer s.Close()

	// Send request to the broker.
	resp, _ := http.Post(s.URL+`/messaging/replicas`, "application/octet-stream", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "replica id required" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
}

// Ensure a handler returns an error when creating a replica that already exists.
func TestHandler_createReplica_ErrReplicaExists(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.Handler.Broker().CreateReplica(200)

	// Send request to the broker.
	resp, _ := http.Post(s.URL+`/messaging/replicas?id=200`, "application/octet-stream", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "replica already exists" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
}

// Ensure a handler can correctly delete a replica.
func TestHandler_deleteReplica(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.Handler.Broker().CreateReplica(200)

	// Send request to the broker.
	req, _ := http.NewRequest("DELETE", s.URL+`/messaging/replicas?id=200`, nil)
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status: %d (%s)", resp.StatusCode, resp.Header.Get("X-Broker-Error"))
	}

	// Verify replica was deleted.
	if r := s.Handler.Broker().Replica(200); r != nil {
		t.Fatalf("replica not deleted")
	}
}

// Ensure a handler returns an error when deleting a replica without an id.
func TestHandler_deleteReplica_ErrReplicaIDRequired(t *testing.T) {
	s := NewServer()
	defer s.Close()

	// Send request to the broker.
	req, _ := http.NewRequest("DELETE", s.URL+`/messaging/replicas`, nil)
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "replica id required" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
}

// Ensure a handler can add a subscription for a replica/topic.
func TestHandler_subscribe(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.Broker().CreateReplica(100)

	// Send request to the broker.
	resp, _ := http.Post(s.URL+`/messaging/subscriptions?replicaID=100&topicID=200`, "application/octet-stream", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}

	// Verify subscription was created.
	if a := s.Handler.Broker().Replica(100).Topics(); !reflect.DeepEqual([]uint64{0, 200}, a) {
		t.Fatalf("topics mismatch: %v", a)
	}
}

// Ensure a handler returns an error when subscribing without a replica id.
func TestHandler_subscribe_ErrReplicaIDRequired(t *testing.T) {
	s := NewServer()
	defer s.Close()
	resp, _ := http.Post(s.URL+`/messaging/subscriptions?topicID=200`, "application/octet-stream", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "replica id required" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
}

// Ensure a handler returns an error when subscribing without a topic id.
func TestHandler_subscribe_ErrTopicRequired(t *testing.T) {
	s := NewServer()
	defer s.Close()
	resp, _ := http.Post(s.URL+`/messaging/subscriptions?replicaID=200`, "application/octet-stream", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "topic required" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
}

// Ensure a handler returns an error when subscribing to a replica that doesn't exist.
func TestHandler_subscribe_ErrReplicaNotFound(t *testing.T) {
	s := NewServer()
	defer s.Close()
	resp, _ := http.Post(s.URL+`/messaging/subscriptions?replicaID=200&topicID=100`, "application/octet-stream", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "replica not found" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
}

// Ensure a handler can unsubscribe a replica from a topic.
func TestHandler_unsubscribe(t *testing.T) {
	s := NewServer()
	defer s.Close()
	s.Handler.Broker().CreateReplica(200)
	s.Handler.Broker().Subscribe(200, 100)

	// Send request to the broker.
	req, _ := http.NewRequest("DELETE", s.URL+`/messaging/subscriptions?replicaID=200&topicID=100`, nil)
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status: %d (%s)", resp.StatusCode, resp.Header.Get("X-Broker-Error"))
	}

	// Verify subscription was removed.
	if a := s.Handler.Broker().Replica(200).Topics(); !reflect.DeepEqual([]uint64{0}, a) {
		t.Fatalf("topics mismatch: %v", a)
	}
}

// Ensure a handler returns an error when unsubscribing without a replica id.
func TestHandler_unsubscribe_ErrReplicaIDRequired(t *testing.T) {
	s := NewServer()
	defer s.Close()
	req, _ := http.NewRequest("DELETE", s.URL+`/messaging/subscriptions?topicID=100`, nil)
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "replica id required" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
}

// Ensure a handler returns an error when unsubscribing without a topic id.
func TestHandler_unsubscribe_ErrTopicRequired(t *testing.T) {
	s := NewServer()
	defer s.Close()
	req, _ := http.NewRequest("DELETE", s.URL+`/messaging/subscriptions?replicaID=100`, nil)
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "topic required" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
}

// Ensure a handler returns an error when unsubscribing to a replica that doesn't exist.
func TestHandler_unsubscribe_ErrReplicaNotFound(t *testing.T) {
	s := NewServer()
	defer s.Close()
	req, _ := http.NewRequest("DELETE", s.URL+`/messaging/subscriptions?replicaID=100&topicID=200`, nil)
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "replica not found" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
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
