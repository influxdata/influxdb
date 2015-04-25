package messaging_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/influxdb/influxdb/messaging"
)

// Ensure a topic can be streamed from an index.
func TestHandler_getMessages(t *testing.T) {
	var hb HandlerBroker
	hb.TopicReaderFunc = func(topicID, index uint64, streaming bool) interface {
		io.ReadCloser
		io.Seeker
	} {
		if topicID != 2000 {
			t.Fatalf("unexpected topic id: %d", topicID)
		} else if index != 10 {
			t.Fatalf("unexpected index: %d", index)
		} else if !streaming {
			t.Fatalf("unexpected streaming value: %v", streaming)
		}

		// Return a reader with one message.
		var buf bytes.Buffer
		(&messaging.Message{Index: 10, Data: []byte{0, 0, 0, 0}}).WriteTo(&buf)

		return &nopSeekCloser{&buf}
	}
	s := httptest.NewServer(&messaging.Handler{Broker: &hb})
	defer s.Close()

	// Send request to stream the replica.
	resp, err := http.Get(s.URL + `/messaging/messages?topicID=2000&index=10&streaming=true`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d: %s", resp.StatusCode, resp.Header.Get("X-Broker-Error"))
	}
	defer resp.Body.Close()

	// Decode from body.
	var m messaging.Message
	if err := messaging.NewMessageDecoder(&nopSeeker{resp.Body}).Decode(&m); err != nil {
		t.Fatalf("message decode error: %s", err)
	} else if !reflect.DeepEqual(&m, &messaging.Message{Index: 10, Data: []byte{0, 0, 0, 0}}) {
		t.Fatalf("unexpected message: %#v", &m)
	}
}

// Ensure a handler returns an error when streaming messages without a topic id.
func TestHandler_getMessages_ErrTopicRequired(t *testing.T) {
	s := httptest.NewServer(&messaging.Handler{})
	defer s.Close()

	// Send request to the broker.
	resp, err := http.Get(s.URL + `/messaging/messages?index=10`)
	if err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "topic required" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
	resp.Body.Close()
}

// Ensure a handler returns an error when streaming messages without an index.
func TestHandler_getMessages_ErrIndexRequired(t *testing.T) {
	s := httptest.NewServer(&messaging.Handler{})
	defer s.Close()

	// Send request to the broker.
	resp, err := http.Get(s.URL + `/messaging/messages?topicID=10`)
	if err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "index required" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
	resp.Body.Close()
}

// Ensure a handler can receive a message.
func TestHandler_postMessages(t *testing.T) {
	var hb HandlerBroker
	hb.PublishFunc = func(m *messaging.Message) (uint64, error) {
		if !reflect.DeepEqual(m, &messaging.Message{Type: 100, TopicID: 200, Data: []byte(`abc`)}) {
			t.Fatalf("unexpected message: %#v", m)
		}
		return 1, nil
	}
	s := httptest.NewServer(&messaging.Handler{Broker: &hb})
	defer s.Close()

	// Send request to the broker.
	resp, err := http.Post(s.URL+`/messaging/messages?type=100&topicID=200`, "application/octet-stream", strings.NewReader(`abc`))
	if err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d: %s", resp.StatusCode, resp.Header.Get("X-Broker-Error"))
	}
	resp.Body.Close()
}

// Ensure a handler returns an error when publishing a message without a type.
func TestHandler_postMessages_ErrMessageTypeRequired(t *testing.T) {
	s := httptest.NewServer(&messaging.Handler{})
	defer s.Close()

	// Send request to the broker.
	resp, err := http.Post(s.URL+`/messaging/messages?topicID=200`, "application/octet-stream", strings.NewReader(`foo`))
	if err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "message type required" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
	resp.Body.Close()
}

// Ensure a handler returns an error when publishing a message without a topic.
func TestHandler_postMessages_ErrTopicRequired(t *testing.T) {
	s := httptest.NewServer(&messaging.Handler{})
	defer s.Close()

	// Send request to the broker.
	resp, err := http.Post(s.URL+`/messaging/messages?type=100`, "application/octet-stream", strings.NewReader(`foo`))
	if err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if resp.Header.Get("X-Broker-Error") != "topic required" {
		t.Fatalf("unexpected error: %s", resp.Header.Get("X-Broker-Error"))
	}
	resp.Body.Close()
}

// Ensure an error is returned when requesting a stream with the wrong HTTP method.
func TestHandler_messages_ErrMethodNotAllowed(t *testing.T) {
	s := httptest.NewServer(&messaging.Handler{})
	defer s.Close()

	// Send request to stream the replica.
	resp, err := http.Head(s.URL + `/messaging/messages?topicID=2000&index=10&streaming=true`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// Ensure a handler can receive a heartbeats.
func TestHandler_postHeartbeat(t *testing.T) {
	var hb HandlerBroker
	hb.SetTopicMaxIndexFunc = func(topicID, index uint64, dataURL url.URL) error {
		if topicID != 1 {
			t.Fatalf("unexpected topic id: %d", topicID)
		} else if index != 2 {
			t.Fatalf("unexpected index: %d", index)
		}
		return nil
	}
	s := httptest.NewServer(&messaging.Handler{Broker: &hb})
	defer s.Close()

	// Send request to the broker.
	resp, err := http.Post(s.URL+`/messaging/heartbeat?topicID=1&index=2`, "application/octet-stream", nil)
	if err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d: %s", resp.StatusCode, resp.Header.Get("X-Broker-Error"))
	}
	resp.Body.Close()
}

// Ensure an error is returned when heartbeating with the wrong HTTP method.
func TestHandler_postHeartbeat_ErrMethodNotAllowed(t *testing.T) {
	s := httptest.NewServer(&messaging.Handler{})
	defer s.Close()

	resp, err := http.Head(s.URL + `/messaging/heartbeat`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// Ensure a handler can respond to a ping with the current cluster configuration.
func TestHandler_servePing(t *testing.T) {
	var hb HandlerBroker
	hb.IsLeaderFunc = func() bool { return true }
	hb.URLsFunc = func() []url.URL { return []url.URL{{Host: "hostA"}, {Host: "hostB"}} }
	s := httptest.NewServer(&messaging.Handler{Broker: &hb})
	defer s.Close()

	// Send request to the broker.
	resp, err := http.Post(s.URL+`/messaging/ping`, "application/octet-stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d: %s", resp.StatusCode, resp.Header.Get("X-Broker-Error"))
	} else if b, _ := ioutil.ReadAll(resp.Body); string(b) != `{"urls":["//hostA","//hostB"]}`+"\n" {
		t.Fatalf("unexpected body: %s", b)
	}
}

// Ensure a handler can respond to a ping with the current cluster configuration.
func TestHandler_servePing_NotLeader(t *testing.T) {
	var hb HandlerBroker
	hb.IsLeaderFunc = func() bool { return false }
	hb.LeaderURLFunc = func() url.URL { return url.URL{Scheme: "http", Host: "other"} }
	s := httptest.NewServer(&messaging.Handler{Broker: &hb})
	defer s.Close()

	// Send request to the broker.
	resp, err := http.Post(s.URL+`/messaging/ping`, "application/octet-stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("unexpected status: %d: %s", resp.StatusCode, resp.Header.Get("X-Broker-Error"))
	} else if loc := resp.Header.Get("Location"); loc != "http://other/messaging/ping" {
		t.Fatalf("unexpected redirect location: %s", loc)
	}
}

// Ensure the handler routes raft requests to the raft handler.
func TestHandler_raft(t *testing.T) {
	var h messaging.Handler
	h.RaftHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	s := httptest.NewServer(&h)
	defer s.Close()

	resp, _ := http.Get(s.URL + `/raft/ping`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
}

// Ensure the handler returns an error for an invalid path.
func TestHandler_ErrNotFound(t *testing.T) {
	s := httptest.NewServer(&messaging.Handler{})
	defer s.Close()
	resp, _ := http.Get(s.URL + `/no_such_path`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
}

// HandlerBroker is a mockable type that implements Handler.Broker.
type HandlerBroker struct {
	URLsFunc        func() []url.URL
	IsLeaderFunc    func() bool
	LeaderURLFunc   func() url.URL
	PublishFunc     func(m *messaging.Message) (uint64, error)
	TopicReaderFunc func(topicID, index uint64, streaming bool) interface {
		io.ReadCloser
		io.Seeker
	}
	SetTopicMaxIndexFunc func(topicID, index uint64, dataURL url.URL) error
}

func (b *HandlerBroker) URLs() []url.URL                              { return b.URLsFunc() }
func (b *HandlerBroker) IsLeader() bool                               { return b.IsLeaderFunc() }
func (b *HandlerBroker) LeaderURL() url.URL                           { return b.LeaderURLFunc() }
func (b *HandlerBroker) Publish(m *messaging.Message) (uint64, error) { return b.PublishFunc(m) }
func (b *HandlerBroker) TopicReader(topicID, index uint64, streaming bool) interface {
	io.ReadCloser
	io.Seeker
} {
	return b.TopicReaderFunc(topicID, index, streaming)
}
func (b *HandlerBroker) SetTopicMaxIndex(topicID, index uint64, dataURL url.URL) error {
	return b.SetTopicMaxIndexFunc(topicID, index, dataURL)
}
func (b *HandlerBroker) DataURLsForTopic(id, index uint64) []url.URL {
	return nil
}

// MustParseURL parses a string into a URL. Panic on error.
func MustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err.Error())
	}
	return u
}

type nopSeeker struct {
	io.Reader
}

func (*nopSeeker) Seek(offset int64, whence int) (int64, error) { return 0, nil }

type nopSeekCloser struct {
	io.Reader
}

func (*nopSeekCloser) Seek(offset int64, whence int) (int64, error) { return 0, nil }
func (*nopSeekCloser) Close() error                                 { return nil }
