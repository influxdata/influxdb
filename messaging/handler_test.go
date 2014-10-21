package messaging_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdb/influxdb/messaging"
)

// Ensure a replica can connect and stream messages.
func TestHandler_serveStream(t *testing.T) {
	h := NewHandler()
	defer h.Close()

	// Create replica.
	h.Broker.CreateReplica("foo")

	// Send request to stream the replica.
	resp, err := http.Get(h.HTTPServer.URL + `/stream?name=foo`)
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
func TestHandler_serveStream_ErrReplicaNameRequired(t *testing.T) {
	h := NewHandler()
	defer h.Close()

	resp, _ := http.Get(h.HTTPServer.URL + `/stream`)
	defer resp.Body.Close()
	if msg := resp.Header.Get("X-Broker-Error"); resp.StatusCode != http.StatusBadRequest || msg != "replica name required" {
		t.Fatalf("unexpected status/error: %d/%s", resp.StatusCode, msg)
	}
}

// Ensure an error is returned when requesting a stream for a non-existent replica.
func TestHandler_serveStream_ErrReplicaNotFound(t *testing.T) {
	h := NewHandler()
	defer h.Close()

	resp, _ := http.Get(h.HTTPServer.URL + `/stream?name=no_such_replica`)
	defer resp.Body.Close()
	if msg := resp.Header.Get("X-Broker-Error"); resp.StatusCode != http.StatusNotFound || msg != "replica not found" {
		t.Fatalf("unexpected status/error: %d/%s", resp.StatusCode, msg)
	}
}

// Handler is a test wrapper for messaging.Handler.
type Handler struct {
	*messaging.Handler
	Broker     *Broker
	HTTPServer *httptest.Server
}

// NewHandler returns a test handler.
func NewHandler() *Handler {
	b := NewBroker()
	h := &Handler{
		Handler: messaging.NewHandler(b.Broker),
		Broker:  b,
	}
	h.HTTPServer = httptest.NewServer(h.Handler)
	return h
}

// Close stops the server and broker and removes all temp data.
func (h *Handler) Close() {
	h.Broker.Close()
	h.HTTPServer.Close()
}
