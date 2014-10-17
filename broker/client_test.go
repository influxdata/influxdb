package broker_test

import (
	"net/url"
	"testing"
	"time"

	"github.com/influxdb/influxdb/broker"
)

// Ensure the client name can be retrieved.
func TestClient_Name(t *testing.T) {
	c := NewClient("node0")
	defer c.Close()
	if name := c.Name(); name != "node0" {
		t.Fatalf("unexpected name: %s", name)
	}
}

// Ensure that a client can open a connect to the broker.
func TestClient_Open(t *testing.T) {
	c := NewClient("node0")
	defer c.Close()

	// Create replica on broker.
	c.Handler.Broker.CreateReplica("node0")

	// Open client to broker.
	u, _ := url.Parse(c.Handler.HTTPServer.URL)
	if err := c.Open([]*url.URL{u}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Receive a message from the stream.
	if m := <-c.C; m.Type != broker.CreateReplicaMessageType {
		t.Fatalf("unexpected message type: %x", m.Type)
	}

	// Close connection to the broker.
	if err := c.Client.Close(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that opening an already open client returns an error.
func TestClient_Open_ErrClientOpen(t *testing.T) {
	c := NewClient("node0")
	defer c.Close()

	// Open client to broker.
	u, _ := url.Parse(c.Handler.HTTPServer.URL)
	c.Open([]*url.URL{u})
	if err := c.Open([]*url.URL{u}); err != broker.ErrClientOpen {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that opening a client without a broker URL returns an error.
func TestClient_Open_ErrBrokerURLRequired(t *testing.T) {
	c := NewClient("node0")
	defer c.Close()
	if err := c.Open([]*url.URL{}); err != broker.ErrBrokerURLRequired {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that a client can close while a message is pending.
func TestClient_Close(t *testing.T) {
	c := NewClient("node0")
	defer c.Close()

	// Create replica on broker.
	c.Handler.Broker.CreateReplica("node0")

	// Open client to broker.
	u, _ := url.Parse(c.Handler.HTTPServer.URL)
	if err := c.Open([]*url.URL{u}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Close connection to the broker.
	if err := c.Client.Close(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Client represents a test wrapper for the broker client.
type Client struct {
	*broker.Client
	Handler *Handler // test handler
}

// NewClient returns a new instance of Client.
func NewClient(name string) *Client {
	c := &Client{
		Client:  broker.NewClient(name),
		Handler: NewHandler(),
	}
	return c
}

// Close shutsdown the test handler.
func (c *Client) Close() {
	c.Client.Close()
	c.Handler.Close()
}
