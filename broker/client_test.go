package broker_test

import (
	"net/url"
	"testing"

	"github.com/influxdb/influxdb/broker"
)

// Ensure that a client can open a connect to the broker.
func TestClient_Open(t *testing.T) {
	c := NewClient("node0")
	defer c.Close()

	// Create replica on broker.
	b := c.Handler.Broker
	ok(b.CreateReplica("node0"))

	// Open client to broker.
	u, _ := url.Parse(c.Handler.HTTPServer.URL)
	if err := c.Open([]*url.URL{u}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Receive a set of messages from the stream.
	if m := <-c.C; m.Type != broker.CreateReplicaMessageType {
		t.Fatalf("unexpected message type: %x", m.Type)
	}

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
