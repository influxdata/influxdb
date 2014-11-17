package messaging_test

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/messaging"
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
	c.Server.Handler.Broker().CreateReplica("node0")

	// Open client to broker.
	u, _ := url.Parse(c.Server.URL)
	if err := c.Open([]*url.URL{u}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Receive a message from the stream.
	if m := <-c.C(); m.Type != messaging.CreateReplicaMessageType {
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
	u, _ := url.Parse(c.Server.URL)
	c.Open([]*url.URL{u})
	if err := c.Open([]*url.URL{u}); err != messaging.ErrClientOpen {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that opening a client without a broker URL returns an error.
func TestClient_Open_ErrBrokerURLRequired(t *testing.T) {
	c := NewClient("node0")
	defer c.Close()
	if err := c.Open([]*url.URL{}); err != messaging.ErrBrokerURLRequired {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that a client can close while a message is pending.
func TestClient_Close(t *testing.T) {
	c := NewClient("node0")
	defer c.Close()

	// Create replica on broker.
	c.Server.Handler.Broker().CreateReplica("node0")

	// Open client to broker.
	u, _ := url.Parse(c.Server.URL)
	if err := c.Open([]*url.URL{u}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Close connection to the broker.
	if err := c.Client.Close(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that a client can publish messages to the broker.
func TestClient_Publish(t *testing.T) {
	c := OpenClient("node0")
	defer c.Close()

	// Publish message to the broker.
	if index, err := c.Publish(&messaging.Message{Type: 100, TopicID: messaging.BroadcastTopicID, Data: []byte{0}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if index != 3 {
		t.Fatalf("unexpected index: %d", index)
	}
}

// Ensure that a client receives an error when publishing to a stopped server.
func TestClient_Publish_ErrConnectionRefused(t *testing.T) {
	c := OpenClient("node0")
	c.Server.Close()
	defer c.Close()

	// Publish message to the broker.
	if _, err := c.Publish(&messaging.Message{Type: 100, TopicID: 0, Data: []byte{0}}); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Ensure that a client receives an error when publishing to a closed broker.
func TestClient_Publish_ErrLogClosed(t *testing.T) {
	c := OpenClient("node0")
	c.Server.Handler.Broker().Close()
	defer c.Close()

	// Publish message to the broker.
	if _, err := c.Publish(&messaging.Message{Type: 100, TopicID: 0, Data: []byte{0}}); err == nil || err.Error() != "log closed" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Client represents a test wrapper for the broker client.
type Client struct {
	*messaging.Client
	Server *Server // test server
}

// NewClient returns a new instance of Client.
func NewClient(name string) *Client {
	c := &Client{
		Client: messaging.NewClient(name),
		Server: NewServer(),
	}
	return c
}

// OpenClient returns a new, open instance of Client.
func OpenClient(name string) *Client {
	c := NewClient(name)
	c.Server.Handler.Broker().CreateReplica(name)

	// Open client to broker.
	u, _ := url.Parse(c.Server.URL)
	if err := c.Open([]*url.URL{u}); err != nil {
		panic(err)
	}
	time.Sleep(10 * time.Millisecond)

	return c
}

// Close shuts down the client and server.
func (c *Client) Close() {
	c.Client.Close()
	c.Server.Close()
}
