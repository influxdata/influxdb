package messaging_test

import (
	"io/ioutil"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/messaging"
)

// Ensure the client replica id can be retrieved.
func TestClient_ReplicaID(t *testing.T) {
	c := NewClient(1000)
	defer c.Close()
	if replicaID := c.ReplicaID(); replicaID != 1000 {
		t.Fatalf("unexpected replica id: %d", replicaID)
	}
}

// Ensure that a client can open a connect to the broker.
func TestClient_Open(t *testing.T) {
	c := NewClient(1000)
	defer c.Close()

	// Create replica on broker.
	c.Server.Handler.Broker().CreateReplica(1000)

	// Open client to broker.
	f := NewTempFile()
	defer os.Remove(f)
	u, _ := url.Parse(c.Server.URL)
	if err := c.Open(f, []*url.URL{u}); err != nil {
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
	c := NewClient(1000)
	defer c.Close()

	// Open client to broker.
	f := NewTempFile()
	defer os.Remove(f)
	u, _ := url.Parse(c.Server.URL)
	c.Open(f, []*url.URL{u})
	if err := c.Open(f, []*url.URL{u}); err != messaging.ErrClientOpen {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that opening a client without a broker URL returns an error.
func TestClient_Open_ErrBrokerURLRequired(t *testing.T) {
	t.Skip()
	c := NewClient(1000)
	defer c.Close()
	f := NewTempFile()
	defer os.Remove(f)
	if err := c.Open(f, []*url.URL{}); err != messaging.ErrBrokerURLRequired {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that a client can close while a message is pending.
func TestClient_Close(t *testing.T) {
	c := NewClient(1000)
	defer c.Close()

	// Create replica on broker.
	c.Server.Handler.Broker().CreateReplica(1000)

	// Open client to broker.
	f := NewTempFile()
	defer os.Remove(f)
	u, _ := url.Parse(c.Server.URL)
	if err := c.Open(f, []*url.URL{u}); err != nil {
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
	c := OpenClient(1000)
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
	c := OpenClient(1000)
	c.Server.Close()
	defer c.Close()

	// Publish message to the broker.
	if _, err := c.Publish(&messaging.Message{Type: 100, TopicID: 0, Data: []byte{0}}); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Ensure that a client receives an error when publishing to a closed broker.
func TestClient_Publish_ErrLogClosed(t *testing.T) {
	c := OpenClient(1000)
	c.Server.Handler.Broker().Close()
	defer c.Close()

	// Publish message to the broker.
	if _, err := c.Publish(&messaging.Message{Type: 100, TopicID: 0, Data: []byte{0}}); err == nil || err.Error() != "log closed" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Ensure that a client can create a replica.
func TestClient_CreateReplica(t *testing.T) {
	c := OpenClient(0)
	defer c.Close()

	// Create replica through client.
	if err := c.CreateReplica(123); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify replica was created.
	if r := c.Server.Handler.Broker().Replica(123); r == nil {
		t.Fatalf("replica not created")
	}
}

// Ensure that a client can passthrough an error while creating a replica.
func TestClient_CreateReplica_Err(t *testing.T) {
	c := OpenClient(0)
	defer c.Close()
	c.Server.Handler.Broker().CreateReplica(123)
	if err := c.CreateReplica(123); err == nil || err.Error() != `replica already exists` {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Ensure that a client can delete a replica.
func TestClient_DeleteReplica(t *testing.T) {
	c := OpenClient(0)
	defer c.Close()
	c.Server.Handler.Broker().CreateReplica(123)

	// Delete replica through client.
	if err := c.DeleteReplica(123); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify replica was deleted.
	if r := c.Server.Handler.Broker().Replica(123); r != nil {
		t.Fatalf("replica not deleted")
	}
}

// Ensure that a client can create a subscription.
func TestClient_Subscribe(t *testing.T) {
	c := OpenClient(0)
	defer c.Close()
	c.Server.Broker().CreateReplica(100)

	// Create subscription through client.
	if err := c.Subscribe(100, 200); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify subscription was created.
	if a := c.Server.Handler.Broker().Replica(100).Topics(); !reflect.DeepEqual([]uint64{0, 200}, a) {
		t.Fatalf("topics mismatch: %v", a)
	}
}

// Ensure that a client can passthrough an error while creating a subscription.
func TestClient_Subscribe_Err(t *testing.T) {
	c := OpenClient(0)
	defer c.Close()
	if err := c.Subscribe(123, 100); err == nil || err.Error() != `replica not found` {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Ensure that a client can remove a subscription.
func TestClient_Unsubscribe(t *testing.T) {
	c := OpenClient(0)
	defer c.Close()
	c.Server.Broker().CreateReplica(100)
	c.Server.Broker().Subscribe(100, 200)

	// Remove subscription through client.
	if err := c.Unsubscribe(100, 200); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify subscription was removed.
	if a := c.Server.Handler.Broker().Replica(100).Topics(); !reflect.DeepEqual([]uint64{0}, a) {
		t.Fatalf("topics mismatch: %v", a)
	}
}

// Ensure that a client can passthrough an error while removing a subscription.
func TestClient_Unsubscribe_Err(t *testing.T) {
	c := OpenClient(0)
	defer c.Close()
	if err := c.Unsubscribe(123, 100); err == nil || err.Error() != `replica not found` {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Client represents a test wrapper for the broker client.
type Client struct {
	clientConfig string // Temporary file for client config.
	*messaging.Client
	Server *Server // test server
}

// NewClient returns a new instance of Client.
func NewClient(replicaID uint64) *Client {
	return &Client{
		clientConfig: "", // Not all tests with NewClient require automatic temp file creation.
		Client:       messaging.NewClient(replicaID),
		Server:       NewServer(),
	}
}

// OpenClient returns a new, open instance of Client.
func OpenClient(replicaID uint64) *Client {
	c := NewClient(replicaID)
	c.Server.Handler.Broker().CreateReplica(replicaID)

	// Open client to broker.
	c.clientConfig = NewTempFile()
	u, _ := url.Parse(c.Server.URL)
	if err := c.Open(c.clientConfig, []*url.URL{u}); err != nil {
		panic(err)
	}
	time.Sleep(10 * time.Millisecond)

	return c
}

// Close shuts down the client and server.
func (c *Client) Close() {
	c.Client.Close()
	if c.clientConfig != "" {
		os.Remove(c.clientConfig)
	}
	c.Server.Close()
}

// NewTempFile returns the path of a new temporary file.
// It is up to the caller to remove it when finished.
func NewTempFile() string {
	f, err := ioutil.TempFile("", "influxdb-client-test")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	return f.Name()
}
