package messaging_test

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/influxdb/influxdb/messaging"
)

/*
// Ensure that a client can open a connect to the broker.
func TestClient_Open(t *testing.T) {
	c := NewClient()
	defer c.Close()

	// Create replica on broker.
	c.Server.Handler.Broker().PublishSync()

	// Open client to broker.
	f := NewTempFile()
	defer os.Remove(f)
	u, _ := url.Parse(c.Server.URL)
	if err := c.Open(f, []*url.URL{u}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Receive messages from the stream.
	if m := <-c.C(); m.Type != messaging.InternalMessageType {
		t.Fatalf("message type mismatch(internal): %x", m.Type)
	} else if m = <-c.C(); m.Type != messaging.CreateReplicaMessageType {
		t.Fatalf("message type mismatch(create replica): %x", m.Type)
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
	c.Server.Handler.Broker().CreateReplica(1000, &url.URL{Host: "localhost"})

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
*/

// Ensure that a client can heartbeat its status to the broker.
func TestClient_Heartbeat(t *testing.T) { t.Skip("pending") }

// Client represents a test wrapper for the broker client.
type Client struct {
	*messaging.Client
}

// NewClient returns a new instance of Client.
func NewClient(replicaID uint64) *Client {
	return &Client{
		Client: messaging.NewClient(replicaID),
	}
}

// Close shuts down the client and server.
func (c *Client) Close() {
	c.Client.Close()
}

// MustPublish publishes a message. Panic on error.
func (c *Client) MustPublish(m *messaging.Message) uint64 {
	index, err := c.Publish(m)
	if err != nil {
		panic(err.Error())
	}
	return index
}

// Sync waits until the client's index reaches the given index.
func (c *Client) Sync(index uint64) {
	for {
		if c.Index() == index {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
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
