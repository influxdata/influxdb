package messaging_test

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/influxdb/influxdb/messaging"
)

// Ensure a client can be opened and connections can be created.
func TestClient_Conn(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Query().Get("topicID") {
		case "1":
			(&messaging.Message{Index: 1, Data: []byte{100}}).WriteTo(w)
		case "2":
			(&messaging.Message{Index: 2, Data: []byte{200}}).WriteTo(w)
		}
	}))
	defer s.Close()

	// Create and open connection to server.
	c := messaging.NewClient()
	if err := c.Open("", []url.URL{*MustParseURL(s.URL)}); err != nil {
		t.Fatal(err)
	}

	// Connect on topic #1.
	conn1 := c.Conn(1)
	if err := conn1.Open(0); err != nil {
		t.Fatal(err)
	} else if m := <-conn1.C(); !reflect.DeepEqual(m, &messaging.Message{Index: 1, Data: []byte{100}}) {
		t.Fatalf("unexpected message(1): %#v", m)
	}

	// Connect on topic #2.
	conn2 := c.Conn(2)
	if err := conn2.Open(0); err != nil {
		t.Fatal(err)
	} else if m := <-conn2.C(); !reflect.DeepEqual(m, &messaging.Message{Index: 2, Data: []byte{200}}) {
		t.Fatalf("unexpected message(2): %#v", m)
	}

	// Close client and all connections.
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

// Ensure that an error is returned when opening an opened connection.
func TestConn_Open_ErrConnOpen(t *testing.T) {
	c := messaging.NewConn(1)
	c.Open(0)
	defer c.Close()
	if err := c.Open(0); err != messaging.ErrConnOpen {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that an error is returned when opening a previously closed connection.
func TestConn_Open_ErrConnCannotReuse(t *testing.T) {
	c := messaging.NewConn(1)
	c.Open(0)
	c.Close()
	if err := c.Open(0); err != messaging.ErrConnCannotReuse {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that an error is returned when closing a closed connection.
func TestConn_Close_ErrConnClosed(t *testing.T) {
	c := messaging.NewConn(1)
	c.Open(0)
	c.Close()
	if err := c.Close(); err != messaging.ErrConnClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that a connection can connect and stream from a broker.
func TestConn_Open(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Verify incoming parameters.
		if req.URL.Path != "/messaging/messages" {
			t.Fatalf("unexpected path: %s", req.URL.Path)
		} else if topicID := req.URL.Query().Get("topicID"); topicID != "100" {
			t.Fatalf("unexpected topic id: %s", topicID)
		} else if index := req.URL.Query().Get("index"); index != "200" {
			t.Fatalf("unexpected index: %s", index)
		}

		// Stream out messages.
		(&messaging.Message{Index: 1, Data: []byte{100}}).WriteTo(w)
		(&messaging.Message{Index: 2, Data: []byte{200}}).WriteTo(w)
	}))
	defer s.Close()

	// Create and open connection to server.
	c := messaging.NewConn(100)
	c.SetURL(*MustParseURL(s.URL))
	if err := c.Open(200); err != nil {
		t.Fatal(err)
	}

	// Receive messages from the stream.
	if m := <-c.C(); !reflect.DeepEqual(m, &messaging.Message{Index: 1, Data: []byte{100}}) {
		t.Fatalf("unexpected message(0): %#v", m)
	}
	if m := <-c.C(); !reflect.DeepEqual(m, &messaging.Message{Index: 2, Data: []byte{200}}) {
		t.Fatalf("unexpected message(1): %#v", m)
	}

	// Close connection.
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a connection can reconnect.
func TestConn_Open_Reconnect(t *testing.T) {
	var requestN int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Error the first time.
		if requestN == 0 {
			requestN++
			http.Error(w, "OH NO", http.StatusInternalServerError)
			return
		}

		// Write a message the second time.
		(&messaging.Message{Index: 1, Data: []byte{100}}).WriteTo(w)
	}))
	defer s.Close()

	// Create and open connection to server.
	c := messaging.NewConn(100)
	c.SetURL(*MustParseURL(s.URL))
	if err := c.Open(0); err != nil {
		t.Fatal(err)
	}

	// Receive messages from the stream.
	if m := <-c.C(); !reflect.DeepEqual(m, &messaging.Message{Index: 1, Data: []byte{100}}) {
		t.Fatalf("unexpected message(0): %#v", m)
	}

	// Close connection.
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a connection can heartbeat to the broker.
func TestConn_Heartbeat(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Verify incoming parameters.
		if req.Method != "POST" {
			t.Fatalf("unexpected method: %s", req.Method)
		} else if req.URL.Path != "/messaging/heartbeat" {
			t.Fatalf("unexpected path: %s", req.URL.Path)
		} else if topicID := req.URL.Query().Get("topicID"); topicID != "100" {
			t.Fatalf("unexpected topic id: %s", topicID)
		} else if index := req.URL.Query().Get("index"); index != "200" {
			t.Fatalf("unexpected index: %s", index)
		}
	}))
	defer s.Close()

	// Create connection and heartbeat.
	c := messaging.NewConn(100)
	c.SetURL(*MustParseURL(s.URL))
	c.SetIndex(200)
	if err := c.Heartbeat(); err != nil {
		t.Fatal(err)
	}
}

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

// Client represents a test wrapper for the broker client.
type Client struct {
	*messaging.Client
}

// NewClient returns a new instance of Client.
func NewClient(replicaID uint64) *Client {
	return &Client{
		Client: messaging.NewClient(),
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
