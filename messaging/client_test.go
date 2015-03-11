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

// Ensure a client can check if the server is alive.
func TestClient_Ping(t *testing.T) {
	var pinged bool
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/messaging/ping" {
			t.Fatalf("unexpected path: %s", req.URL.Path)
		}
		pinged = true
	}))
	defer s.Close()

	// Create client.
	c := messaging.NewClient()
	if err := c.Open("", []url.URL{*MustParseURL(s.URL)}); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Ping server.
	if err := c.Ping(); err != nil {
		t.Fatal(err)
	} else if !pinged {
		t.Fatal("ping not received")
	}
}

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
	if err := conn1.Open(0, false); err != nil {
		t.Fatal(err)
	} else if m := <-conn1.C(); !reflect.DeepEqual(m, &messaging.Message{Index: 1, Data: []byte{100}}) {
		t.Fatalf("unexpected message(1): %#v", m)
	}

	// Connect on topic #2.
	conn2 := c.Conn(2)
	if err := conn2.Open(0, false); err != nil {
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
	c.Open(0, false)
	defer c.Close()
	if err := c.Open(0, false); err != messaging.ErrConnOpen {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that an error is returned when opening a previously closed connection.
func TestConn_Open_ErrConnCannotReuse(t *testing.T) {
	c := messaging.NewConn(1)
	c.Open(0, false)
	c.Close()
	if err := c.Open(0, false); err != messaging.ErrConnCannotReuse {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that an error is returned when closing a closed connection.
func TestConn_Close_ErrConnClosed(t *testing.T) {
	c := messaging.NewConn(1)
	c.Open(0, false)
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
	if err := c.Open(200, false); err != nil {
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
	if err := c.Open(0, false); err != nil {
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
