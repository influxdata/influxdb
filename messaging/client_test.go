package messaging_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/influxdb/influxdb/messaging"
)

var testDataURL *url.URL

func init() {
	testDataURL, _ = url.Parse("http://localhost:1234/data")
}

func is_connection_refused(err error) bool {
	return strings.Contains(err.Error(), `connection refused`) || strings.Contains(err.Error(), `No connection could be made`)
}

// Ensure a client can open the configuration file, if it exists.
func TestClient_Open_WithConfig(t *testing.T) {
	// Write configuration file.
	path := NewTempFile()
	defer os.Remove(path)
	MustWriteFile(path, []byte(`{"urls":["//hostA"]}`))

	// Open new client against path.
	c := NewClient()
	if err := c.Open(path); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer c.Close()

	// Verify that urls were populated.
	if a := c.URLs(); !reflect.DeepEqual(a, []url.URL{{Host: "hostA"}}) {
		t.Fatalf("unexpected urls: %#v", a)
	}
}

// Ensure a client will ignore non-existent a config file.
func TestClient_Open_WithMissingConfig(t *testing.T) {
	path := NewTempFile()
	c := NewClient()
	c.SetURLs([]url.URL{{Host: "hostA"}})
	if err := c.Open(path); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer c.Close()

	// Verify that urls were cleared.
	if a := c.URLs(); len(a) != 1 {
		t.Fatalf("unexpected urls: exp 1, got: %d", len(a))
	}

	if exp, got := "//hostA", c.URLs()[0].String(); exp != got {
		t.Fatalf("unexpected urls: exp %q, got: %v", got, exp)
	}

}

// Ensure a client can return an error if the configuration file is corrupt.
func TestClient_Open_WithInvalidConfig(t *testing.T) {
	// Write bad configuration file.
	path := NewTempFile()
	defer os.Remove(path)
	MustWriteFile(path, []byte(`{"urls":`))

	// Open new client against path.
	c := NewClient()
	if err := c.Open(path); err == nil || err.Error() != `load config: decode config: unexpected EOF` {
		t.Fatalf("unexpected error: %s", err)
	}
	defer c.Close()
}

// Ensure a client can return an error if the configuration file has non-readable permissions.
func TestClient_Open_WithBadPermConfig(t *testing.T) {
	if "windows" == runtime.GOOS {
		t.Skip("skip it on the windows")
	}
	// Write inaccessible configuration file.
	path := NewTempFile()
	defer os.Remove(path)
	MustWriteFile(path, []byte(`{"urls":["//hostA"]}`))
	os.Chmod(path, 0000)

	// Open new client against path.
	c := NewClient()
	if err := c.Open(path); err == nil || !strings.Contains(err.Error(), `permission denied`) {
		t.Fatalf("unexpected error: %v", err)
	}
	defer c.Close()
}

// Ensure a client returns an error when reopening.
func TestClient_Open_ErrClientOpen(t *testing.T) {
	c := NewClient()
	c.Open("")
	defer c.Close()
	if err := c.Open(""); err != messaging.ErrClientOpen {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the URL on a client can be set and retrieved.
func TestClient_SetURL(t *testing.T) {
	c := NewClient()
	defer c.Close()

	c.SetURL(url.URL{Host: "localhost"})
	if u := c.URL(); u != (url.URL{Host: "localhost"}) {
		t.Fatalf("unexpected url: %s", u)
	}
}

// Ensure a client will update its connection urls.
func TestClient_SetURL_UpdateConn(t *testing.T) {
	c := NewClient()
	c.MustOpen("")
	c.SetURLs([]url.URL{{Host: "hostA"}})
	defer c.Close()

	// Create connection & check URL.
	conn := c.Conn(0)
	if u := conn.URL(); u != (url.URL{Host: "hostA"}) {
		t.Fatalf("unexpected initial connection url: %s", u)
	}

	// Update client url.
	c.SetURL(url.URL{Host: "hostB"})

	// Check that connection url was updated.
	if u := conn.URL(); u != (url.URL{Host: "hostB"}) {
		t.Fatalf("unexpected new connection url: %s", u)
	}
}

// Ensure a set of URLs can be set on the client and retrieved.
// One of those URLs should be randomly set as the current URL.
func TestClient_SetURLs(t *testing.T) {
	c := NewClient()
	defer c.Close()

	// Set and retrieve URLs.
	c.SetURLs([]url.URL{{Host: "hostA"}, {Host: "hostB"}})
	if a := c.URLs(); a[0] != (url.URL{Host: "hostA"}) {
		t.Fatalf("unexpected urls length: %d", len(a))
	} else if a := c.URLs(); a[0] != (url.URL{Host: "hostA"}) {
		t.Fatalf("unexpected url(0): %s", a[0])
	} else if a := c.URLs(); a[1] != (url.URL{Host: "hostB"}) {
		t.Fatalf("unexpected url(1): %s", a[1])
	}

	// Current URL should be one of the URLs set.
	if u := c.URL(); u != (url.URL{Host: "hostA"}) && u != (url.URL{Host: "hostB"}) {
		t.Fatalf("unexpected url: %s", u)
	}
}

// Ensure that an empty set of URLs can be set to the client.
func TestClient_SetURLs_NoURLs(t *testing.T) {
	c := NewClient()
	defer c.Close()
	c.SetURLs([]url.URL{})
}

// Ensure a client can publish a message to the broker.
func TestClient_Publish(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/messaging/messages" {
			t.Fatalf("unexpected path: %s", req.URL.Path)
		} else if req.Method != "POST" {
			t.Fatalf("unexpected method: %s", req.Method)
		} else if typ := req.URL.Query().Get("type"); typ != "1" {
			t.Fatalf("unexpected type: %s", typ)
		} else if topicID := req.URL.Query().Get("topicID"); topicID != "2" {
			t.Fatalf("unexpected topicID: %s", topicID)
		}

		w.Header().Set("X-Broker-Index", "200")
	}))
	defer s.Close()

	// Create client.
	c := NewClient()
	c.MustOpen("")
	c.SetURL(*MustParseURL(s.URL))
	defer c.Close()

	// Publish message to server.
	if index, err := c.Publish(&messaging.Message{Type: 1, TopicID: 2, Data: []byte{0, 0, 0, 0}}); err != nil {
		t.Fatal(err)
	} else if index != 200 {
		t.Fatalf("unexpected index: %d", index)
	}
}

// Ensure a client can redirect a published a message to another broker.
func TestClient_Publish_Redirect(t *testing.T) {
	// Create a server to receive redirection.
	s0 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/messaging/messages" {
			t.Fatalf("unexpected path: %s", req.URL.Path)
		} else if req.Method != "POST" {
			t.Fatalf("unexpected method: %s", req.Method)
		} else if typ := req.URL.Query().Get("type"); typ != "1" {
			t.Fatalf("unexpected type: %s", typ)
		} else if topicID := req.URL.Query().Get("topicID"); topicID != "2" {
			t.Fatalf("unexpected topicID: %s", topicID)
		}

		w.Header().Set("X-Broker-Index", "200")
	}))
	defer s0.Close()

	// Create another server to redirect to the first one.
	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, s0.URL+req.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer s1.Close()

	// Create client.
	c := NewClient()
	c.MustOpen("")
	c.SetURL(*MustParseURL(s1.URL))
	defer c.Close()

	// Publish message to server.
	if index, err := c.Publish(&messaging.Message{Type: 1, TopicID: 2, Data: []byte{0, 0, 0, 0}}); err != nil {
		t.Fatal(err)
	} else if index != 200 {
		t.Fatalf("unexpected index: %d", index)
	}
}

// Ensure a client returns an error if the responses Location header is invalid.
func TestClient_Publish_Redirect_ErrInvalidLocation(t *testing.T) {
	// Create another server to redirect to the first one.
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, "http://%f", http.StatusTemporaryRedirect)
	}))
	defer s.Close()

	// Create client.
	c := NewClient()
	c.MustOpen("")
	c.SetURL(*MustParseURL(s.URL))
	defer c.Close()

	// Publish message to server.
	if _, err := c.Publish(&messaging.Message{}); err == nil || err.Error() != `do: invalid redirect location: http://%f` {
		t.Fatal(err)
	}
}

// Ensure a client returns an error publishing to a down broker.
func TestClient_Publish_ErrConnectionRefused(t *testing.T) {
	s := httptest.NewServer(nil)
	s.Close()

	// Create client.
	c := NewClient()
	c.MustOpen("")
	c.SetURL(*MustParseURL(s.URL))
	defer c.Close()

	// Publish message to server.
	if _, err := c.Publish(&messaging.Message{}); err == nil || !is_connection_refused(err) {
		t.Fatal(err)
	}
}

// Ensure a client returns an error if returned by the server.
func TestClient_Publish_ErrBrokerError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("X-Broker-Error", "oh no")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer s.Close()

	// Create client.
	c := NewClient()
	c.MustOpen("")
	c.SetURL(*MustParseURL(s.URL))
	defer c.Close()

	// Publish message to server.
	if _, err := c.Publish(&messaging.Message{}); err == nil || err.Error() != `oh no` {
		t.Fatal(err)
	}
}

// Ensure a client returns an error if a non-broker error occurs.
func TestClient_Publish_ErrHTTPError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer s.Close()

	// Create client.
	c := NewClient()
	c.MustOpen("")
	c.SetURL(*MustParseURL(s.URL))
	defer c.Close()

	// Publish message to server.
	if _, err := c.Publish(&messaging.Message{}); err == nil || err.Error() != `cannot publish: status=500` {
		t.Fatal(err)
	}
}

// Ensure a client returns an error if the returned index is invalid.
func TestClient_Publish_ErrInvalidIndex(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("X-Broker-Index", "xxx")

	}))
	defer s.Close()

	// Create client.
	c := NewClient()
	c.MustOpen("")
	c.SetURL(*MustParseURL(s.URL))
	defer c.Close()

	// Publish message to server.
	if _, err := c.Publish(&messaging.Message{}); err == nil || err.Error() != `invalid index: strconv.ParseUint: parsing "xxx": invalid syntax` {
		t.Fatal(err)
	}
}

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
	c := NewClient()
	c.MustOpen("")
	c.SetURLs([]url.URL{*MustParseURL(s.URL)})
	defer c.Close()

	// Ping server.
	if err := c.Ping(); err != nil {
		t.Fatal(err)
	} else if !pinged {
		t.Fatal("ping not received")
	}
}

// Ensure a client returns an error if the ping cannot connect to the server.
func TestClient_Ping_ErrConnectionRefused(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {}))
	s.Close()

	// Create client.
	c := NewClient()
	c.MustOpen("")
	c.SetURLs([]url.URL{*MustParseURL(s.URL)})
	defer c.Close()

	// Ping server.
	if err := c.Ping(); err == nil || !is_connection_refused(err) {
		t.Fatal(err)
	}
}

// Ensure a client returns an error if the body of the response cannot be read.
func TestClient_Ping_ErrRead(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Length", "10")
		w.Write(make([]byte, 9))
	}))
	defer s.Close()

	// Create client.
	c := NewClient()
	c.MustOpen("")
	c.SetURLs([]url.URL{*MustParseURL(s.URL)})
	defer c.Close()

	// Ping server.
	if err := c.Ping(); err == nil || err.Error() != `read ping body: unexpected EOF` {
		t.Fatal(err)
	}
}

// Ensure a client can receive config data from the broker on ping.
func TestClient_Ping_ReceiveConfig(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Write([]byte(`{"urls":["//local.dev"]}`))
	}))
	defer s.Close()

	// Create a temp file for configuration.
	path := NewTempFile()
	defer os.Remove(path)

	// Create client.
	c := NewClient()
	c.MustOpen(path)
	c.SetURLs([]url.URL{*MustParseURL(s.URL)})
	defer c.Close()

	// Ping server.
	if err := c.Ping(); err != nil {
		t.Fatal(err)
	}

	// Confirm config change.
	if a := c.URLs(); len(a) != 1 {
		t.Fatalf("unexpected urls length: %d", len(a))
	} else if a[0] != (url.URL{Host: "local.dev"}) {
		t.Fatalf("unexpected url(0): %s", a[0])
	}

	// Confirm config was rewritten.
	if b, _ := ioutil.ReadFile(path); string(b) != `{"urls":["//local.dev"]}`+"\n" {
		t.Fatalf("unexpected config file: %s", b)
	}
}

// Ensure a client returns an error when ping response is invalid.
func TestClient_Ping_ErrInvalidResponse(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Write([]byte(`{"urls":`))
	}))
	defer s.Close()

	// Create client.
	c := NewClient()
	c.MustOpen("")
	c.SetURLs([]url.URL{*MustParseURL(s.URL)})
	defer c.Close()

	// Ping server.
	if err := c.Ping(); err == nil || err.Error() != `unmarshal config: unexpected end of JSON input` {
		t.Fatal(err)
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
	c := NewClient()
	c.MustOpen("")
	c.SetURLs([]url.URL{*MustParseURL(s.URL)})

	// Connect on topic #1.
	conn1 := c.Conn(1)
	if err := conn1.Open(0, false); err != nil {
		t.Fatal(err)
	} else if conn1.TopicID() != 1 {
		t.Fatalf("unexpected topic id(1): %d", conn1.TopicID())
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
	c := messaging.NewConn(1, testDataURL)
	c.Open(0, false)
	defer c.Close()
	if err := c.Open(0, false); err != messaging.ErrConnOpen {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that an error is returned when opening a previously closed connection.
func TestConn_Open_ErrConnCannotReuse(t *testing.T) {
	c := messaging.NewConn(1, testDataURL)
	c.Open(0, false)
	c.Close()
	if err := c.Open(0, false); err != messaging.ErrConnCannotReuse {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that an error is returned when closing a closed connection.
func TestConn_Close_ErrConnClosed(t *testing.T) {
	c := messaging.NewConn(1, testDataURL)
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
	c := messaging.NewConn(100, testDataURL)
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
	c := messaging.NewConn(100, testDataURL)
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
		} else if url := req.URL.Query().Get("url"); url != "http://localhost:1234/data" {
			t.Fatalf("unexpected url: %s, got %s", "http://localhost:1234/data", url)
		}
	}))
	defer s.Close()

	// Create connection and heartbeat.
	c := messaging.NewConn(100, testDataURL)
	c.SetURL(*MustParseURL(s.URL))
	c.SetIndex(200)
	if err := c.Heartbeat(); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a connection returns an error if it cannot connect to the broker.
func TestConn_Heartbeat_ErrConnectionRefused(t *testing.T) {
	s := httptest.NewServer(nil)
	s.Close()

	// Create connection and heartbeat.
	c := messaging.NewConn(0, testDataURL)
	c.SetURL(*MustParseURL(s.URL))
	if err := c.Heartbeat(); err == nil || !is_connection_refused(err) {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that a connection returns an error if the heartbeat is redirected.
// This occurs when the broker is not the leader. The client will update the URL later.
func TestConn_Heartbeat_ErrNoLeader(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer s.Close()

	// Create connection and heartbeat.
	c := messaging.NewConn(0, testDataURL)
	c.SetURL(*MustParseURL(s.URL))
	if err := c.Heartbeat(); err != messaging.ErrNoLeader {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that a connection returns a broker error while heartbeating.
func TestConn_Heartbeat_ErrBrokerError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("X-Broker-Error", "oh no")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer s.Close()

	// Create connection and heartbeat.
	c := messaging.NewConn(0, testDataURL)
	c.SetURL(*MustParseURL(s.URL))
	if err := c.Heartbeat(); err == nil || err.Error() != `oh no` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that a connection returns an http error while heartbeating.
func TestConn_Heartbeat_ErrHTTPError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer s.Close()

	// Create connection and heartbeat.
	c := messaging.NewConn(0, testDataURL)
	c.SetURL(*MustParseURL(s.URL))
	if err := c.Heartbeat(); err == nil || err.Error() != `heartbeat error: status=500` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that the client config can be serialized to JSON.
func TestClientConfig_MarshalJSON(t *testing.T) {
	c := messaging.ClientConfig{URLs: []url.URL{{Host: "hostA"}, {Host: "hostB"}}}
	if b, err := json.Marshal(&c); err != nil {
		t.Fatal(err)
	} else if string(b) != `{"urls":["//hostA","//hostB"]}` {
		t.Fatalf("unexpected json: %s", b)
	}
}

// Ensure that the client config can be deserialized from JSON.
func TestClientConfig_UnmarshalJSON(t *testing.T) {
	var c messaging.ClientConfig
	if err := json.Unmarshal([]byte(`{"urls":["//hostA","//hostB"]}`), &c); err != nil {
		t.Fatal(err)
	}
	if len(c.URLs) != 2 {
		t.Fatalf("unexpected url count: %d", len(c.URLs))
	} else if c.URLs[0] != (url.URL{Host: "hostA"}) {
		t.Fatalf("unexpected url(0): %s", c.URLs[0])
	} else if c.URLs[1] != (url.URL{Host: "hostB"}) {
		t.Fatalf("unexpected url(1): %s", c.URLs[1])
	}
}

// Ensure that the client config returns an error when handling an invalid field type.
func TestClientConfig_UnmarshalJSON_ErrInvalidType(t *testing.T) {
	var c messaging.ClientConfig
	if err := json.Unmarshal([]byte(`{"urls":0}`), &c); err == nil || err.Error() != `json: cannot unmarshal number into Go value of type []string` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that the client config returns an error when handling an invalid url.
func TestClientConfig_UnmarshalJSON_ErrInvalidURL(t *testing.T) {
	var c messaging.ClientConfig
	if err := json.Unmarshal([]byte(`{"urls":["http://%foo"]}`), &c); err == nil || err.Error() != `parse http://%foo: hexadecimal escape in host` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Client represents a test wrapper for messaging.Client.
type Client struct {
	*messaging.Client
}

// NewClient returns an new instance of Client.
func NewClient() *Client {
	return &Client{messaging.NewClient(*testDataURL)}
}

// MustOpen opens the client. Panic on error.
func (c *Client) MustOpen(path string) {
	if err := c.Open(path); err != nil {
		panic(err.Error())
	}
}

// NewTempFile returns the path of a new temporary file.
// It is up to the caller to remove it when finished.
func NewTempFile() string {
	f, err := ioutil.TempFile("", "influxdb-client-test")
	if err != nil {
		panic(err)
	}
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}
