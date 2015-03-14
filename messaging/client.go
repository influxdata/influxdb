package messaging

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	// DefaultReconnectTimeout is the default time to wait between when a broker
	// stream disconnects and another connection is retried.
	DefaultReconnectTimeout = 1000 * time.Millisecond

	// DefaultPingInterval is the default time to wait between checks to the broker.
	DefaultPingInterval = 1000 * time.Millisecond
)

// Client represents a client for the broker's HTTP API.
type Client struct {
	mu    sync.Mutex
	path  string    // config file path
	conns []*Conn   // all connections opened by client
	url   url.URL   // current known leader URL
	urls  []url.URL // list of available broker URLs

	opened bool

	wg      sync.WaitGroup
	closing chan struct{}

	// The amount of time to wait before reconnecting to a broker stream.
	ReconnectTimeout time.Duration

	// The amount of time between pings to verify the broker is alive.
	PingInterval time.Duration

	// The logging interface used by the client for out-of-band errors.
	Logger *log.Logger
}

// NewClient returns a new instance of Client with defaults set.
func NewClient() *Client {
	c := &Client{
		ReconnectTimeout: DefaultReconnectTimeout,
		PingInterval:     DefaultPingInterval,
	}
	c.SetLogOutput(os.Stderr)
	return c
}

// URL returns the current broker leader's URL.
func (c *Client) URL() url.URL {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.url
}

// SetURL sets the current URL to connect to for the client and its connections.
func (c *Client) SetURL(u url.URL) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setURL(u)
}

func (c *Client) setURL(u url.URL) {
	// Set the client URL.
	c.url = u

	// Update all connections.
	for _, conn := range c.conns {
		conn.SetURL(u)
	}
}

// URLs returns a list of possible broker URLs to connect to.
func (c *Client) URLs() []url.URL {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.urls
}

// SetURLs sets a list of possible URLs to connect to for the client and its connections.
func (c *Client) SetURLs(a []url.URL) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setURLs(a)
}

func (c *Client) setURLs(a []url.URL) {
	// Ignore if the URL list is the same.
	if urlsEqual(c.urls, a) {
		return
	}

	c.urls = a
	c.randomizeURL()
}

func (c *Client) randomizeURL() {
	// Clear URL if no brokers exist.
	if len(c.urls) == 0 {
		return
	}

	// Otherwise randomly select a URL.
	c.setURL(c.urls[rand.Intn(len(c.urls))])
}

// SetLogOutput sets writer for all Client log output.
func (c *Client) SetLogOutput(w io.Writer) {
	c.Logger = log.New(w, "[messaging] ", log.LstdFlags)
}

// Open opens the client and reads the configuration from the specified path.
func (c *Client) Open(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Return error if the client is already open.
	if c.opened {
		return ErrClientOpen
	}

	if err := func() error {
		// Read URLs from file if no URLs are provided.
		c.path = path
		if err := c.loadConfig(); err != nil {
			return fmt.Errorf("load config: %s", err)
		}

		// Set open flag.
		c.opened = true

		return nil
	}(); err != nil {
		_ = c.close()
		return err
	}

	// Start background ping.
	c.closing = make(chan struct{}, 0)
	c.wg.Add(1)
	go c.pinger(c.closing)

	return nil
}

// Close disconnects the client from the broker cluster.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.close()
}

func (c *Client) close() error {
	// Return error if the client is already closed.
	if !c.opened {
		return ErrClientClosed
	}

	// Close all connections.
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	c.conns = nil

	// Close goroutines.
	if c.closing != nil {
		close(c.closing)
		c.closing = nil
	}

	// Wait for goroutines to finish.
	c.mu.Unlock()
	c.wg.Wait()
	c.mu.Lock()

	// Unset open flag.
	c.opened = false

	return nil
}

// loadConfig reads the configuration from disk and sets the options on the client.
func (c *Client) loadConfig() error {
	// Open config file for reading.
	f, err := os.Open(c.path)
	if os.IsNotExist(err) {
		c.urls = nil
		return nil
	} else if err != nil {
		return fmt.Errorf("open config: %s", err)
	}
	defer f.Close()

	// Decode config from file.
	var config ClientConfig
	if err := json.NewDecoder(f).Decode(&config); err != nil {
		return fmt.Errorf("decode config: %s", err)
	}

	// Set options.
	c.urls = config.URLs

	return nil
}

// setConfig writes a new config to disk and updates urls on the client.
func (c *Client) setConfig(config ClientConfig) error {
	// Only write to disk if we have a path.
	if c.path != "" {
		// Open config file for writing.
		f, err := os.Create(c.path)
		if err != nil {
			return fmt.Errorf("create: %s", err)
		}
		defer f.Close()

		// Encode config to file.
		if err := json.NewEncoder(f).Encode(&config); err != nil {
			return fmt.Errorf("encode config: %s", err)
		}
	}

	// Set options.
	c.urls = config.URLs

	return nil
}

// Publish sends a message to the broker and returns an index or error.
func (c *Client) Publish(m *Message) (uint64, error) {
	// Post message to broker.
	values := url.Values{
		"type":    {strconv.FormatUint(uint64(m.Type), 10)},
		"topicID": {strconv.FormatUint(m.TopicID, 10)},
	}
	resp, err := c.do("POST", "/messaging/messages", values, "application/octet-stream", m.Data)
	if err != nil {
		return 0, fmt.Errorf("do: %s", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Check response code.
	if resp.StatusCode != http.StatusOK {
		if errstr := resp.Header.Get("X-Broker-Error"); errstr != "" {
			return 0, errors.New(errstr)
		}
		return 0, fmt.Errorf("cannot publish: status=%d", resp.StatusCode)
	}

	// Parse broker index.
	index, err := strconv.ParseUint(resp.Header.Get("X-Broker-Index"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid index: %s", err)
	}

	return index, nil
}

// Ping sends a request to the current broker to check if it is alive.
// If the broker is down then a new URL is tried.
func (c *Client) Ping() error {
	// Post message to broker.
	resp, err := c.do("POST", "/messaging/ping", nil, "application/octet-stream", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read entire body.
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read ping body: %s", err)
	}

	// Update config if body is passed back.
	if len(b) != 0 {
		var config ClientConfig
		if err := json.Unmarshal(b, &config); err != nil {
			return fmt.Errorf("unmarshal config: %s", err)
		}

		if err := c.setConfig(config); err != nil {
			return fmt.Errorf("update config: %s", err)
		}
	}

	return nil
}

// do sends an HTTP request to the given path with the current leader URL.
// This will automatically retry the request if it is redirected.
func (c *Client) do(method, path string, values url.Values, contentType string, body []byte) (*http.Response, error) {
	for {
		// Generate URL.
		u := c.URL()
		u.Path = path
		u.RawQuery = values.Encode()

		// Create request.
		req, err := http.NewRequest(method, u.String(), bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("new request: %s", err)
		}

		// Send HTTP request.
		// If it cannot connect then select a different URL from the config.
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			c.randomizeURL()
			return nil, err
		}

		// If a temporary redirect occurs then update the leader and retry.
		// If a non-200 status is returned then an error occurred.
		if resp.StatusCode == http.StatusTemporaryRedirect {
			redirectURL, err := url.Parse(resp.Header.Get("Location"))
			if err != nil {
				resp.Body.Close()
				return nil, fmt.Errorf("invalid redirect location: %s", resp.Header.Get("Location"))
			}
			c.SetURL(*redirectURL)
			continue
		}

		return resp, nil
	}

}

// Conn returns a connection to the broker for a given topic.
func (c *Client) Conn(topicID uint64) *Conn {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create connection and set current URL.
	conn := NewConn(topicID)
	conn.SetURL(c.url)

	// Add to list of client connections.
	c.conns = append(c.conns, conn)

	return conn
}

// pinger periodically pings the broker to check that it is alive.
func (c *Client) pinger(closing chan struct{}) {
	defer c.wg.Done()

	for {
		select {
		case <-closing:
			return
		case <-time.After(c.PingInterval):
			c.Ping()
		}
	}
}

// ClientConfig represents the configuration that must be persisted across restarts.
type ClientConfig struct {
	URLs []url.URL
}

func (c ClientConfig) MarshalJSON() ([]byte, error) {
	var other clientConfigJSON
	other.URLs = make([]string, len(c.URLs))
	for i, u := range c.URLs {
		other.URLs[i] = u.String()
	}
	return json.Marshal(&other)
}

func (c *ClientConfig) UnmarshalJSON(b []byte) error {
	var other clientConfigJSON
	if err := json.Unmarshal(b, &other); err != nil {
		return err
	}

	c.URLs = make([]url.URL, len(other.URLs))
	for i := range other.URLs {
		u, err := url.Parse(other.URLs[i])
		if err != nil {
			return err
		}
		c.URLs[i] = *u
	}

	return nil
}

// clientConfigJSON represents the JSON
type clientConfigJSON struct {
	URLs []string `json:"urls"`
}

// Conn represents a stream over the client for a single topic.
type Conn struct {
	mu        sync.Mutex
	topicID   uint64  // topic identifier
	index     uint64  // highest index sent over the channel
	streaming bool    // use streaming reader, if true
	url       url.URL // current broker url

	opened bool
	c      chan *Message // channel streams messages from the broker.

	wg      sync.WaitGroup
	closing chan struct{}

	// The amount of time to wait before reconnecting to a broker stream.
	ReconnectTimeout time.Duration

	// The logging interface used by the connection for out-of-band errors.
	Logger *log.Logger
}

// NewConn returns a new connection to the broker for a topic.
func NewConn(topicID uint64) *Conn {
	return &Conn{
		topicID:          topicID,
		ReconnectTimeout: DefaultReconnectTimeout,
		Logger:           log.New(os.Stderr, "", log.LstdFlags),
	}
}

// TopicID returns the connection's topic id.
func (c *Conn) TopicID() uint64 { return c.topicID }

// C returns streaming channel for the connection.
func (c *Conn) C() <-chan *Message { return c.c }

// Index returns the highest index replicated to the caller.
func (c *Conn) Index() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.index
}

// SetIndex sets the highest index replicated to the caller.
func (c *Conn) SetIndex(index uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index = index
}

// Streaming returns true if the connection streams messages continuously.
func (c *Conn) Streaming() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streaming
}

// URL returns the current URL of the connection.
func (c *Conn) URL() url.URL {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.url
}

// SetURL sets the current URL of the connection.
func (c *Conn) SetURL(u url.URL) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.url = u
}

// Open opens a streaming connection to the broker.
func (c *Conn) Open(index uint64, streaming bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Exit if aleady open or previously closed.
	if c.opened {
		return ErrConnOpen
	} else if c.c != nil {
		return ErrConnCannotReuse
	}
	c.opened = true

	// Set starting index.
	c.index = index
	c.streaming = streaming

	// Create streaming channel.
	c.c = make(chan *Message, 0)

	// Start goroutines.
	c.wg.Add(1)
	c.closing = make(chan struct{})
	go c.streamer(c.closing)

	return nil
}

// Close closes a connection.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.close()
}

func (c *Conn) close() error {
	if !c.opened {
		return ErrConnClosed
	}

	// Notify goroutines that the connection is closing.
	if c.closing != nil {
		close(c.closing)
		c.closing = nil
	}

	// Wait for goroutines to finish.
	c.mu.Unlock()
	c.wg.Wait()
	c.mu.Lock()

	// Close channel.
	close(c.c)

	// Mark connection as closed.
	c.opened = false

	return nil
}

// Heartbeat sends a heartbeat back to the broker with the client's index.
func (c *Conn) Heartbeat() error {
	var resp *http.Response
	var err error

	// Retrieve the parameters under lock.
	c.mu.Lock()
	topicID, index, u := c.topicID, c.index, c.url
	c.mu.Unlock()

	// Send the message to the messages endpoint.
	u.Path = "/messaging/heartbeat"
	u.RawQuery = url.Values{
		"topicID": {strconv.FormatUint(topicID, 10)},
		"index":   {strconv.FormatUint(index, 10)},
	}.Encode()
	resp, err = http.Post(u.String(), "application/octet-stream", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// If the server returns a redirect then it's not the leader.
	// If it returns a non-200 code then return the error.
	if resp.StatusCode == http.StatusTemporaryRedirect {
		return ErrNoLeader
	} else if resp.StatusCode != http.StatusOK {
		if errstr := resp.Header.Get("X-Broker-Error"); errstr != "" {
			return errors.New(errstr)
		}
		return fmt.Errorf("heartbeat error: status=%d", resp.StatusCode)
	}
	return nil
}

// streamer connects to a broker server and streams the replica's messages.
func (c *Conn) streamer(closing <-chan struct{}) {
	defer c.wg.Done()

	// Continually connect and retry streaming from server.
	var req *http.Request
	var reqlock sync.Mutex

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			// Check that the connection is not closing.
			select {
			case <-closing:
				return
			default:
			}

			// Create URL.
			u := c.URL()
			u.Path = "/messaging/messages"
			u.RawQuery = url.Values{
				"topicID":   {strconv.FormatUint(c.topicID, 10)},
				"index":     {strconv.FormatUint(c.Index(), 10)},
				"streaming": {strconv.FormatBool(c.Streaming())},
			}.Encode()

			// Create request.
			reqlock.Lock()
			req, _ = http.NewRequest("GET", u.String(), nil)
			reqlock.Unlock()

			// Begin streaming request.
			if err := c.stream(req, closing); err != nil {
				c.Logger.Printf("reconnecting to broker: url=%s, err=%s", u, err)
				time.Sleep(c.ReconnectTimeout)
			}
		}
	}()

	// Wait for the connection to close or the request to close.
	<-closing

	// Close in-flight request.
	reqlock.Lock()
	if req != nil {
		http.DefaultTransport.(*http.Transport).CancelRequest(req)
	}
	reqlock.Unlock()
}

// stream connects to a broker server and streams the topic messages.
func (c *Conn) stream(req *http.Request, closing <-chan struct{}) error {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	// Ensure that we received a 200 OK from the server before streaming.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid stream status code: %d", resp.StatusCode)
	}

	c.Logger.Printf("connected to broker: %s", req.URL.String())

	// Continuously decode messages from request body in a separate goroutine.
	dec := NewMessageDecoder(resp.Body)
	for {
		// Decode message from the stream.
		m := &Message{}
		if err := dec.Decode(m); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("decode: %s", err)
		}

		// Panic if we received no data.
		if len(m.Data) == 0 {
			panic("messaging conn no data recv")
		}

		// TODO: Write broker set updates, do not passthrough to channel.

		// Write message to streaming channel.
		select {
		case <-closing:
			return nil
		case c.c <- m:
		}
	}
}

// urlsEqual returns true if a and b contain the same URLs in the same order.
func urlsEqual(a, b []url.URL) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
