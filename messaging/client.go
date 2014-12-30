package messaging

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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

// DefaultReconnectTimeout is the default time to wait between when a broker
// stream disconnects and another connection is retried.
const DefaultReconnectTimeout = 100 * time.Millisecond

// ClientConfig represents the Client configuration that must be persisted
// across restarts.
type ClientConfig struct {
	Brokers []*url.URL `json:"brokers"`
}

// NewClientConfig returns a new instance of ClientConfig.
func NewClientConfig(u []*url.URL) *ClientConfig {
	return &ClientConfig{
		Brokers: u,
	}
}

// Client represents a client for the broker's HTTP API.
// Once opened, the client will stream down all messages that
type Client struct {
	mu        sync.Mutex
	replicaID uint64       // the replica that the client is connecting as.
	config    ClientConfig // The Client state that must be persisted to disk.

	opened bool
	done   chan chan struct{} // disconnection notification

	// Channel streams messages from the broker.
	c chan *Message

	// The amount of time to wait before reconnecting to a broker stream.
	ReconnectTimeout time.Duration

	// The logging interface used by the client for out-of-band errors.
	Logger *log.Logger
}

// NewClient returns a new instance of Client.
func NewClient(replicaID uint64) *Client {
	return &Client{
		replicaID:        replicaID,
		ReconnectTimeout: DefaultReconnectTimeout,
		Logger:           log.New(os.Stderr, "[messaging] ", log.LstdFlags),
	}
}

// ReplicaID returns the replica id that the client was opened with.
func (c *Client) ReplicaID() uint64 { return c.replicaID }

// C returns streaming channel.
// Messages can be duplicated so it is important to check the index
// of the incoming message index to make sure it has not been processed.
func (c *Client) C() <-chan *Message { return c.c }

// URLs returns a list of broker URLs to connect to.
func (c *Client) URLs() []*url.URL {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.config.Brokers
}

// LeaderURL returns the URL of the broker leader.
func (c *Client) LeaderURL() *url.URL {
	c.mu.Lock()
	defer c.mu.Unlock()

	// TODO(benbjohnson): Actually keep track of the leader.
	// HACK(benbjohnson): For testing, just grab a url.
	return c.config.Brokers[0]
}

// Open initializes and opens the connection to the cluster. The
// URLs used to contact the cluster are either those supplied to
// the function, or if none are supplied, are read from the file
// at "path". These URLs do need to be URLs of actual Brokers.
// Regardless of URL source, at least 1 URL must be available
// for the client to be successfully opened.
func (c *Client) Open(path string, urls []*url.URL) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Return error if the client is already open.
	if c.opened {
		return ErrClientOpen
	}

	var seedUrls []*url.URL

	// Determine which seed URLs to use.
	if len(urls) > 0 {
		seedUrls = urls
	} else {
		// Read URLs from config file. There is no guarantee
		// that the Brokers URLs in the config file are still
		// the Brokers, so we're going to double-check.
		b, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, &c.config); err != nil {
			return err
		}
		seedUrls = c.config.Brokers
	}

	if len(seedUrls) < 1 {
		return ErrBrokerURLRequired
	}

	// Now that we have the seed URLs, actually use these to
	// get the actual Broker URLs. Do that here.
	c.config.Brokers = seedUrls // Let's pretend they are the same

	// Write the Broker URLs to disk.
	b, err := json.Marshal(c.config.Brokers)
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(path, b, 0644); err != nil {
		return err
	}

	// Create a channel for streaming messages.
	c.c = make(chan *Message, 0)

	// Open the streamer.
	c.done = make(chan chan struct{})
	go c.streamer(c.done)

	// Set open flag.
	c.opened = true

	return nil
}

// Close disconnects the client from the broker cluster.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Return error if the client is already closed.
	if !c.opened {
		return ErrClientClosed
	}

	// Shutdown streamer.
	ch := make(chan struct{})
	c.done <- ch
	<-ch
	c.done = nil

	// Close message stream.
	close(c.c)
	c.c = nil

	// Unset open flag.
	c.opened = false

	return nil
}

// Publish sends a message to the broker and returns an index or error.
func (c *Client) Publish(m *Message) (uint64, error) {
	// Send the message to the messages endpoint.
	u := *c.LeaderURL()
	u.Path = "/messages"
	u.RawQuery = url.Values{
		"type":    {strconv.FormatUint(uint64(m.Type), 10)},
		"topicID": {strconv.FormatUint(m.TopicID, 10)},
	}.Encode()
	resp, err := http.Post(u.String(), "application/octet-stream", bytes.NewReader(m.Data))
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	// If a non-200 status is returned then an error occurred.
	if resp.StatusCode != http.StatusOK {
		return 0, errors.New(resp.Header.Get("X-Broker-Error"))
	}

	// Parse broker index.
	index, err := strconv.ParseUint(resp.Header.Get("X-Broker-Index"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid index: %s", err)
	}

	return index, nil
}

// streamer connects to a broker server and streams the replica's messages.
func (c *Client) streamer(done chan chan struct{}) {
	for {
		// Check for the client disconnection.
		select {
		case ch := <-done:
			close(ch)
			return
		default:
		}

		// TODO: Validate that there is at least one broker URL.

		// Choose a random broker URL.
		urls := c.URLs()
		u := *urls[rand.Intn(len(urls))]

		// Connect to broker and stream.
		u.Path = "/messages"
		if err := c.streamFromURL(&u, done); err == errDone {
			return
		} else if err != nil {
			c.Logger.Print(err)
		}
	}
}

// streamFromURL connects to a broker server and streams the replica's messages.
func (c *Client) streamFromURL(u *url.URL, done chan chan struct{}) error {
	// Set the replica id on the URL and open the stream.
	u.RawQuery = url.Values{"replicaID": {strconv.FormatUint(c.replicaID, 10)}}.Encode()
	resp, err := http.Get(u.String())
	if err != nil {
		time.Sleep(c.ReconnectTimeout)
		return nil
	}
	defer func() { _ = resp.Body.Close() }()

	// Ensure that we received a 200 OK from the server before streaming.
	if resp.StatusCode != http.StatusOK {
		time.Sleep(c.ReconnectTimeout)
		return nil
	}

	// Continuously decode messages from request body in a separate goroutine.
	errNotify := make(chan error, 0)
	go func() {
		dec := NewMessageDecoder(resp.Body)
		for {
			// Decode message from the stream.
			m := &Message{}
			if err := dec.Decode(m); err != nil {
				errNotify <- err
				return
			}

			// Write message to streaming channel.
			c.c <- m
		}
	}()

	// Check for the client disconnect or error from the stream.
	select {
	case ch := <-done:
		// Close body.
		_ = resp.Body.Close()

		// Clear message buffer.
		select {
		case <-c.c:
		default:
		}

		// Notify the close function and return marker error.
		close(ch)
		return errDone

	case err := <-errNotify:
		return err
	}
}

// marker error for the streamer.
var errDone = errors.New("done")
