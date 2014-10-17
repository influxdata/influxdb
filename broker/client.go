package broker

import (
	"errors"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// ReconnectTimeout is the time to wait between stream disconnects before retrying.
const ReconnectTimeout = 100 * time.Millisecond

// Client represents a client for the broker's HTTP API.
// Once opened, the client will stream down all messages that
type Client struct {
	mu   sync.Mutex
	name string     // the name of the client connecting.
	urls []*url.URL // list of URLs for all known brokers.

	opened bool
	done   chan struct{} // disconnection notification

	// Channel streams messages from the broker.
	// Messages can be duplicated so it is important to check the index
	// of the incoming message index to make sure it has not been processed.
	C chan *Message
}

// NewClient returns a new instance of Client.
func NewClient(name string) *Client {
	return &Client{
		name: name,
	}
}

// Name returns the replica name that the client was opened with.
func (c *Client) Name() string { return c.name }

// URLs returns a list of broker URLs to connect to.
func (c *Client) URLs() []*url.URL {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.urls
}

// Open initializes and opens the connection to the broker cluster.
func (c *Client) Open(urls []*url.URL) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Return error if the client is already open.
	// Require at least one broker URL.
	if c.opened {
		return ErrClientOpen
	} else if len(urls) == 0 {
		return ErrBrokerURLRequired
	}

	// Set the URLs to connect to on the client.
	c.urls = urls

	// Create a channel for streaming messages.
	c.C = make(chan *Message, 0)

	// Open the streamer.
	c.done = make(chan struct{})
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

	// Close message stream.
	close(c.C)
	c.C = nil

	// Shutdown streamer.
	close(c.done)
	c.done = nil

	// Unset open flag.
	c.opened = false

	return nil
}

// streamer connects to a broker server and streams the replica's messages.
func (c *Client) streamer(done chan struct{}) {
	for {
		// Check for the client disconnection.
		select {
		case <-done:
			return
		default:
		}

		// TODO: Validate that there is at least one broker URL.

		// Choose a random broker URL.
		urls := c.URLs()
		u := *urls[rand.Intn(len(urls))]

		// Connect to broker and stream.
		u.Path = "/stream"
		if err := c.streamFromURL(&u, done); err == errDone {
			return
		}
	}
}

// streamFromURL connects to a broker server and streams the replica's messages.
func (c *Client) streamFromURL(u *url.URL, done chan struct{}) error {
	u.RawQuery = url.Values{"name": {c.name}}.Encode()
	resp, err := http.Get(u.String())
	if err != nil {
		time.Sleep(ReconnectTimeout)
		return nil
	}
	defer func() { _ = resp.Body.Close() }()

	// Ensure that we received a 200 OK from the server before streaming.
	if resp.StatusCode != http.StatusOK {
		warn("status:", resp.StatusCode)
	}

	// Continuously decode messages from request body.
	dec := NewMessageDecoder(resp.Body)
	for {
		// Decode message from the stream.
		m := &Message{}
		if err := dec.Decode(m); err != nil {
			return err
		}

		// Send message to channel.
		c.C <- m

		// Check for notification of disconnect.
		select {
		case <-done:
			return errDone
		default:
		}
	}
}

// marker error for the streamer.
var errDone = errors.New("done")
