package influxdb

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/influxdb/influxdb/messaging"
)

const (
	// DefaultContinuousQueryCheckTime is how frequently the broker will ask a data node
	// in the cluster to run any continuous queries that should be run.
	DefaultContinuousQueryCheckTime = 1 * time.Second

	// DefaultDataNodeTimeout is how long the broker will wait before timing out on a data node
	// that it has requested process continuous queries.
	DefaultDataNodeTimeout = 1 * time.Second

	// DefaultFailureSleep is how long the broker will sleep before trying the next data node in
	// the cluster if the current data node failed to respond
	DefaultFailureSleep = 100 * time.Millisecond
)

// Broker represents an InfluxDB specific messaging broker.
type Broker struct {
	mu sync.RWMutex
	*messaging.Broker
	client *http.Client

	done chan struct{}

	// variables to control when to trigger processing and when to timeout
	TriggerInterval     time.Duration
	TriggerTimeout      time.Duration
	TriggerFailurePause time.Duration
}

// NewBroker returns a new instance of a Broker with default values.
func NewBroker() *Broker {
	return &Broker{
		Broker: messaging.NewBroker(),
		client: &http.Client{
			Timeout: DefaultDataNodeTimeout,
		},

		TriggerInterval:     5 * time.Second,
		TriggerTimeout:      20 * time.Second,
		TriggerFailurePause: 1 * time.Second,
	}
}

// RunContinuousQueryLoop starts running continuous queries on a background goroutine.
func (b *Broker) RunContinuousQueryLoop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.done = make(chan struct{})
	go b.continuousQueryLoop(b.done)
}

// Close closes the broker.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.done != nil {
		close(b.done)
		b.done = nil
	}

	// since the client doesn't specify a Transport when created,
	// it will use the DefaultTransport.
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()

	return b.Broker.Close()
}

func (b *Broker) continuousQueryLoop(done chan struct{}) {
	for {
		// Check if broker is currently leader.
		if b.Broker.IsLeader() {
			b.runContinuousQueries()
		}

		// Sleep until either the broker is closed or we need to run continuous queries again
		select {
		case <-done:
			return
		case <-time.After(DefaultContinuousQueryCheckTime):
		}
	}
}

func (b *Broker) runContinuousQueries() {
	topic := b.Broker.Topic(BroadcastTopicID)
	if topic == nil {
		log.Println("broker cq: no broadcast topic currently available.")
		return // don't have any topics to get data urls from, give it up
	}
	dataURLs := topic.DataURLs()
	if len(dataURLs) == 0 {
		log.Println("broker cq: no data nodes currently available.")
		return // don't have any data urls to try, give it up
	}

	rand.Seed(time.Now().UnixNano())
	// get a set of random indexes so we can randomly distribute cq load over nodes
	ri := rand.Perm(len(dataURLs))
	for _, i := range ri {
		u := dataURLs[i]
		// if no error, we're all good
		err := b.requestContinuousQueryProcessing(u)
		if err == nil {
			return
		}
		log.Printf("broker cq: error hitting data node: %s: %s\n", u.String(), err.Error())

		// let the loop try the next data node in the cluster
		<-time.After(DefaultFailureSleep)
	}
}

func (b *Broker) requestContinuousQueryProcessing(cqURL url.URL) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	// Send request.
	cqURL.Path = "/data/process_continuous_queries"
	cqURL.Scheme = "http"
	resp, err := b.client.Post(cqURL.String(), "application/octet-stream", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check if created.
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("request returned status %s", resp.Status)
	}

	return nil
}
