package influxdb

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
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
	*messaging.Broker

	done chan struct{}

	// send CQ processing requests to the same data node
	currentCQProcessingNode *url.URL

	// variables to control when to trigger processing and when to timeout
	TriggerInterval     time.Duration
	TriggerTimeout      time.Duration
	TriggerFailurePause time.Duration
}

// NewBroker returns a new instance of a Broker with default values.
func NewBroker() *Broker {
	return &Broker{
		Broker:              messaging.NewBroker(),
		TriggerInterval:     5 * time.Second,
		TriggerTimeout:      20 * time.Second,
		TriggerFailurePause: 1 * time.Second,
	}
}

// RunContinuousQueryLoop starts running continuous queries on a background goroutine.
func (b *Broker) RunContinuousQueryLoop() {
	log.Println("and...")
	b.done = make(chan struct{})
	log.Println("boom!")
	go b.continuousQueryLoop(b.done)
}

// Close closes the broker.
func (b *Broker) Close() error {
	if b.done != nil {
		close(b.done)
		b.done = nil
	}
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
	next := 0
	for {
		// if the current node hasn't been set it's our first time or we're reset. move to the next one
		if b.currentCQProcessingNode == nil {
			topic := b.Broker.Topic(BroadcastTopicID)
			if topic == nil {
				log.Println("broker cq: no topics currently available.")
				return // don't have any nodes to try, give it up
			}
			dataURLs := topic.DataURLs()
			if len(dataURLs) == 0 {
				log.Println("broker cq: no data nodes currently available.")
				return // don't have any nodes to try, give it up
			}
			next = next % len(dataURLs)
			u := dataURLs[next]
			b.currentCQProcessingNode = &u
			next++
		}

		// if no error, we're all good
		err := b.requestContinuousQueryProcessing()
		if err == nil {
			return
		}
		log.Printf("broker cq: error hitting data node: %s: %s\n", b.currentCQProcessingNode, err.Error())

		// reset and let the loop try the next data node in the cluster
		b.currentCQProcessingNode = nil
		<-time.After(DefaultFailureSleep)
	}
}

func (b *Broker) requestContinuousQueryProcessing() error {
	// Send request.
	cqURL := copyURL(b.currentCQProcessingNode)
	cqURL.Path = "/process_continuous_queries"
	cqURL.Scheme = "http"
	client := &http.Client{
		Timeout: DefaultDataNodeTimeout,
	}
	resp, err := client.Post(cqURL.String(), "application/octet-stream", nil)
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
