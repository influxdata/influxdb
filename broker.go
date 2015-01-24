package influxdb

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/influxdb/influxdb/messaging"
)

// Broker represents an InfluxDB specific messaging broker.
type Broker struct {
	*messaging.Broker

	done   chan struct{}
	server dataNodeStore

	// send CQ processing requests to the same data node
	currentCQProcessingNode *DataNode

	// variables to control when to trigger processing and when to timeout
	TriggerInterval     time.Duration
	TriggerTimeout      time.Duration
	TriggerFailurePause time.Duration
}

type dataNodeStore interface {
	DataNodes() []*DataNode
}

// NewBroker returns a new instance of a Broker with default values.
func NewBroker() *Broker {
	b := &Broker{
		TriggerInterval:     1 * time.Second,
		TriggerTimeout:      2 * time.Second,
		TriggerFailurePause: 100 * time.Millisecond,
	}
	b.Broker = messaging.NewBroker()
	return b
}

func (b *Broker) RunContinuousQueryLoop(d dataNodeStore) {
	b.server = d
	b.done = make(chan struct{})
	go b.continuousQueryLoop()
}

func (b *Broker) Close() error {
	if b.done != nil {
		close(b.done)
		b.done = nil
	}
	return b.Broker.Close()
}

func (b *Broker) continuousQueryLoop() {
	for {
		// Check if broker is currently leader.
		if b.Broker.IsLeader() {
			b.runContinuousQueries()
		}

		// Wait for a timeout or until done.
		select {
		case <-b.done:
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func (b *Broker) runContinuousQueries() {
	if b.server == nil {
		return
	}

	next := 0
	for {
		// if the current node hasn't been set it's our first time or we're reset. move to the next one
		if b.currentCQProcessingNode == nil {
			dataNodes := b.server.DataNodes()
			if len(dataNodes) == 0 {
				return // don't have any nodes to try, give it up
			}
			next = next % len(dataNodes)
			b.currentCQProcessingNode = dataNodes[next]
			next++
		}

		// if no error, we're all good
		err := b.requestContinuousQueryProcessing()
		if err == nil {
			return
		}
		log.Printf("broker cq: error hitting data node: %s: %s\n", b.currentCQProcessingNode.URL, err.Error())

		// reset and let the loop try the next data node in the cluster
		b.currentCQProcessingNode = nil
		<-time.After(100 * time.Millisecond)
	}
}

func (b *Broker) requestContinuousQueryProcessing() error {
	// Send request.
	cqUrl := copyURL(b.currentCQProcessingNode.URL)
	cqUrl.Path = "/process_continuous_queries"
	cqUrl.Scheme = "http"
	client := &http.Client{
		Timeout: 1 * time.Second,
	}
	resp, err := client.Post(cqUrl.String(), "application/octet-stream", nil)
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
