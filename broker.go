package influxdb

import (
	"time"

	"github.com/influxdb/influxdb/messaging"
)

// Broker represents an InfluxDB specific messaging broker.
type Broker struct {
	*messaging.Broker

	done   chan struct{}
	server continuousQueryStore
}

type continuousQueryStore interface {
	DataNodes() []*DataNode
	Databases() []string
	ContinuousQueries(string) []*ContinuousQuery
}

// NewBroker returns a new instance of a Broker with default values.
func NewBroker() *Broker {
	b := &Broker{}
	b.Broker = messaging.NewBroker()
	return b
}

func (b *Broker) RunContinuousQueryLoop(c continuousQueryStore) {
	b.server = c
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
			// do stuff
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
}
