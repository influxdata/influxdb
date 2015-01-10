package influxdb

import (
	"time"

	"github.com/influxdb/influxdb/messaging"
)

// Broker represents an InfluxDB specific messaging broker.
type Broker struct {
	*messaging.Broker

	done chan struct{}
}

func (b *Broker) Open(path string, addr string) error {
	if err := b.Broker.Open(path, addr); err != nil {
		return err
	}

	b.done = make(chan struct{})
	go b.querier(b.done)

	return nil
}

func (b *Broker) Close() error {
	if b.done != nil {
		close(b.done)
		b.done = nil
	}
	return b.Broker.Close()
}

func (b *Broker) querier() {
	for {
		// Check if broker is currently leader.
		if b.Broker.IsLeader() {
			// DO SOME CQ SHIT
		}

		// Wait for a timeout or until done.
		select {
		case <-done:
			return
		case <-time.After(1 * time.Second):
		}
	}
}
