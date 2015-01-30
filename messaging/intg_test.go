package messaging_test

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/influxdb/influxdb/messaging"
)

// Ensure a broker can join to another existing broker and copy a snapshot.
func TestBroker_Join(t *testing.T) {
	t.Skip()
	s0, s1 := NewServer(), NewUninitializedServer()
	defer s0.Close()
	defer s1.Close()

	// Retrieve broker references.
	b0, b1 := s0.Broker(), s1.Broker()

	// Create data on the first server.
	b0.CreateReplica(20)
	b0.Subscribe(20, 1000)
	index, _ := b0.Publish(&messaging.Message{Type: 100, TopicID: 1000, Data: []byte("XXXX")})
	b0.Sync(index)

	// Join the second server.
	if err := b1.Join(b0.URL()); err != nil {
		t.Fatalf("join error: %s", err)
	}

	// Publish a message after the join & sync second broker.
	index, _ = b0.Publish(&messaging.Message{Type: 100, TopicID: 1000, Data: []byte("YYYY")})
	if err := b1.Sync(index); err != nil {
		t.Fatalf("unable to sync: idx=%d; err=%s", index, err)
	}

	// Verify the second server copied a snapshot of the first server.
	if r := b1.Replica(20); r == nil {
		t.Fatalf("replica not found")
	}

	// Check that one publish message was sent.
	if a := Messages(b1.MustReadAll(20)).Unicasted(); len(a) != 2 {
		t.Fatalf("message count mismatch: %d", len(a))
	} else if m := a[0]; string(m.Data) != `XXXX` {
		t.Fatalf("unexpected message: %s", m.Data)
	} else if m := a[1]; string(m.Data) != `YYYY` {
		t.Fatalf("unexpected message: %s", m.Data)
	}

	// Publish another message to ensure logs are appended after writer is advanced.
	index, _ = b0.Publish(&messaging.Message{Type: 100, TopicID: 1000, Data: []byte("ZZZZ")})
	if err := b1.Sync(index); err != nil {
		t.Fatalf("unable to sync: idx=%d; err=%s", index, err)
	}

	// Check messages one more time to ensure we have the last one.
	if a := Messages(b1.MustReadAll(20)).Unicasted(); len(a) != 3 {
		t.Fatalf("message count mismatch: %d", len(a))
	} else if m := a.Last(); string(m.Data) != `ZZZZ` {
		t.Fatalf("unexpected message: %s", m.Data)
	}
}

// Benchmarks a cluster of 3 brokers over HTTP.
func BenchmarkCluster_Publish(b *testing.B) {
	c := NewCluster(3)
	defer c.Close()

	// Create replica and connect client.
	c.Leader().Broker().CreateReplica(100)
	client := messaging.NewClient(100)
	client.Open("", []*url.URL{c.URL()})

	b.ResetTimer()

	var index uint64
	for i := 0; i < b.N; i++ {
		var err error
		index, err = client.Publish(&messaging.Message{Type: 0, TopicID: 1, Data: make([]byte, 50)})
		if err != nil {
			b.Fatalf("unexpected error: %s", err)
		}
	}

	// Wait for the broker to commit.
	c.MustSync(index)
}

// Cluster represents a set of joined Servers.
type Cluster struct {
	Servers []*Server
}

// NewCluster returns a Cluster with n servers.
func NewCluster(n int) *Cluster {
	c := &Cluster{}
	c.Servers = []*Server{NewServer()}

	// Join additional servers on.
	for i := 1; i < n; i++ {
		// Join each additional server to the first.
		other := NewUninitializedServer()
		if err := other.Broker().Join(c.Leader().Broker().URL()); err != nil {
			panic("join error: " + err.Error())
		}

		c.Servers = append(c.Servers, other)
	}

	return c
}

func (c *Cluster) Leader() *Server { return c.Servers[0] }
func (c *Cluster) URL() *url.URL   { return c.Leader().Broker().URL() }

// MustSync runs sync against every server in the cluster. Panic on error.
func (c *Cluster) MustSync(index uint64) {
	for i, s := range c.Servers {
		if err := s.Broker().Sync(index); err != nil {
			panic(fmt.Sprintf("sync error(%d): %s", i, err))
		}
	}
}

func (c *Cluster) Close() {
	for _, s := range c.Servers {
		s.Close()
	}
}
