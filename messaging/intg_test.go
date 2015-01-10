package messaging_test

import (
	"testing"
	//"time"

	"github.com/influxdb/influxdb/messaging"
)

// Ensure a broker can join to another existing broker and copy a snapshot.
func TestBroker_Join(t *testing.T) {
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
