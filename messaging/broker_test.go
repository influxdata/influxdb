package messaging_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/messaging"
)

// Ensure that opening a broker without a path returns an error.
func TestBroker_Open_ErrPathRequired(t *testing.T) {
	b := messaging.NewBroker()
	if err := b.Open(""); err != messaging.ErrPathRequired {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that closing an already closed broker returns an error.
func TestBroker_Close_ErrClosed(t *testing.T) {
	b := NewBroker()
	b.Close()
	if err := b.Broker.Close(); err != messaging.ErrClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the broker can write messages to the appropriate topics.
func TestBroker_Publish(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// Create a new named replica.
	if err := b.CreateReplica("node0"); err != nil {
		t.Fatalf("create replica: %s", err)
	}

	// Subscribe replica to a topic.
	if err := b.Subscribe("node0", 20); err != nil {
		t.Fatalf("subscribe: %s", err)
	}

	// Write a message to the broker.
	index, err := b.Publish(&messaging.Message{Type: 100, TopicID: 20, Data: []byte("0000")})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if index != 4 {
		t.Fatalf("unexpected index: %d", index)
	}
	if err := b.Sync(index); err != nil {
		t.Fatalf("sync error: %s", err)
	}

	// Read message from the replica.
	var buf bytes.Buffer
	go func() {
		if _, err := b.Replica("node0").WriteTo(&buf); err != nil {
			t.Fatalf("write to: %s", err)
		}
	}()
	time.Sleep(10 * time.Millisecond)

	// Read out the config messages first.
	var m messaging.Message
	dec := messaging.NewMessageDecoder(&buf)
	if err := dec.Decode(&m); err != nil || m.Type != messaging.CreateReplicaMessageType {
		t.Fatalf("decode(create replica): %x (%v)", m.Type, err)
	}
	if err := dec.Decode(&m); err != nil || m.Type != messaging.SubscribeMessageType {
		t.Fatalf("decode(subscribe): %x (%v)", m.Type, err)
	}

	// Read out the published message.
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("decode: %s", err)
	} else if !reflect.DeepEqual(&m, &messaging.Message{Type: 100, TopicID: 20, Index: 4, Data: []byte("0000")}) {
		t.Fatalf("unexpected message: %#v", &m)
	}

	// Unsubscribe replica from the topic.
	if err := b.Unsubscribe("node0", 20); err != nil {
		t.Fatalf("unsubscribe: %s", err)
	}

	// Write another message (that shouldn't be read).
	if _, err := b.Publish(&messaging.Message{Type: 101, TopicID: 20}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Read unsubscribe.
	if err := dec.Decode(&m); err != nil || m.Type != messaging.UnsubscribeMessageType {
		t.Fatalf("decode(unsubscribe): %x (%v)", m.Type, err)
	}

	// EOF
	if err := dec.Decode(&m); err != io.EOF {
		t.Fatalf("decode(eof): %x (%v)", m.Type, err)
	}
}

// Ensure that creating a duplicate replica will return an error.
func TestBroker_CreateReplica_ErrReplicaExists(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// Create a replica twice.
	b.CreateReplica("node0")
	if err := b.CreateReplica("node0"); err != messaging.ErrReplicaExists {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the broker can remove an existing replica.
func TestBroker_DeleteReplica(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// Create a new named replica.
	if err := b.CreateReplica("node0"); err != nil {
		t.Fatalf("create replica: %s", err)
	}

	// Attach a replica writer.
	var buf bytes.Buffer
	var closed bool
	go func() {
		if _, err := b.Replica("node0").WriteTo(&buf); err != nil {
			t.Fatalf("write to: %s", err)
		}
		closed = true
	}()
	time.Sleep(10 * time.Millisecond)

	// Delete the replica.
	if err := b.DeleteReplica("node0"); err != nil {
		t.Fatalf("delete replica: %s", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Ensure the writer was closed.
	if !closed {
		t.Fatal("replica writer did not close")
	}

	// Ensure the replica no longer exists.
	if r := b.Replica("node0"); r != nil {
		t.Fatal("replica still exists")
	}
}

// Ensure an error is returned when deleting a non-existent replica.
func TestBroker_DeleteReplica_ErrReplicaNotFound(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	if err := b.DeleteReplica("no_such_replica"); err != messaging.ErrReplicaNotFound {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that subscribing to a missing replica returns an error.
func TestBroker_Subscribe_ErrReplicaNotFound(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	b.CreateReplica("bar")
	if err := b.Subscribe("foo", 20); err != messaging.ErrReplicaNotFound {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that unsubscribing from a missing replica returns an error.
func TestBroker_Unsubscribe_ErrReplicaNotFound(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	if err := b.Unsubscribe("foo", 20); err != messaging.ErrReplicaNotFound {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Broker is a wrapper for broker.Broker that creates the broker in a temporary location.
type Broker struct {
	*messaging.Broker
}

// NewBroker returns a new open tempoarary broker.
func NewBroker() *Broker {
	b := messaging.NewBroker()
	if err := b.Open(tempfile()); err != nil {
		panic("open: " + err.Error())
	}
	if err := b.Initialize(); err != nil {
		panic("initialize: " + err.Error())
	}
	return &Broker{b}
}

// Close closes and deletes the temporary broker.
func (b *Broker) Close() {
	defer os.RemoveAll(b.Path())
	b.Broker.Close()
}

// tempfile returns a temporary path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "influxdb-messaging-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
