package broker_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/broker"
)

// Ensure that opening a broker without a path returns an error.
func TestBroker_Open_ErrPathRequired(t *testing.T) {
	b := broker.New()
	if err := b.Open(""); err != broker.ErrPathRequired {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that closing an already closed broker returns an error.
func TestBroker_Close_ErrClosed(t *testing.T) {
	b := NewBroker()
	b.Close()
	if err := b.Broker.Close(); err != broker.ErrClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the broker can write messages to the appropriate topics.
func TestBroker_Publish(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// Create a topic.
	if err := b.CreateTopic("foo/bar"); err != nil {
		t.Fatalf("create topic error: %s", err)
	}

	// Create a new named replica.
	if err := b.CreateReplica("node0"); err != nil {
		t.Fatalf("create replica: %s", err)
	}

	// Subscribe replica to the foo/bar topic.
	if err := b.Subscribe("node0", "foo/bar"); err != nil {
		t.Fatalf("subscribe: %s", err)
	}

	// Write a message to the broker.
	index, err := b.Publish("foo/bar", &broker.Message{Type: 100, Data: []byte("0000")})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if index != 5 {
		t.Fatalf("unexpected index: %d", index)
	}
	if err := b.Wait(index); err != nil {
		t.Fatalf("wait error: %s", err)
	}

	// Read message from the replica.
	var buf bytes.Buffer
	go func() {
		if _, err := b.Replica("node0").WriteTo(&buf); err != nil {
			t.Fatalf("write to: %s", err)
		}
	}()
	time.Sleep(10 * time.Millisecond)

	// Unsubscribe replica from the foo/bar topic.
	if err := b.Unsubscribe("node0", "foo/bar"); err != nil {
		t.Fatalf("unsubscribe: %s", err)
	}

	// Write another message (that shouldn't be read).
	if _, err := b.Publish("foo/bar", &broker.Message{Type: 101}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Read out the config messages first.
	var m broker.Message
	dec := broker.NewMessageDecoder(&buf)
	if err := dec.Decode(&m); err != nil || m.Type != broker.CreateReplicaMessageType {
		t.Fatalf("decode(create replica): %x (%v)", m.Type, err)
	}
	if err := dec.Decode(&m); err != nil || m.Type != broker.SubscribeMessageType {
		t.Fatalf("decode(subscribe): %x (%v)", m.Type, err)
	}

	// Read out the published message.
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("decode: %s", err)
	} else if !reflect.DeepEqual(&m, &broker.Message{Type: 100, Index: 5, Data: []byte("0000")}) {
		t.Fatalf("unexpected message: %#v", &m)
	}

	// Read unsubscribe.
	if err := dec.Decode(&m); err != nil || m.Type != broker.UnsubscribeMessageType {
		t.Fatalf("decode(unsubscribe): %x (%v)", m.Type, err)
	}

	// EOF
	if err := dec.Decode(&m); err != io.EOF {
		t.Fatalf("decode(eof): %x (%v)", m.Type, err)
	}
}

// Ensure the broker returns an error when publishing to a topic that doesn't exist.
func TestBroker_Publish_ErrTopicNotFound(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// Publish to a topic that doesn't exist.
	if _, err := b.Publish("foo/bar", &broker.Message{}); err != broker.ErrTopicNotFound {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the broker cannot create a duplicate topic.
func TestBroker_CreateTopic_ErrTopicExists(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// Create a topic. And then create it again.
	if err := b.CreateTopic("foo/bar"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := b.CreateTopic("foo/bar"); err != broker.ErrTopicExists {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure the broker can delete a topic.
func TestBroker_DeleteTopic(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// Create a topic & replica.
	b.CreateTopic("foo/bar")
	b.CreateReplica("node0")

	// Attach replica.
	r := b.Replica("node0")
	r.Subscribe("foo/bar")
	if a := r.Topics(); !reflect.DeepEqual(a, []string{"config", "foo/bar"}) {
		t.Fatalf("unexpected replica topics: %#v", a)
	}

	// Delete topic.
	if err := b.DeleteTopic("foo/bar"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Ensure topic has been removed from replica subscriptions.
	if a := r.Topics(); !reflect.DeepEqual(a, []string{"config"}) {
		t.Fatalf("unexpected replica topics: %#v", a)
	}

	// Try to delete it again.
	if err := b.DeleteTopic("foo/bar"); err != broker.ErrTopicNotFound {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that creating a duplicate replica will return an error.
func TestBroker_CreateReplica_ErrReplicaExists(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// Create a replica twice.
	b.CreateReplica("node0")
	if err := b.CreateReplica("node0"); err != broker.ErrReplicaExists {
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
	if err := b.DeleteReplica("no_such_replica"); err != broker.ErrReplicaNotFound {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that subscribing to a missing replica returns an error.
func TestBroker_Subscribe_ErrReplicaNotFound(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	b.CreateReplica("bar")
	if err := b.Subscribe("foo", "bar"); err != broker.ErrReplicaNotFound {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that subscribing to a missing topic returns an error.
func TestBroker_Subscribe_ErrTopicNotFound(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	b.CreateReplica("foo")
	if err := b.Subscribe("foo", "bar"); err != broker.ErrTopicNotFound {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that unsubscribing from a missing replica returns an error.
func TestBroker_Unsubscribe_ErrReplicaNotFound(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	b.CreateTopic("bar")
	if err := b.Unsubscribe("foo", "bar"); err != broker.ErrReplicaNotFound {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that unsubscribing from a missing topic returns an error.
func TestBroker_Unsubscribe_ErrTopicNotFound(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	b.CreateReplica("foo")
	if err := b.Unsubscribe("foo", "bar"); err != broker.ErrTopicNotFound {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Broker is a wrapper for broker.Broker that creates the broker in a temporary location.
type Broker struct {
	*broker.Broker
}

// NewBroker returns a new open tempoarary broker.
func NewBroker() *Broker {
	b := broker.New()
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
	f, _ := ioutil.TempFile("", "influxdb-broker-")
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

func ok(err error) {
	if err != nil {
		panic("unexpected error")
	}
}
