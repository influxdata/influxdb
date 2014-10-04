package broker_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/broker"
)

// Ensure the broker can write messages to the appropriate topics.
func TestBroker_Write(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// Write a message to the broker.
	index, err := b.Publish("foo/bar", &broker.Message{Type: 100, Data: []byte("0000")})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if index != 2 {
		t.Fatalf("unexpected index: %d", index)
	}
	if err := b.Wait(index); err != nil {
		t.Fatalf("wait error: %s", err)
	}

	// Create a new named stream and subscription.
	if err := b.CreateStream("node0"); err != nil {
		t.Fatalf("create stream: %s", err)
	}

	// Retrieve stream and subscribe.
	s := b.Stream("node0")
	if err := s.Subscribe("foo/bar", 0); err != nil {
		t.Fatalf("subscribe: %s", err)
	}

	// Read message from the stream.
	var buf bytes.Buffer
	go func() {
		if _, err := s.WriteTo(&buf); err != nil {
			t.Fatalf("write to: %s", err)
		}
	}()
	time.Sleep(10 * time.Millisecond)

	// Read out the message.
	var m broker.Message
	dec := broker.NewMessageDecoder(&buf)
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("decode: %s", err)
	} else if !reflect.DeepEqual(&m, &broker.Message{Type: 100, Index: 2, Data: []byte("0000")}) {
		t.Fatalf("unexpected message: %#v", &m)
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
