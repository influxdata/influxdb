package broker_test

import (
	"bytes"
	"fmt"
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
func TestBroker_Write(t *testing.T) {
	b := NewBroker()
	defer b.Close()

	// Create a topic.
	if err := b.CreateTopic("foo/bar"); err != nil {
		t.Fatalf("create topic error: %s", err)
	}

	// Create a new named stream.
	if err := b.CreateStream("node0"); err != nil {
		t.Fatalf("create stream: %s", err)
	}

	// Subscribe stream to the foo/bar topic.
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

	// Read message from the stream.
	var buf bytes.Buffer
	go func() {
		if _, err := b.Stream("node0").WriteTo(&buf); err != nil {
			t.Fatalf("write to: %s", err)
		}
	}()
	time.Sleep(10 * time.Millisecond)

	// Read out the config messages first.
	var m broker.Message
	dec := broker.NewMessageDecoder(&buf)
	if err := dec.Decode(&m); err != nil || m.Type != broker.CreateStreamMessageType {
		t.Fatalf("decode(create stream): %x (%v)", m.Type, err)
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
