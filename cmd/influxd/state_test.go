package main_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/influxdb/influxdb/cmd/influxd"
)

// Ensure that the State encoder can properly encode a state.
func TestStateEncoder_Encode(t *testing.T) {
	s := &main.State{
		Local: true,
	}

	var buf bytes.Buffer
	if err := main.NewStateEncoder(&buf).Encode(s); err != nil {
		t.Fatal(err)
	} else if buf.String() != `{"local":true}`+"\n" {
		t.Fatalf("unexpected output: %s", buf.String())
	}
}

// Ensure that the State decoder can properly decode a state.
func TestStateDecoder_Decode(t *testing.T) {
	exp := &main.State{
		Local: false,
	}

	var s main.State
	r := strings.NewReader(`{"local": false}`)
	if err := main.NewStateDecoder(r).Decode(&s); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(exp, &s) {
		t.Fatalf("unexpected state: %#v", &s)
	}
}

// Ensure that the decoder returns an error on missing Local flag
func TestStateDecoder_Decode_ErrMissingLocal(t *testing.T) {
	var s main.State
	r := strings.NewReader(`{}`)
	if err := main.NewStateDecoder(r).Decode(&s); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that the decoder returns an error on invalid Local value
func TestStateDecoder_Decode_ErrInvalidLocal(t *testing.T) {
	var s main.State
	r := strings.NewReader(`{"local": 1234`)
	if err := main.NewStateDecoder(r).Decode(&s); err == nil || err.Error() != "unexpected EOF" {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that the decoder returns an error on invalid JSON.
func TestStateDecoder_Decode_ErrUnexpectedEOF(t *testing.T) {
	var s main.State
	if err := main.NewStateDecoder(strings.NewReader(`{"no`)).Decode(&s); err == nil || err.Error() != "unexpected EOF" {
		t.Fatalf("unexpected error: %s", err)
	}
}
