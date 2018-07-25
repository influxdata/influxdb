package snowflake

import (
	"bytes"
	"testing"

	"github.com/influxdata/platform"
)

func TestIDLength(t *testing.T) {
	gen := NewIDGenerator()
	id := gen.ID()
	if id == nil {
		t.Fail()
	}
	if len(id.Encode()) != platform.IDLength {
		t.Fail()
	}
}

func TestToFromString(t *testing.T) {
	gen := NewIDGenerator()
	id := gen.ID()
	var clone platform.ID
	if err := clone.DecodeFromString(id.String()); err != nil {
		t.Error(err)
	} else if !bytes.Equal(id.Encode(), clone.Encode()) {
		t.Errorf("id started as %x but got back %x", id, clone)
	}
}
