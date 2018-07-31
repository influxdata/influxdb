package snowflake

import (
	"testing"

	"github.com/influxdata/platform"
)

func TestIDStringLength(t *testing.T) {
	gen := NewIDGenerator()
	id := gen.ID()
	if !id.Valid() {
		t.Fail()
	}
	enc, _ := id.Encode()
	if len(enc) != platform.IDStringLength {
		t.Fail()
	}
}

func TestToFromString(t *testing.T) {
	gen := NewIDGenerator()
	id := gen.ID()
	var clone platform.ID
	if err := clone.DecodeFromString(id.String()); err != nil {
		t.Error(err)
	} else if id != clone {
		t.Errorf("id started as %x but got back %x", id, clone)
	}
}
