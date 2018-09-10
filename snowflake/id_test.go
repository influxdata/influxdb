package snowflake

import (
	"bytes"
	"testing"

	"github.com/influxdata/platform"
)

func TestIDLength(t *testing.T) {
	gen := NewIDGenerator()
	id := gen.ID()
	if len(id) != 8 {
		t.Fail()
	}
}

func TestToFromString(t *testing.T) {
	gen := NewIDGenerator()
	id := gen.ID()
	var clone platform.ID
	if err := clone.DecodeFromString(id.String()); err != nil {
		t.Error(err)
	} else if !bytes.Equal(id, clone) {
		t.Errorf("id started as %x but got back %x", id, clone)
	}
}

func TestWithMachineID(t *testing.T) {
	gen := NewIDGenerator(WithMachineID(1023))
	if gen.Generator.MachineID() != 1023 {
		t.Errorf("expected machineID of %d but got %d", 1023, gen.Generator.MachineID())
	}
	gen = NewIDGenerator(WithMachineID(1023))
	if gen.Generator.MachineID() != 1023 {
		t.Errorf("expected machineID of %d but got %d", 1023, gen.Generator.MachineID())
	}
	gen = NewIDGenerator(WithMachineID(99))
	if gen.Generator.MachineID() != 99 {
		t.Errorf("expected machineID of %d but got %d", 99, gen.Generator.MachineID())
	}
	gen = NewIDGenerator(WithMachineID(101376))
	if gen.Generator.MachineID() != 0 {
		t.Errorf("expected machineID of %d but got %d", 0, gen.Generator.MachineID())
	}
	gen = NewIDGenerator(WithMachineID(102399))
	if gen.Generator.MachineID() != 1023 {
		t.Errorf("expected machineID of %d but got %d", 1023, gen.Generator.MachineID())
	}
}
