package snowflake

import (
	"testing"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
)

func TestIDLength(t *testing.T) {
	gen := NewIDGenerator()
	id := gen.ID()
	if !id.Valid() {
		t.Fail()
	}
	enc, _ := id.Encode()
	if len(enc) != platform2.IDLength {
		t.Fail()
	}
}

func TestToFromString(t *testing.T) {
	gen := NewIDGenerator()
	id := gen.ID()
	var clone platform2.ID
	if err := clone.DecodeFromString(id.String()); err != nil {
		t.Error(err)
	} else if id != clone {
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

func TestGlobalMachineID(t *testing.T) {
	if !globalmachineID.set {
		t.Error("expected global machine ID to be set")
	}
	if GlobalMachineID() < 0 || GlobalMachineID() > 1023 {
		t.Error("expected global machine ID to be between 0 and 1023 inclusive")
	}
}
