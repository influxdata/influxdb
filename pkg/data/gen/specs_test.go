package gen

import (
	"testing"

	"github.com/BurntSushi/toml"
)

func TestSpecFromSchema(t *testing.T) {
	in := `
title = "example schema"

[[measurements]]
name = "m0"
tags = [
	{ name = "tag0", source = [ "host1", "host2" ] },
	{ name = "tag1", source = [ "process1", "process2" ] },
	{ name = "tag2", source = { type = "sequence", format = "value%s", start = 0, count = 100 } }
]
fields = [
	{ name = "f0", count = 5000, source = 0.5 },
	{ name = "f1", count = 5000, source = 0.5 },
]
[[measurements]]
name = "m1"
tags = [
	{ name = "tag0", source = [ "host1", "host2" ] },
]
fields = [
	{ name = "f0", count = 5000, source = 0.5 },
]
`
	var out Schema
	if _, err := toml.Decode(in, &out); err != nil {
		t.Fatalf("unxpected error: %v", err)
	}

	spec, err := NewSpecFromSchema(&out)
	if err != nil {
		t.Error(err)
	}
	t.Log(spec)
}

func TestSpecFromSchemaError(t *testing.T) {
	in := `
title = "example schema"

[[measurements]]
name = "m0"
tags = [
	{ name = "tag0", source = [ "host1", "host2" ] },
	{ name = "tag1", source = { type = "sequence", format = "value%s", start = 0, count = 100 } },
]
fields = [
	{ name = "f0", count = 5000, source = 0.5 },
]
[[measurements]]
name = "m1"
tags = [
	{ name = "tag0", source = [ "host1", "host2" ] },
]
fields = [
	{ name = "f0", count = 5000, source = 0.5 },
]
`

	var out Schema
	if _, err := toml.Decode(in, &out); err != nil {
		t.Fatalf("unxpected error: %v", err)
	}

	spec, err := NewSpecFromSchema(&out)
	if err != nil {
		t.Error(err)
	}
	t.Log(spec)
}
