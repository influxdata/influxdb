package gen

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/models"
)

func countableSequenceFnCmp(a, b NewCountableSequenceFn) bool {
	// these aren't comparable
	return true
}

func timeValuesSequenceFnCmp(a, b NewTimeValuesSequenceFn) bool {
	// these aren't comparable
	return true
}

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
	{ name = "f1", count = 5000, source = 2 },
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

	got, err := NewSpecFromSchema(&out)
	if err != nil {
		t.Error(err)
	}

	samples := []sample{0.5}
	exp := &Spec{
		SeriesLimit: nil,
		Measurements: []MeasurementSpec{
			{
				Name:        "m0",
				SeriesLimit: nil,
				TagsSpec: &TagsSpec{
					Tags: []*TagValuesSpec{
						{TagKey: "tag0"},
						{TagKey: "tag1"},
						{TagKey: "tag2"},
					},
					Sample: &samples[0],
				},
				FieldValuesSpec: &FieldValuesSpec{
					TimeSequenceSpec: TimeSequenceSpec{
						Count:     5000,
						Precision: time.Millisecond,
					},
					Name:     "f0",
					DataType: models.Float,
				},
			},
			{
				Name:        "m0",
				SeriesLimit: nil,
				TagsSpec: &TagsSpec{
					Tags: []*TagValuesSpec{
						{TagKey: "tag0"},
						{TagKey: "tag1"},
						{TagKey: "tag2"},
					},
					Sample: &samples[0],
				},
				FieldValuesSpec: &FieldValuesSpec{
					TimeSequenceSpec: TimeSequenceSpec{
						Count:     5000,
						Precision: time.Millisecond,
					},
					Name:     "f1",
					DataType: models.Integer,
				},
			},
			{
				Name:        "m1",
				SeriesLimit: nil,
				TagsSpec: &TagsSpec{
					Tags: []*TagValuesSpec{
						{TagKey: "tag0"},
					},
					Sample: &samples[0],
				},
				FieldValuesSpec: &FieldValuesSpec{
					TimeSequenceSpec: TimeSequenceSpec{
						Count:     5000,
						Precision: time.Millisecond,
					},
					Name:     "f0",
					DataType: models.Float,
				},
			},
		},
	}

	// TODO(sgc): use a Spec rather than closures for NewCountableSequenceFn and NewTimeValuesSequenceFn
	if !cmp.Equal(got, exp, cmp.Comparer(countableSequenceFnCmp), cmp.Comparer(timeValuesSequenceFnCmp)) {
		t.Errorf("unexpected spec; -got/+exp\n%s", cmp.Diff(got, exp, cmp.Comparer(countableSequenceFnCmp), cmp.Comparer(timeValuesSequenceFnCmp)))
	}
}

func TestSpecFromSchemaError(t *testing.T) {
	tests := []struct {
		name string
		in   string

		decodeErr string
		specErr   string
	}{
		{
			in: `
[[measurements]]
tags = [ { name = "tag0", source = [ "host1", "host2" ] } ]
fields = [ { name = "f0", count = 5000, source = 0.5 } ]
`,
			specErr: "error processing schema: missing measurement name",
		},
		{
			in: `
[[measurements]]
sample = -0.1
tags = [ { name = "tag0", source = [ "host1", "host2" ] } ]
fields = [ { name = "f0", count = 5000, source = 0.5 } ]
`,
			decodeErr: "sample: must be 0 < sample ≤ 1.0",
		},
		{
			in: `
[[measurements]]
name = "m0"
tags = [ { source = [ "host1", "host2" ] } ]
fields = [ { name = "f0", count = 5000, source = 0.5 } ]
`,
			decodeErr: "tag: missing or invalid value for name",
		},
		{
			in: `
[[measurements]]
name = "m0"
tags = [ { name = "tag0" } ]
fields = [ { name = "f0", count = 5000, source = 0.5 } ]
`,
			decodeErr: `missing source for tag "tag0"`,
		},
		{
			in: `
[[measurements]]
name = "m0"
tags = [ { name = "tag0", source = [ "host1", "host2" ] } ]
fields = [ { count = 5000, source = 0.5 } ]
`,
			decodeErr: `field: missing or invalid value for name`,
		},
		{
			in: `
[[measurements]]
name = "m0"
tags = [ { name = "tag0", source = [ "host1", "host2" ] } ]
fields = [ { name = "f0", count = 5000 } ]
`,
			decodeErr: `missing source for field "f0"`,
		},
	}

	checkErr := func(t *testing.T, err error, exp string) {
		t.Helper()
		if exp == "" {
			if err == nil {
				return
			}

			t.Errorf("unexpected error, got %v", err)
		}

		if err == nil {
			t.Errorf("expected error, got nil")
		} else if err.Error() != exp {
			t.Errorf("unexpected error, -got/+exp\n%s", cmp.Diff(err.Error(), exp))
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out Schema
			_, err := toml.Decode(test.in, &out)
			checkErr(t, err, test.decodeErr)

			if test.decodeErr == "" {
				_, err = NewSpecFromSchema(&out)
				checkErr(t, err, test.specErr)
			}
		})
	}
}
