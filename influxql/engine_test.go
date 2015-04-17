package influxql

import (
	"testing"
	"time"
)

func TestMergeOutputs(t *testing.T) {
	job := MapReduceJob{}

	test := []struct {
		name     string
		first    []*rawQueryMapOutput
		second   []*rawQueryMapOutput
		expected []*rawQueryMapOutput
	}{
		{
			name:     "empty slices",
			first:    []*rawQueryMapOutput{},
			second:   []*rawQueryMapOutput{},
			expected: []*rawQueryMapOutput{},
		},
		{
			name:     "first empty",
			first:    []*rawQueryMapOutput{},
			second:   []*rawQueryMapOutput{&rawQueryMapOutput{time.Unix(0, 0).UnixNano(), 0}},
			expected: []*rawQueryMapOutput{&rawQueryMapOutput{time.Unix(0, 0).UnixNano(), 0}},
		},
		{
			name:     "second empty",
			first:    []*rawQueryMapOutput{&rawQueryMapOutput{time.Unix(0, 0).UnixNano(), 0}},
			second:   []*rawQueryMapOutput{},
			expected: []*rawQueryMapOutput{&rawQueryMapOutput{time.Unix(0, 0).UnixNano(), 0}},
		},
		{
			name:   "first before",
			first:  []*rawQueryMapOutput{&rawQueryMapOutput{time.Unix(0, 0).UnixNano(), 0}},
			second: []*rawQueryMapOutput{&rawQueryMapOutput{time.Unix(1, 0).UnixNano(), 0}},
			expected: []*rawQueryMapOutput{
				&rawQueryMapOutput{time.Unix(0, 0).UnixNano(), 0},
				&rawQueryMapOutput{time.Unix(1, 0).UnixNano(), 0},
			},
		},
		{
			name:   "second before",
			first:  []*rawQueryMapOutput{&rawQueryMapOutput{time.Unix(1, 0).UnixNano(), 0}},
			second: []*rawQueryMapOutput{&rawQueryMapOutput{time.Unix(0, 0).UnixNano(), 0}},
			expected: []*rawQueryMapOutput{
				&rawQueryMapOutput{time.Unix(0, 0).UnixNano(), 0},
				&rawQueryMapOutput{time.Unix(1, 0).UnixNano(), 0},
			},
		},
		{
			name:   "dups removed",
			first:  []*rawQueryMapOutput{&rawQueryMapOutput{time.Unix(1, 0).UnixNano(), 0}},
			second: []*rawQueryMapOutput{&rawQueryMapOutput{time.Unix(1, 0).UnixNano(), 0}},
			expected: []*rawQueryMapOutput{
				&rawQueryMapOutput{time.Unix(1, 0).UnixNano(), 0},
			},
		},
		{
			name: "sorted dups removed",
			first: []*rawQueryMapOutput{
				&rawQueryMapOutput{time.Unix(0, 0).UnixNano(), 0},
				&rawQueryMapOutput{time.Unix(1, 0).UnixNano(), 0},
			},
			second: []*rawQueryMapOutput{
				&rawQueryMapOutput{time.Unix(1, 0).UnixNano(), 0},
				&rawQueryMapOutput{time.Unix(2, 0).UnixNano(), 0},
			},
			expected: []*rawQueryMapOutput{
				&rawQueryMapOutput{time.Unix(0, 0).UnixNano(), 0},
				&rawQueryMapOutput{time.Unix(1, 0).UnixNano(), 0},
				&rawQueryMapOutput{time.Unix(2, 0).UnixNano(), 0},
			},
		},
	}

	for _, c := range test {
		got := job.mergeOutputs(c.first, c.second)

		if len(got) != len(c.expected) {
			t.Errorf("test %s: result length mismatch: got %v, exp %v", c.name, len(got), len(c.expected))
		}

		for j := 0; j < len(c.expected); j++ {
			if exp := c.expected[j]; exp.Timestamp != got[j].Timestamp {
				t.Errorf("test %s: timestamp mismatch: got %v, exp %v", c.name, got[j].Timestamp, exp.Timestamp)
			}
		}

	}
}
