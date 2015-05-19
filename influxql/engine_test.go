package influxql

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func derivativeJob(t *testing.T, fn, interval string) *MapReduceJob {

	if interval != "" {
		interval = ", " + interval
	}

	q, err := ParseQuery(fmt.Sprintf("SELECT %s(mean(value)%s) FROM foo", fn, interval))
	if err != nil {
		t.Fatalf("failed to parse query: %s", err)
	}
	m := &MapReduceJob{
		stmt: q.Statements[0].(*SelectStatement),
	}

	return m
}

// TestProccessDerivative tests the processDerivative transformation function on the engine.
// The is called for a query with a group by.
func TestProcessDerivative(t *testing.T) {
	tests := []struct {
		name     string
		fn       string
		interval string
		in       [][]interface{}
		exp      [][]interface{}
	}{
		{
			name:     "empty input",
			fn:       "derivative",
			interval: "1d",
			in:       [][]interface{}{},
			exp:      [][]interface{}{},
		},

		{
			name:     "single row returns 0.0",
			fn:       "derivative",
			interval: "1d",
			in: [][]interface{}{
				[]interface{}{
					time.Unix(0, 0), 1.0,
				},
			},
			exp: [][]interface{}{
				[]interface{}{
					time.Unix(0, 0), 0.0,
				},
			},
		},
		{
			name:     "derivative normalized to 1s by default",
			fn:       "derivative",
			interval: "",
			in: [][]interface{}{
				[]interface{}{
					time.Unix(0, 0), 1.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(24 * time.Hour), 3.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(48 * time.Hour), 5.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(72 * time.Hour), 9.0,
				},
			},
			exp: [][]interface{}{
				[]interface{}{
					time.Unix(0, 0).Add(24 * time.Hour), 2.0 / 86400,
				},
				[]interface{}{
					time.Unix(0, 0).Add(48 * time.Hour), 2.0 / 86400,
				},
				[]interface{}{
					time.Unix(0, 0).Add(72 * time.Hour), 4.0 / 86400,
				},
			},
		},
		{
			name:     "basic derivative",
			fn:       "derivative",
			interval: "1d",
			in: [][]interface{}{
				[]interface{}{
					time.Unix(0, 0), 1.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(24 * time.Hour), 3.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(48 * time.Hour), 5.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(72 * time.Hour), 9.0,
				},
			},
			exp: [][]interface{}{
				[]interface{}{
					time.Unix(0, 0).Add(24 * time.Hour), 2.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(48 * time.Hour), 2.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(72 * time.Hour), 4.0,
				},
			},
		},
		{
			name:     "12h interval",
			fn:       "derivative",
			interval: "12h",
			in: [][]interface{}{
				[]interface{}{
					time.Unix(0, 0), 1.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(24 * time.Hour), 2.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(48 * time.Hour), 3.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(72 * time.Hour), 4.0,
				},
			},
			exp: [][]interface{}{
				[]interface{}{
					time.Unix(0, 0).Add(24 * time.Hour), 0.5,
				},
				[]interface{}{
					time.Unix(0, 0).Add(48 * time.Hour), 0.5,
				},
				[]interface{}{
					time.Unix(0, 0).Add(72 * time.Hour), 0.5,
				},
			},
		},
		{
			name:     "negative derivatives",
			fn:       "derivative",
			interval: "1d",
			in: [][]interface{}{
				[]interface{}{
					time.Unix(0, 0), 1.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(24 * time.Hour), 2.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(48 * time.Hour), 0.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(72 * time.Hour), 4.0,
				},
			},
			exp: [][]interface{}{
				[]interface{}{
					time.Unix(0, 0).Add(24 * time.Hour), 1.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(48 * time.Hour), -2.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(72 * time.Hour), 4.0,
				},
			},
		},
		{
			name:     "negative derivatives",
			fn:       "non_negative_derivative",
			interval: "1d",
			in: [][]interface{}{
				[]interface{}{
					time.Unix(0, 0), 1.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(24 * time.Hour), 2.0,
				},
				// Show resultes in negative derivative
				[]interface{}{
					time.Unix(0, 0).Add(48 * time.Hour), 0.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(72 * time.Hour), 4.0,
				},
			},
			exp: [][]interface{}{
				[]interface{}{
					time.Unix(0, 0).Add(24 * time.Hour), 1.0,
				},
				[]interface{}{
					time.Unix(0, 0).Add(72 * time.Hour), 4.0,
				},
			},
		},
	}

	for _, test := range tests {
		m := derivativeJob(t, test.fn, test.interval)
		got := m.processDerivative(test.in)

		if len(got) != len(test.exp) {
			t.Fatalf("processDerivative(%s) - %s\nlen mismatch: got %d, exp %d", test.fn, test.name, len(got), len(test.exp))
		}

		for i := 0; i < len(test.exp); i++ {
			if test.exp[i][0] != got[i][0] || test.exp[i][1] != got[i][1] {
				t.Fatalf("processDerivative - %s results mismatch:\ngot %v\nexp %v", test.name, got, test.exp)
			}
		}
	}
}

// TestProcessRawQueryDerivative tests the processRawQueryDerivative transformation function on the engine.
// The is called for a queries that do not have a group by.
func TestProcessRawQueryDerivative(t *testing.T) {
	tests := []struct {
		name     string
		fn       string
		interval string
		in       []*rawQueryMapOutput
		exp      []*rawQueryMapOutput
	}{
		{
			name:     "empty input",
			fn:       "derivative",
			interval: "1d",
			in:       []*rawQueryMapOutput{},
			exp:      []*rawQueryMapOutput{},
		},

		{
			name:     "single row returns 0.0",
			fn:       "derivative",
			interval: "1d",
			in: []*rawQueryMapOutput{
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Unix(),
					Values: 1.0,
				},
			},
			exp: []*rawQueryMapOutput{
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Unix(),
					Values: 0.0,
				},
			},
		},
		{
			name:     "basic derivative",
			fn:       "derivative",
			interval: "1d",
			in: []*rawQueryMapOutput{
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Unix(),
					Values: 0.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(24 * time.Hour).UnixNano(),
					Values: 3.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(48 * time.Hour).UnixNano(),
					Values: 5.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(72 * time.Hour).UnixNano(),
					Values: 9.0,
				},
			},
			exp: []*rawQueryMapOutput{
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(24 * time.Hour).UnixNano(),
					Values: 3.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(48 * time.Hour).UnixNano(),
					Values: 2.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(72 * time.Hour).UnixNano(),
					Values: 4.0,
				},
			},
		},
		{
			name:     "12h interval",
			fn:       "derivative",
			interval: "12h",
			in: []*rawQueryMapOutput{
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).UnixNano(),
					Values: 1.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(24 * time.Hour).UnixNano(),
					Values: 2.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(48 * time.Hour).UnixNano(),
					Values: 3.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(72 * time.Hour).UnixNano(),
					Values: 4.0,
				},
			},
			exp: []*rawQueryMapOutput{
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(24 * time.Hour).UnixNano(),
					Values: 0.5,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(48 * time.Hour).UnixNano(),
					Values: 0.5,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(72 * time.Hour).UnixNano(),
					Values: 0.5,
				},
			},
		},
		{
			name:     "negative derivatives",
			fn:       "derivative",
			interval: "1d",
			in: []*rawQueryMapOutput{
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Unix(),
					Values: 1.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(24 * time.Hour).UnixNano(),
					Values: 2.0,
				},
				// should go negative
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(48 * time.Hour).UnixNano(),
					Values: 0.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(72 * time.Hour).UnixNano(),
					Values: 4.0,
				},
			},
			exp: []*rawQueryMapOutput{
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(24 * time.Hour).UnixNano(),
					Values: 1.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(48 * time.Hour).UnixNano(),
					Values: -2.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(72 * time.Hour).UnixNano(),
					Values: 4.0,
				},
			},
		},
		{
			name:     "negative derivatives",
			fn:       "non_negative_derivative",
			interval: "1d",
			in: []*rawQueryMapOutput{
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Unix(),
					Values: 1.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(24 * time.Hour).UnixNano(),
					Values: 2.0,
				},
				// should go negative
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(48 * time.Hour).UnixNano(),
					Values: 0.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(72 * time.Hour).UnixNano(),
					Values: 4.0,
				},
			},
			exp: []*rawQueryMapOutput{
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(24 * time.Hour).UnixNano(),
					Values: 1.0,
				},
				&rawQueryMapOutput{
					Time:   time.Unix(0, 0).Add(72 * time.Hour).UnixNano(),
					Values: 4.0,
				},
			},
		},
	}

	for _, test := range tests {
		m := derivativeJob(t, test.fn, test.interval)
		got := m.processRawQueryDerivative(nil, test.in)

		if len(got) != len(test.exp) {
			t.Fatalf("processRawQueryDerivative(%s) - %s\nlen mismatch: got %d, exp %d", test.fn, test.name, len(got), len(test.exp))
		}

		for i := 0; i < len(test.exp); i++ {
			if test.exp[i].Time != got[i].Time || math.Abs((test.exp[i].Values.(float64)-got[i].Values.(float64))) > 0.0000001 {
				t.Fatalf("processRawQueryDerivative - %s results mismatch:\ngot %v\nexp %v", test.name, got, test.exp)
			}
		}
	}
}
