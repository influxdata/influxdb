package main

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func TestScrubValues(t *testing.T) {
	dummy := Converter{
		tracker: new(tracker),
	}

	epoch := time.Unix(0, 0)
	simple := []tsm1.Value{tsm1.NewValue(epoch, 1.0)}

	for _, tt := range []struct {
		input, expected []tsm1.Value
	}{
		{
			input:    simple,
			expected: simple,
		}, {
			input:    []tsm1.Value{simple[0], tsm1.NewValue(epoch, math.NaN())},
			expected: simple,
		}, {
			input:    []tsm1.Value{simple[0], tsm1.NewValue(epoch, math.Inf(-1))},
			expected: simple,
		}, {
			input:    []tsm1.Value{simple[0], tsm1.NewValue(epoch, math.Inf(1)), tsm1.NewValue(epoch, math.NaN())},
			expected: simple,
		},
	} {
		out := dummy.scrubValues(tt.input)
		if !reflect.DeepEqual(out, tt.expected) {
			t.Errorf("Failed to scrub '%s': Got '%s', Expected '%s'", pretty(tt.input), pretty(out), pretty(tt.expected))
		}
	}
}

func pretty(vals []tsm1.Value) string {
	if len(vals) == 0 {
		return "[]"
	}

	strs := make([]string, len(vals))

	for i := range vals {
		strs[i] = fmt.Sprintf("{%v: %v}", vals[i].UnixNano(), vals[i].Value())
	}

	return "[" + strings.Join(strs, ", ") + "]"
}
