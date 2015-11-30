package tsdb

import (
	"testing"
	"time"
)

func TestCompatDurationUnmarshalText(t *testing.T) {
	tests := []struct {
		in  string
		out time.Duration
	}{
		{
			"",
			time.Duration(0)},
		{
			"0",
			time.Duration(0),
		},
		{
			"1",
			1 * time.Second,
		},
		{
			"150ms",
			150 * time.Millisecond,
		},
		{
			"10s",
			10 * time.Second,
		},
	}

	for ii, tt := range tests {
		var x compatDuration
		x.UnmarshalText([]byte(tt.in))
		if time.Duration(x) != tt.out {
			t.Errorf("[%d] compatDuration.UnmarshalText(%#v) = %v, expected %v", ii, tt.in, x, tt.out)
		}
	}
}
