package gen

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCompareSeries(t *testing.T) {
	mk := func(k, f string) seriesKeyField {
		return &constSeries{key: []byte(k), field: []byte(f)}
	}

	tests := []struct {
		name string
		a    seriesKeyField
		b    seriesKeyField
		exp  int
	}{
		{
			name: "nil a,b",
			exp:  0,
		},
		{
			name: "a(nil) < b",
			a:    nil,
			b:    mk("cpu,t0=v0", "f0"),
			exp:  -1,
		},
		{
			name: "a > b(nil)",
			a:    mk("cpu,t0=v0", "f0"),
			b:    nil,
			exp:  1,
		},
		{
			name: "a = b",
			a:    mk("cpu,t0=v0", "f0"),
			b:    mk("cpu,t0=v0", "f0"),
			exp:  0,
		},
		{
			name: "a(f0) < b(f1)",
			a:    mk("cpu,t0=v0", "f0"),
			b:    mk("cpu,t0=v0", "f1"),
			exp:  -1,
		},
		{
			name: "a(v0) < b(v1)",
			a:    mk("cpu,t0=v0", "f0"),
			b:    mk("cpu,t0=v1", "f0"),
			exp:  -1,
		},
		{
			name: "a(f1) > b(f0)",
			a:    mk("cpu,t0=v0", "f1"),
			b:    mk("cpu,t0=v0", "f0"),
			exp:  1,
		},
		{
			name: "a(v1) > b(v0)",
			a:    mk("cpu,t0=v1", "f0"),
			b:    mk("cpu,t0=v0", "f0"),
			exp:  1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CompareSeries(tt.a, tt.b); got != tt.exp {
				t.Errorf("unexpected value -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}
