package fields

import (
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
)

func makeFields(args ...string) Fields {
	if len(args)%2 != 0 {
		panic("uneven number of arguments")
	}

	var f Fields
	for i := 0; i+1 < len(args); i += 2 {
		f = append(f, String(args[i], args[i+1]))
	}
	return f
}

func TestNew(t *testing.T) {
	cases := []struct {
		n   string
		l   []string
		exp Fields
	}{
		{
			n:   "empty",
			l:   nil,
			exp: makeFields(),
		},
		{
			n:   "not duplicates",
			l:   []string{"k01", "v01", "k03", "v03", "k02", "v02"},
			exp: makeFields("k01", "v01", "k02", "v02", "k03", "v03"),
		},
		{
			n:   "duplicates at end",
			l:   []string{"k01", "v01", "k02", "v02", "k02", "v02"},
			exp: makeFields("k01", "v01", "k02", "v02"),
		},
		{
			n:   "duplicates at start",
			l:   []string{"k01", "v01", "k02", "v02", "k01", "v01"},
			exp: makeFields("k01", "v01", "k02", "v02"),
		},
		{
			n:   "duplicates in middle",
			l:   []string{"k01", "v01", "k02", "v02", "k03", "v03", "k02", "v02", "k02", "v02"},
			exp: makeFields("k01", "v01", "k02", "v02", "k03", "v03"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.n, func(t *testing.T) {
			l := New(makeFields(tc.l...)...)
			assert.Equal(t, tc.exp, l)
		})
	}
}

func TestFields_Merge(t *testing.T) {
	cases := []struct {
		n    string
		l, r Fields
		exp  Fields
	}{
		{
			n:   "no matching keys",
			l:   New(String("k05", "v05"), String("k03", "v03"), String("k01", "v01")),
			r:   New(String("k02", "v02"), String("k04", "v04"), String("k00", "v00")),
			exp: New(String("k05", "v05"), String("k03", "v03"), String("k01", "v01"), String("k02", "v02"), String("k04", "v04"), String("k00", "v00")),
		},
		{
			n:   "multiple matching keys",
			l:   New(String("k05", "v05"), String("k03", "v03"), String("k01", "v01")),
			r:   New(String("k02", "v02"), String("k03", "v03a"), String("k05", "v05a")),
			exp: New(String("k05", "v05a"), String("k03", "v03a"), String("k01", "v01"), String("k02", "v02")),
		},
		{
			n:   "source empty",
			l:   New(),
			r:   New(String("k02", "v02"), String("k04", "v04"), String("k00", "v00")),
			exp: New(String("k02", "v02"), String("k04", "v04"), String("k00", "v00")),
		},
		{
			n:   "other empty",
			l:   New(String("k02", "v02"), String("k04", "v04"), String("k00", "v00")),
			r:   New(),
			exp: New(String("k02", "v02"), String("k04", "v04"), String("k00", "v00")),
		},
	}

	for _, tc := range cases {
		t.Run(tc.n, func(t *testing.T) {
			l := tc.l
			l.Merge(tc.r)
			assert.Equal(t, tc.exp, l)
		})
	}
}
