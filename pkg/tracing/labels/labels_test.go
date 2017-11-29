package labels

import (
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
)

func makeLabels(args ...string) Labels {
	if len(args)%2 != 0 {
		panic("uneven number of arguments")
	}

	var l Labels
	for i := 0; i+1 < len(args); i += 2 {
		l = append(l, Label{Key: args[i], Value: args[i+1]})
	}
	return l
}

func TestNew(t *testing.T) {
	cases := []struct {
		n   string
		l   []string
		exp Labels
	}{
		{
			n:   "empty",
			l:   nil,
			exp: makeLabels(),
		},
		{
			n:   "not duplicates",
			l:   []string{"k01", "v01", "k03", "v03", "k02", "v02"},
			exp: makeLabels("k01", "v01", "k02", "v02", "k03", "v03"),
		},
		{
			n:   "duplicates at end",
			l:   []string{"k01", "v01", "k02", "v02", "k02", "v02"},
			exp: makeLabels("k01", "v01", "k02", "v02"),
		},
		{
			n:   "duplicates at start",
			l:   []string{"k01", "v01", "k02", "v02", "k01", "v01"},
			exp: makeLabels("k01", "v01", "k02", "v02"),
		},
		{
			n:   "duplicates in middle",
			l:   []string{"k01", "v01", "k02", "v02", "k03", "v03", "k02", "v02", "k02", "v02"},
			exp: makeLabels("k01", "v01", "k02", "v02", "k03", "v03"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.n, func(t *testing.T) {
			l := New(tc.l...)
			assert.Equal(t, l, tc.exp)
		})
	}
}

func TestLabels_Merge(t *testing.T) {
	cases := []struct {
		n    string
		l, r Labels
		exp  Labels
	}{
		{
			n:   "no matching keys",
			l:   New("k05", "v05", "k03", "v03", "k01", "v01"),
			r:   New("k02", "v02", "k04", "v04", "k00", "v00"),
			exp: New("k05", "v05", "k03", "v03", "k01", "v01", "k02", "v02", "k04", "v04", "k00", "v00"),
		},
		{
			n:   "multiple matching keys",
			l:   New("k05", "v05", "k03", "v03", "k01", "v01"),
			r:   New("k02", "v02", "k03", "v03a", "k05", "v05a"),
			exp: New("k05", "v05a", "k03", "v03a", "k01", "v01", "k02", "v02"),
		},
		{
			n:   "source empty",
			l:   New(),
			r:   New("k02", "v02", "k04", "v04", "k00", "v00"),
			exp: New("k02", "v02", "k04", "v04", "k00", "v00"),
		},
		{
			n:   "other empty",
			l:   New("k02", "v02", "k04", "v04", "k00", "v00"),
			r:   New(),
			exp: New("k02", "v02", "k04", "v04", "k00", "v00"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.n, func(t *testing.T) {
			l := tc.l
			l.Merge(tc.r)
			assert.Equal(t, l, tc.exp)
		})
	}
}
