package gen

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func sg(name, prefix, field string, counts ...int) SeriesGenerator {
	spec := TimeSequenceSpec{Count: 1, Start: time.Unix(0, 0), Delta: time.Second}
	ts := NewTimestampSequenceFromSpec(spec)
	vs := NewFloatConstantValuesSequence(1)
	vg := NewTimeFloatValuesSequence(spec.Count, ts, vs)
	return NewSeriesGenerator([]byte(name), []byte(field), vg, NewTagsValuesSequenceCounts(prefix, counts))
}

func tags(sb *strings.Builder, prefix string, vals []int) {
	sb.WriteByte(',')

	// max tag width
	tw := int(math.Ceil(math.Log10(float64(len(vals)))))
	tf := fmt.Sprintf("%s%%0%dd=value%%d", prefix, tw)
	tvs := make([]string, len(vals))
	for i := range vals {
		tvs[i] = fmt.Sprintf(tf, i, vals[i])
	}
	sb.WriteString(strings.Join(tvs, ","))
}

func line(name, prefix, field string, vals ...int) string {
	var sb strings.Builder
	sb.WriteString(name)
	tags(&sb, prefix, vals)
	sb.WriteString("#!~#")
	sb.WriteString(field)
	return sb.String()
}

func seriesGeneratorString(sg SeriesGenerator) []string {
	var lines []string
	for sg.Next() {
		lines = append(lines, fmt.Sprintf("%s#!~#%s", string(sg.Key()), string(sg.Field())))
	}
	return lines
}

func TestNewMergedSeriesGenerator(t *testing.T) {
	tests := []struct {
		n   string
		s   []SeriesGenerator
		exp []string
	}{
		{
			n: "single",
			s: []SeriesGenerator{
				sg("cpu", "t", "f0", 2, 1),
			},
			exp: []string{
				line("cpu", "t", "f0", 0, 0),
				line("cpu", "t", "f0", 1, 0),
			},
		},
		{
			n: "multiple,interleaved",
			s: []SeriesGenerator{
				sg("cpu", "t", "f0", 2, 1),
				sg("cpu", "t", "f1", 2, 1),
			},
			exp: []string{
				line("cpu", "t", "f0", 0, 0),
				line("cpu", "t", "f1", 0, 0),
				line("cpu", "t", "f0", 1, 0),
				line("cpu", "t", "f1", 1, 0),
			},
		},
		{
			n: "multiple,sequential",
			s: []SeriesGenerator{
				sg("cpu", "t", "f0", 2),
				sg("cpu", "u", "f0", 2, 1),
			},
			exp: []string{
				line("cpu", "t", "f0", 0),
				line("cpu", "t", "f0", 1),
				line("cpu", "u", "f0", 0, 0),
				line("cpu", "u", "f0", 1, 0),
			},
		},
		{
			n: "multiple,sequential",
			s: []SeriesGenerator{
				sg("m1", "t", "f0", 2, 1),
				sg("m0", "t", "f0", 2, 1),
			},
			exp: []string{
				line("m0", "t", "f0", 0, 0),
				line("m0", "t", "f0", 1, 0),
				line("m1", "t", "f0", 0, 0),
				line("m1", "t", "f0", 1, 0),
			},
		},
		{
			// ensure duplicates are removed
			n: "duplicates",
			s: []SeriesGenerator{
				sg("cpu", "t", "f0", 2, 1),
				sg("cpu", "t", "f0", 2, 1),
			},
			exp: []string{
				line("cpu", "t", "f0", 0, 0),
				line("cpu", "t", "f0", 1, 0),
			},
		},
		{
			// ensure duplicates are removed, but non-dupes from same SeriesGenerator
			// are still included
			n: "duplicates,multiple,interleaved",
			s: []SeriesGenerator{
				sg("cpu", "t", "f0", 2, 1),
				sg("cpu", "t", "f1", 2, 1),
				sg("cpu", "t", "f0", 2, 1),
				sg("cpu", "t", "f1", 3, 1),
			},
			exp: []string{
				line("cpu", "t", "f0", 0, 0),
				line("cpu", "t", "f1", 0, 0),
				line("cpu", "t", "f0", 1, 0),
				line("cpu", "t", "f1", 1, 0),
				line("cpu", "t", "f1", 2, 0),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.n, func(t *testing.T) {
			sg := NewMergedSeriesGenerator(tt.s)
			if got := seriesGeneratorString(sg); !cmp.Equal(got, tt.exp) {
				t.Errorf("unpexected -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}

func TestNewMergedSeriesGeneratorLimit(t *testing.T) {
	tests := []struct {
		n   string
		s   []SeriesGenerator
		lim int64
		exp []string
	}{
		{
			n: "single",
			s: []SeriesGenerator{
				sg("cpu", "t", "f0", 4, 1),
			},
			lim: 2,
			exp: []string{
				line("cpu", "t", "f0", 0, 0),
				line("cpu", "t", "f0", 1, 0),
			},
		},
		{
			n: "multiple,interleaved",
			s: []SeriesGenerator{
				sg("cpu", "t", "f0", 2, 1),
				sg("cpu", "t", "f1", 2, 1),
			},
			lim: 3,
			exp: []string{
				line("cpu", "t", "f0", 0, 0),
				line("cpu", "t", "f1", 0, 0),
				line("cpu", "t", "f0", 1, 0),
			},
		},
		{
			n: "multiple,sequential",
			s: []SeriesGenerator{
				sg("cpu", "t", "f0", 2),
				sg("cpu", "u", "f0", 2, 1),
			},
			lim: 2,
			exp: []string{
				line("cpu", "t", "f0", 0),
				line("cpu", "t", "f0", 1),
			},
		},
		{
			n: "multiple,sequential",
			s: []SeriesGenerator{
				sg("m1", "t", "f0", 2, 1),
				sg("m0", "t", "f0", 2, 1),
			},
			lim: 4,
			exp: []string{
				line("m0", "t", "f0", 0, 0),
				line("m0", "t", "f0", 1, 0),
				line("m1", "t", "f0", 0, 0),
				line("m1", "t", "f0", 1, 0),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.n, func(t *testing.T) {
			sg := NewMergedSeriesGeneratorLimit(tt.s, tt.lim)
			if got := seriesGeneratorString(sg); !cmp.Equal(got, tt.exp) {
				t.Errorf("unpexected -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}
