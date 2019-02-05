package gen

import (
	"math/rand"
)

type floatRandomValuesSequence struct {
	r *rand.Rand
	a float64
	b float64
}

func NewFloatRandomValuesSequence(min, max float64, r *rand.Rand) FloatValuesSequence {
	return &floatRandomValuesSequence{r: r, a: max - min, b: min}
}

func (g *floatRandomValuesSequence) Reset() {}

func (g *floatRandomValuesSequence) Write(vs []float64) {
	var (
		a = g.a
		b = g.b
	)
	for i := 0; i < len(vs); i++ {
		vs[i] = a*g.r.Float64() + b // ax + b
	}
}

type integerRandomValuesSequence struct {
	r *rand.Zipf
}

// NewIntegerZipfValuesSequence produces int64 values using a Zipfian distribution
// described by s.
func NewIntegerZipfValuesSequence(s *FieldIntegerZipfSource) IntegerValuesSequence {
	r := rand.New(rand.NewSource(s.Seed))
	return &integerRandomValuesSequence{r: rand.NewZipf(r, s.S, s.V, s.IMAX)}
}

func (g *integerRandomValuesSequence) Reset() {}

func (g *integerRandomValuesSequence) Write(vs []int64) {
	for i := 0; i < len(vs); i++ {
		vs[i] = int64(g.r.Uint64())
	}
}
