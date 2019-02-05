package gen

type TimestampSequence interface {
	Reset()
	Write(ts []int64)
}

type timestampSequence struct {
	t     int64
	start int64
	delta int64
}

func NewTimestampSequenceFromSpec(spec TimeSequenceSpec) TimestampSequence {
	return &timestampSequence{
		t:     spec.Start.UnixNano(),
		start: spec.Start.UnixNano(),
		delta: int64(spec.Delta),
	}
}

func (g *timestampSequence) Reset() {
	g.t = g.start
}

func (g *timestampSequence) Write(ts []int64) {
	var (
		t = g.t
		d = g.delta
	)
	for i := 0; i < len(ts); i++ {
		ts[i] = t
		t += d
	}
	g.t = t
}
