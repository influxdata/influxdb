package influxql

// FloatMeanReducer calculates the mean of the aggregated points.
type FloatMeanReducer struct {
	sum   float64
	count uint32
}

// NewFloatMeanReducer creates a new FloatMeanReducer.
func NewFloatMeanReducer() *FloatMeanReducer {
	return &FloatMeanReducer{}
}

// AggregateFloat aggregates a point into the reducer.
func (r *FloatMeanReducer) AggregateFloat(p *FloatPoint) {
	if p.Aggregated >= 2 {
		r.sum += p.Value * float64(p.Aggregated)
		r.count += p.Aggregated
	} else {
		r.sum += p.Value
		r.count++
	}
}

// Emit emits the mean of the aggregated points as a single point.
func (r *FloatMeanReducer) Emit() []FloatPoint {
	return []FloatPoint{{
		Time:       ZeroTime,
		Value:      r.sum / float64(r.count),
		Aggregated: r.count,
	}}
}

// IntegerMeanReducer calculates the mean of the aggregated points.
type IntegerMeanReducer struct {
	sum   int64
	count uint32
}

// NewIntegerMeanReducer creates a new IntegerMeanReducer.
func NewIntegerMeanReducer() *IntegerMeanReducer {
	return &IntegerMeanReducer{}
}

// AggregateInteger aggregates a point into the reducer.
func (r *IntegerMeanReducer) AggregateInteger(p *IntegerPoint) {
	if p.Aggregated >= 2 {
		r.sum += p.Value * int64(p.Aggregated)
		r.count += p.Aggregated
	} else {
		r.sum += p.Value
		r.count++
	}
}

// Emit emits the mean of the aggregated points as a single point.
func (r *IntegerMeanReducer) Emit() []FloatPoint {
	return []FloatPoint{{
		Time:       ZeroTime,
		Value:      float64(r.sum) / float64(r.count),
		Aggregated: r.count,
	}}
}

// FloatMovingAverageReducer calculates the moving average of the aggregated points.
type FloatMovingAverageReducer struct {
	pos  int
	sum  float64
	time int64
	buf  []float64
}

// NewFloatMovingAverageReducer creates a new FloatMovingAverageReducer.
func NewFloatMovingAverageReducer(n int) *FloatMovingAverageReducer {
	return &FloatMovingAverageReducer{
		buf: make([]float64, 0, n),
	}
}

// AggregateFloat aggregates a point into the reducer and updates the current window.
func (r *FloatMovingAverageReducer) AggregateFloat(p *FloatPoint) {
	if len(r.buf) != cap(r.buf) {
		r.buf = append(r.buf, p.Value)
	} else {
		r.sum -= r.buf[r.pos]
		r.buf[r.pos] = p.Value
	}
	r.sum += p.Value
	r.time = p.Time
	r.pos++
	if r.pos >= cap(r.buf) {
		r.pos = 0
	}
}

// Emit emits the moving average of the current window. Emit should be called
// after every call to AggregateFloat and it will produce one point if there
// is enough data to fill a window, otherwise it will produce zero points.
func (r *FloatMovingAverageReducer) Emit() []FloatPoint {
	if len(r.buf) != cap(r.buf) {
		return []FloatPoint{}
	}
	return []FloatPoint{
		{
			Value:      r.sum / float64(len(r.buf)),
			Time:       r.time,
			Aggregated: uint32(len(r.buf)),
		},
	}
}

// IntegerMovingAverageReducer calculates the moving average of the aggregated points.
type IntegerMovingAverageReducer struct {
	pos  int
	sum  int64
	time int64
	buf  []int64
}

// NewIntegerMovingAverageReducer creates a new IntegerMovingAverageReducer.
func NewIntegerMovingAverageReducer(n int) *IntegerMovingAverageReducer {
	return &IntegerMovingAverageReducer{
		buf: make([]int64, 0, n),
	}
}

// AggregateInteger aggregates a point into the reducer and updates the current window.
func (r *IntegerMovingAverageReducer) AggregateInteger(p *IntegerPoint) {
	if len(r.buf) != cap(r.buf) {
		r.buf = append(r.buf, p.Value)
	} else {
		r.sum -= r.buf[r.pos]
		r.buf[r.pos] = p.Value
	}
	r.sum += p.Value
	r.time = p.Time
	r.pos++
	if r.pos >= cap(r.buf) {
		r.pos = 0
	}
}

// Emit emits the moving average of the current window. Emit should be called
// after every call to AggregateInteger and it will produce one point if there
// is enough data to fill a window, otherwise it will produce zero points.
func (r *IntegerMovingAverageReducer) Emit() []FloatPoint {
	if len(r.buf) != cap(r.buf) {
		return []FloatPoint{}
	}
	return []FloatPoint{
		{
			Value:      float64(r.sum) / float64(len(r.buf)),
			Time:       r.time,
			Aggregated: uint32(len(r.buf)),
		},
	}
}
