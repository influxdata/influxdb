package influxql

type FloatMeanReducer struct {
	sum   float64
	count uint32
}

func NewFloatMeanReducer() *FloatMeanReducer {
	return &FloatMeanReducer{}
}

func (r *FloatMeanReducer) Aggregate(p *FloatPoint) {
	if p.Aggregated >= 2 {
		r.sum += p.Value * float64(p.Aggregated)
		r.count += p.Aggregated
	} else {
		r.sum += p.Value
		r.count++
	}
}

func (r *FloatMeanReducer) Emit() *FloatPoint {
	return &FloatPoint{
		Time:       ZeroTime,
		Value:      r.sum / float64(r.count),
		Aggregated: r.count,
	}
}

type IntegerMeanReducer struct {
	sum   int64
	count uint32
}

func NewIntegerMeanReducer() *IntegerMeanReducer {
	return &IntegerMeanReducer{}
}

func (r *IntegerMeanReducer) Aggregate(p *IntegerPoint) {
	if p.Aggregated >= 2 {
		r.sum += p.Value * int64(p.Aggregated)
		r.count += p.Aggregated
	} else {
		r.sum += p.Value
		r.count++
	}
}

func (r *IntegerMeanReducer) Emit() *FloatPoint {
	return &FloatPoint{
		Time:       ZeroTime,
		Value:      float64(r.sum) / float64(r.count),
		Aggregated: r.count,
	}
}

type IntegerSliceFloatFuncReducer struct {
	slice []IntegerPoint
	fn    IntegerReduceSliceFloatFunc
}

// NewIntegerDerivativeReducer returns the derivative value within a window.
func NewIntegerSliceFloatFuncReducer(fn IntegerReduceSliceFloatFunc) *IntegerSliceFloatFuncReducer {
	return &IntegerSliceFloatFuncReducer{
		fn: fn,
	}
}

func (r *IntegerSliceFloatFuncReducer) Aggregate(a IntegerPoint) {
	r.slice = append(r.slice, a)
}

func (r *IntegerSliceFloatFuncReducer) AggregateSlice(a []IntegerPoint) {
	r.slice = a
}

func (r *IntegerSliceFloatFuncReducer) Emit(opt *ReduceOptions) []FloatPoint {
	return r.fn(r.slice, opt)
}
