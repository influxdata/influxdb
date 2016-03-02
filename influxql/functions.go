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
		Value:      float64(r.sum) / float64(r.count),
		Aggregated: r.count,
	}
}
