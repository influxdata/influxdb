package influxql

// All aggregate and query functions are defined in this file along with any intermediate data objects they need to process.
// Query functions are represented as two discreet functions: Map and Reduce. These roughly follow the MapReduce
// paradigm popularized by Google and Hadoop.
//
// When adding an aggregate function, define a mapper, a reducer, and add them in the switch statement in the MapReduceFuncs function

import (
	"math"
	"sort"
)

// how many values we will map before emitting
const emitBatchSize = 1000

// MapFunc represents a function used for mapping iterators.
type MapFunc func(Iterator, *Emitter, int64)

// ReduceFunc represents a function used for reducing mapper output.
type ReduceFunc func(int64, []interface{}, *Emitter)

// MapReduceFuncs take an aggregate call and returns the map and reduce functions needed to calculate it
func MapReduceFuncs(c *Call) (MapFunc, ReduceFunc, error) {
	// Ensure that there is either a single argument or if for percentile, two
	if c.Name == "percentile" {
		if len(c.Args) != 2 {
			return nil, nil, fmt.Errorf("expected two arguments for percentile()")
		}
	} else if len(c.Args) != 1 {
		return nil, nil, fmt.Errorf("expected one argument for %s()", c.Name)
	}

	// Ensure the argument is a variable reference.
	ref, ok := c.Args[0].(*VarRef)
	if !ok {
		return nil, fmt.Errorf("expected field argument in %s()", c.Name)
	}

	// Retrieve map & reduce functions by name.
	var mapFn MapFunc
	var reduceFn ReduceFunc
	switch strings.ToLower(c.Name) {
	case "count":
		mapFn, reduceFn = MapCount, ReduceSum
	case "sum":
		mapFn, reduceFn = MapSum, ReduceSum
	case "mean":
		mapFn, reduceFn = MapMean, ReduceMean
	case "min":
		mapFn, reduceFn = MapMin, ReduceMin
	case "max":
		mapFn, reduceFn = MapMax, ReduceMax
	case "spread":
		mapFn, reduceFn = MapSpread, ReduceSpread
	case "stddev":
		mapFn, reduceFn = MapStddev, ReduceStddev
	case "first":
		mapFn, reduceFn = MapFirst, ReduceFirst
	case "last":
		mapFn, reduceFn = MapLast, ReduceLast
	case "percentile":
		lit, ok := c.Args[1].(*NumberLiteral)
		if !ok {
			return nil, nil, fmt.Errorf("expected float argument in percentile()")
		}
		mapFn, reduceFn = MapEcho, ReducePercentile(lit.Val)
	default:
		return nil, nil, fmt.Errorf("function not found: %q", c.Name)
	}

	return mapFn, reduceFn, nil
}

// MapCount computes the number of values in an iterator.
func MapCount(itr Iterator, e *Emitter, tmin int64) {
	n := 0
	for k, _, _ := itr.Next(); k != 0; k, _, _ = itr.Next() {
		n++
	}
	e.Emit(Key{tmin, itr.Tags()}, float64(n))
}

// MapSum computes the summation of values in an iterator.
func MapSum(itr Iterator, e *Emitter, tmin int64) {
	n := float64(0)
	for k, _, v := itr.Next(); k != 0; k, _, v = itr.Next() {
		n += v.(float64)
	}
	warn("- ", tmin, itr.Tags())
	e.Emit(Key{tmin, itr.Tags()}, n)
}

// ReduceSum computes the sum of values for each key.
func ReduceSum(key Key, values []interface{}, e *Emitter) {
	var n float64
	for _, v := range values {
		n += v.(float64)
	}
	e.Emit(key, n)
}

// MapMean computes the count and sum of values in an iterator to be combined by the reducer.
func MapMean(itr Iterator, e *Emitter, tmin int64) {
	out := &meanMapOutput{}

	for k, _, v := itr.Next(); k != 0; k, _, v = itr.Next() {
		out.Count++
		out.Sum += v.(float64)
	}
	e.Emit(Key{tmin, itr.Tags()}, out)
}

type meanMapOutput struct {
	Count int
	Sum   float64
}

// ReduceMean computes the mean of values for each key.
func ReduceMean(key Key, values []interface{}, e *Emitter) {
	out := &meanMapOutput{}
	for _, v := range values {
		val := v.(*meanMapOutput)
		out.Count += val.Count
		out.Sum += val.Sum
	}
	e.Emit(key, out.Sum/float64(out.Count))
}

// MapMin collects the values to pass to the reducer
func MapMin(itr Iterator, e *Emitter, tmin int64) {
	var min float64
	pointsYielded := false

	for k, _, v := itr.Next(); k != 0; k, _, v = itr.Next() {
		val := v.(float64)
		// Initialize min
		if !pointsYielded {
			min = val
			pointsYielded = true
		}
		min = math.Min(min, val)
	}
	if pointsYielded {
		e.Emit(Key{tmin, itr.Tags()}, min)
	}
}

// ReduceMin computes the min of value.
func ReduceMin(key Key, values []interface{}, e *Emitter) {
	var min float64
	pointsYielded := false

	for _, v := range values {
		val := v.(float64)
		// Initialize min
		if !pointsYielded {
			min = val
			pointsYielded = true
		}
		m := math.Min(min, val)
		min = m
	}
	if pointsYielded {
		e.Emit(key, min)
	}
}

// MapMax collects the values to pass to the reducer
func MapMax(itr Iterator, e *Emitter, tmax int64) {
	var max float64
	pointsYielded := false

	for k, _, v := itr.Next(); k != 0; k, _, v = itr.Next() {
		val := v.(float64)
		// Initialize max
		if !pointsYielded {
			max = val
			pointsYielded = true
		}
		max = math.Max(max, val)
	}
	if pointsYielded {
		e.Emit(Key{tmax, itr.Tags()}, max)
	}
}

// ReduceMax computes the max of value.
func ReduceMax(key Key, values []interface{}, e *Emitter) {
	var max float64
	pointsYielded := false

	for _, v := range values {
		val := v.(float64)
		// Initialize max
		if !pointsYielded {
			max = val
			pointsYielded = true
		}
		max = math.Max(max, val)
	}
	if pointsYielded {
		e.Emit(key, max)
	}
}

type spreadMapOutput struct {
	Min, Max float64
}

// MapSpread collects the values to pass to the reducer
func MapSpread(itr Iterator, e *Emitter, tmax int64) {
	var out spreadMapOutput
	pointsYielded := false

	for k, _, v := itr.Next(); k != 0; k, _, v = itr.Next() {
		val := v.(float64)
		// Initialize
		if !pointsYielded {
			out.Max = val
			out.Min = val
			pointsYielded = true
		}
		out.Max = math.Max(out.Max, val)
		out.Min = math.Min(out.Min, val)
	}
	if pointsYielded {
		e.Emit(Key{tmax, itr.Tags()}, out)
	}
}

// ReduceSpread computes the spread of values.
func ReduceSpread(key Key, values []interface{}, e *Emitter) {
	var result spreadMapOutput
	pointsYielded := false

	for _, v := range values {
		val := v.(spreadMapOutput)
		// Initialize
		if !pointsYielded {
			result.Max = val.Max
			result.Min = val.Min
			pointsYielded = true
		}
		result.Max = math.Max(result.Max, val.Max)
		result.Min = math.Min(result.Min, val.Min)
	}
	if pointsYielded {
		e.Emit(key, result.Max-result.Min)
	}
}

// MapStddev collects the values to pass to the reducer
func MapStddev(itr Iterator, e *Emitter, tmax int64) {
	var values []float64

	for k, _, v := itr.Next(); k != 0; k, _, v = itr.Next() {
		values = append(values, v.(float64))
		// Emit in batches.
		// unbounded emission of data can lead to excessive memory use
		// or other potential performance problems.
		if len(values) == emitBatchSize {
			e.Emit(Key{tmax, itr.Tags()}, values)
			values = []float64{}
		}
	}
	if len(values) > 0 {
		e.Emit(Key{tmax, itr.Tags()}, values)
	}
}

// ReduceStddev computes the stddev of values.
func ReduceStddev(key Key, values []interface{}, e *Emitter) {
	var data []float64
	// Collect all the data points
	for _, value := range values {
		data = append(data, value.([]float64)...)
	}
	// If no data, leave
	if len(data) == 0 {
		return
	}
	// If we only have one data point, the std dev is undefined
	if len(data) == 1 {
		e.Emit(key, "undefined")
		return
	}
	// Get the sum
	var sum float64
	for _, v := range data {
		sum += v
	}
	// Get the mean
	mean := sum / float64(len(data))
	// Get the variance
	var variance float64
	for _, v := range data {
		dif := v - mean
		sq := math.Pow(dif, 2)
		variance += sq
	}
	variance = variance / float64(len(data)-1)
	stddev := math.Sqrt(variance)

	e.Emit(key, stddev)
}

type firstLastMapOutput struct {
	Time int64
	Val  interface{}
}

// MapFirst collects the values to pass to the reducer
func MapFirst(itr Iterator, e *Emitter, tmax int64) {
	out := firstLastMapOutput{}
	pointsYielded := false

	for k, _, v := itr.Next(); k != 0; k, _, v = itr.Next() {
		// Initialize first
		if !pointsYielded {
			out.Time = k
			out.Val = v
			pointsYielded = true
		}
		if k < out.Time {
			out.Time = k
			out.Val = v
		}
	}
	if pointsYielded {
		e.Emit(Key{tmax, itr.Tags()}, out)
	}
}

// ReduceFirst computes the first of value.
func ReduceFirst(key Key, values []interface{}, e *Emitter) {
	out := firstLastMapOutput{}
	pointsYielded := false

	for _, v := range values {
		val := v.(firstLastMapOutput)
		// Initialize first
		if !pointsYielded {
			out.Time = val.Time
			out.Val = val.Val
			pointsYielded = true
		}
		if val.Time < out.Time {
			out.Time = val.Time
			out.Val = val.Val
		}
	}
	if pointsYielded {
		e.Emit(key, out.Val)
	}
}

// MapLast collects the values to pass to the reducer
func MapLast(itr Iterator, e *Emitter, tmax int64) {
	out := firstLastMapOutput{}
	pointsYielded := false

	for k, _, v := itr.Next(); k != 0; k, _, v = itr.Next() {
		// Initialize last
		if !pointsYielded {
			out.Time = k
			out.Val = v
			pointsYielded = true
		}
		if k > out.Time {
			out.Time = k
			out.Val = v
		}
	}
	if pointsYielded {
		e.Emit(Key{tmax, itr.Tags()}, out)
	}
}

// ReduceLast computes the last of value.
func ReduceLast(key Key, values []interface{}, e *Emitter) {
	out := firstLastMapOutput{}
	pointsYielded := false

	for _, v := range values {
		val := v.(firstLastMapOutput)
		// Initialize last
		if !pointsYielded {
			out.Time = val.Time
			out.Val = val.Val
			pointsYielded = true
		}
		if val.Time > out.Time {
			out.Time = val.Time
			out.Val = val.Val
		}
	}
	if pointsYielded {
		e.Emit(key, out.Val)
	}
}

// MapEcho emits the data points for each group by interval
func MapEcho(itr Iterator, e *Emitter, tmin int64) {
	var values []interface{}

	for k, _, v := itr.Next(); k != 0; k, _, v = itr.Next() {
		values = append(values, v)
	}
	e.Emit(Key{tmin, itr.Tags()}, values)
}

// ReducePercentile computes the percentile of values for each key.
func ReducePercentile(percentile float64) ReduceFunc {
	return func(key Key, values []interface{}, e *Emitter) {
		var allValues []float64

		for _, v := range values {
			vals := v.([]interface{})
			for _, v := range vals {
				allValues = append(allValues, v.(float64))
			}
		}

		sort.Float64s(allValues)
		length := len(allValues)
		index := int(math.Floor(float64(length)*percentile/100.0+0.5)) - 1

		if index < 0 || index >= len(allValues) {
			e.Emit(key, 0.0)
		}

		e.Emit(key, allValues[index])
	}
}

// TODO: make this more efficient to stream data.
func MapRawQuery(fields []*Field) MapFunc {
	return func(itr Iterator, e *Emitter, tmin int64) {
		var values []*rawQueryMapOutput

		for k, d, v := itr.Next(); k != 0; k, d, v = itr.Next() {
			values = append(values, &rawQueryMapOutput{k, d, v})
			// Emit in batches.
			// unbounded emission of data can lead to excessive memory use
			// or other potential performance problems.
			if len(values) == emitBatchSize {
				e.Emit(Key{0, itr.Tags()}, values)
				values = []*rawQueryMapOutput{}
			}
		}
		if len(values) > 0 {
			e.Emit(Key{0, itr.Tags()}, values)
		}
	}
}

type rawQueryMapOutput struct {
	timestamp int64
	data      []byte
	value     interface{}
}
type rawOutputs []*rawQueryMapOutput

func (a rawOutputs) Len() int           { return len(a) }
func (a rawOutputs) Less(i, j int) bool { return a[i].timestamp < a[j].timestamp }
func (a rawOutputs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// TODO: make this streaming so it doesn't buffer everything in memory
func ReduceRawQuery(fields []*Field) ReduceFunc {
	return func(key Key, values []interface{}, e *Emitter) {
		var a []*rawQueryMapOutput
		for _, v := range values {
			a = append(a, v.([]*rawQueryMapOutput)...)
		}
		if len(a) > 0 {
			sort.Sort(rawOutputs(a))
			for _, o := range a {
				e.Emit(Key{o.timestamp, key.Values}, o.value)
			}
		}
	}
}
