package influxql

import (
	"fmt"
	"math"
	"sort"
)

/*
This file contains iterator implementations for each function call available
in InfluxQL. Call iterators are separated into two groups:

1. Map/reduce-style iterators - these are passed to IteratorCreator so that
   processing can be at the low-level storage and aggregates are returned.

2. Raw aggregate iterators - these require the full set of data for a window.
   These are handled by the select() function and raw points are streamed in
   from the low-level storage.

There are helpers to aid in building aggregate iterators. For simple map/reduce
iterators, you can use the reduceIterator types and pass a reduce function. This
reduce function is passed a previous and current value and the new timestamp,
value, and auxilary fields are returned from it.

For raw aggregate iterators, you can use the reduceSliceIterators which pass
in a slice of all points to the function and return a point. For more complex
iterator types, you may need to create your own iterators by hand.

Once your iterator is complete, you'll need to add it to the NewCallIterator()
function if it is to be available to IteratorCreators and add it to the select()
function to allow it to be included during planning.
*/

// NewCallIterator returns a new iterator for a Call.
func NewCallIterator(input Iterator, opt IteratorOptions) Iterator {
	name := opt.Expr.(*Call).Name
	switch name {
	case "count":
		return newCountIterator(input, opt)
	case "min":
		return newMinIterator(input, opt)
	case "max":
		return newMaxIterator(input, opt)
	case "sum":
		return newSumIterator(input, opt)
	case "first":
		return newFirstIterator(input, opt)
	case "last":
		return newLastIterator(input, opt)
	default:
		panic(fmt.Sprintf("unsupported function call: %s", name))
	}
}

// newCountIterator returns an iterator for operating on a count() call.
func newCountIterator(input Iterator, opt IteratorOptions) Iterator {
	// FIXME: Wrap iterator in int-type iterator and always output int value.

	switch input := input.(type) {
	case FloatIterator:
		return &floatReduceIterator{input: newBufFloatIterator(input), opt: opt, fn: floatCountReduce}
	default:
		panic(fmt.Sprintf("unsupported count iterator type: %T", input))
	}
}

// floatCountReduce returns the count of points.
func floatCountReduce(prev, curr *FloatPoint, opt *reduceOptions) (int64, float64, []interface{}) {
	if prev == nil {
		return opt.startTime, 1, nil
	}
	return prev.Time, prev.Value + 1, nil
}

// newMinIterator returns an iterator for operating on a min() call.
func newMinIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return &floatReduceIterator{input: newBufFloatIterator(input), opt: opt, fn: floatMinReduce}
	default:
		panic(fmt.Sprintf("unsupported min iterator type: %T", input))
	}
}

// floatMinReduce returns the minimum value between prev & curr.
func floatMinReduce(prev, curr *FloatPoint, opt *reduceOptions) (int64, float64, []interface{}) {
	if prev == nil || curr.Value < prev.Value || (curr.Value == prev.Value && curr.Time < prev.Time) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// newMaxIterator returns an iterator for operating on a max() call.
func newMaxIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return &floatReduceIterator{input: newBufFloatIterator(input), opt: opt, fn: floatMaxReduce}
	default:
		panic(fmt.Sprintf("unsupported max iterator type: %T", input))
	}
}

// floatMaxReduce returns the maximum value between prev & curr.
func floatMaxReduce(prev, curr *FloatPoint, opt *reduceOptions) (int64, float64, []interface{}) {
	if prev == nil || curr.Value > prev.Value || (curr.Value == prev.Value && curr.Time < prev.Time) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// newSumIterator returns an iterator for operating on a sum() call.
func newSumIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return &floatReduceIterator{input: newBufFloatIterator(input), opt: opt, fn: floatSumReduce}
	default:
		panic(fmt.Sprintf("unsupported sum iterator type: %T", input))
	}
}

// floatSumReduce returns the sum prev value & curr value.
func floatSumReduce(prev, curr *FloatPoint, opt *reduceOptions) (int64, float64, []interface{}) {
	if prev == nil {
		return opt.startTime, curr.Value, nil
	}
	return opt.startTime, prev.Value + curr.Value, nil
}

// newFirstIterator returns an iterator for operating on a first() call.
func newFirstIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return &floatReduceIterator{input: newBufFloatIterator(input), opt: opt, fn: floatFirstReduce}
	default:
		panic(fmt.Sprintf("unsupported first iterator type: %T", input))
	}
}

// floatFirstReduce returns the first point sorted by time.
func floatFirstReduce(prev, curr *FloatPoint, opt *reduceOptions) (int64, float64, []interface{}) {
	if prev == nil || curr.Time < prev.Time || (curr.Time == prev.Time && curr.Value > prev.Value) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// newLastIterator returns an iterator for operating on a last() call.
func newLastIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return &floatReduceIterator{input: newBufFloatIterator(input), opt: opt, fn: floatLastReduce}
	default:
		panic(fmt.Sprintf("unsupported last iterator type: %T", input))
	}
}

// floatLastReduce returns the last point sorted by time.
func floatLastReduce(prev, curr *FloatPoint, opt *reduceOptions) (int64, float64, []interface{}) {
	if prev == nil || curr.Time > prev.Time || (curr.Time == prev.Time && curr.Value > prev.Value) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// newDistinctIterator returns an iterator for operating on a distinct() call.
func newDistinctIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return &floatReduceSliceIterator{input: newBufFloatIterator(input), opt: opt, fn: floatDistinctReduceSlice}
	case StringIterator:
		return &stringReduceSliceIterator{input: newBufStringIterator(input), opt: opt, fn: stringDistinctReduceSlice}
	default:
		panic(fmt.Sprintf("unsupported distinct iterator type: %T", input))
	}
}

// floatDistinctReduceSlice returns the distinct value within a window.
func floatDistinctReduceSlice(a []FloatPoint, opt *reduceOptions) []FloatPoint {
	m := make(map[float64]FloatPoint)
	for _, p := range a {
		if _, ok := m[p.Value]; !ok {
			m[p.Value] = p
		}
	}

	points := make([]FloatPoint, 0, len(m))
	for _, p := range m {
		points = append(points, FloatPoint{Time: p.Time, Value: p.Value})
	}
	sort.Sort(floatPoints(points))
	return points
}

// stringDistinctReduceSlice returns the distinct value within a window.
func stringDistinctReduceSlice(a []StringPoint, opt *reduceOptions) []StringPoint {
	m := make(map[string]StringPoint)
	for _, p := range a {
		if _, ok := m[p.Value]; !ok {
			m[p.Value] = p
		}
	}

	points := make([]StringPoint, 0, len(m))
	for _, p := range m {
		points = append(points, StringPoint{Time: p.Time, Value: p.Value})
	}
	sort.Sort(stringPoints(points))
	return points
}

// newMeanIterator returns an iterator for operating on a mean() call.
func newMeanIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return &floatReduceSliceIterator{input: newBufFloatIterator(input), opt: opt, fn: floatMeanReduceSlice}
	default:
		panic(fmt.Sprintf("unsupported mean iterator type: %T", input))
	}
}

// floatMeanReduceSlice returns the mean value within a window.
func floatMeanReduceSlice(a []FloatPoint, opt *reduceOptions) []FloatPoint {
	var mean float64
	var count int
	for _, p := range a {
		if math.IsNaN(p.Value) {
			continue
		}
		count++
		mean += (p.Value - mean) / float64(count)
	}
	return []FloatPoint{{Time: opt.startTime, Value: mean}}
}

// newMedianIterator returns an iterator for operating on a median() call.
func newMedianIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return &floatReduceSliceIterator{input: newBufFloatIterator(input), opt: opt, fn: floatMedianReduceSlice}
	default:
		panic(fmt.Sprintf("unsupported median iterator type: %T", input))
	}
}

// floatMedianReduceSlice returns the median value within a window.
func floatMedianReduceSlice(a []FloatPoint, opt *reduceOptions) []FloatPoint {
	if len(a) == 1 {
		return []FloatPoint{{Time: opt.startTime, Value: a[0].Value}}
	}

	// OPTIMIZE(benbjohnson): Use getSortedRange() from v0.9.5.1.

	// Return the middle value from the points.
	// If there are an even number of points then return the mean of the two middle points.
	sort.Sort(floatPointsByValue(a))
	if len(a)%2 == 0 {
		lo, hi := a[len(a)/2-1], a[(len(a)/2)]
		return []FloatPoint{{Time: opt.startTime, Value: lo.Value + (hi.Value-lo.Value)/2}}
	}
	return []FloatPoint{{Time: opt.startTime, Value: a[len(a)/2].Value}}
}

// newStddevIterator returns an iterator for operating on a stddev() call.
func newStddevIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return &floatReduceSliceIterator{input: newBufFloatIterator(input), opt: opt, fn: floatStddevReduceSlice}
	case StringIterator:
		return &stringReduceSliceIterator{input: newBufStringIterator(input), opt: opt, fn: stringStddevReduceSlice}
	default:
		panic(fmt.Sprintf("unsupported stddev iterator type: %T", input))
	}
}

// floatStddevReduceSlice returns the stddev value within a window.
func floatStddevReduceSlice(a []FloatPoint, opt *reduceOptions) []FloatPoint {
	// If there is only one point then return NaN.
	if len(a) < 2 {
		return []FloatPoint{{Time: opt.startTime, Value: math.NaN()}}
	}

	// Calculate the mean.
	var mean float64
	var count int
	for _, p := range a {
		if math.IsNaN(p.Value) {
			continue
		}
		count++
		mean += (p.Value - mean) / float64(count)
	}

	// Calculate the variance.
	var variance float64
	for _, p := range a {
		if math.IsNaN(p.Value) {
			continue
		}
		variance += math.Pow(p.Value-mean, 2)
	}
	return []FloatPoint{{
		Time:  opt.startTime,
		Value: math.Sqrt(variance / float64(count-1)),
	}}
}

// stringStddevReduceSlice always returns "".
func stringStddevReduceSlice(a []StringPoint, opt *reduceOptions) []StringPoint {
	return []StringPoint{{Time: opt.startTime, Value: ""}}
}

// newSpreadIterator returns an iterator for operating on a spread() call.
func newSpreadIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return &floatReduceSliceIterator{input: newBufFloatIterator(input), opt: opt, fn: floatSpreadReduceSlice}
	default:
		panic(fmt.Sprintf("unsupported spread iterator type: %T", input))
	}
}

// floatSpreadReduceSlice returns the spread value within a window.
func floatSpreadReduceSlice(a []FloatPoint, opt *reduceOptions) []FloatPoint {
	// Find min & max values.
	min, max := a[0].Value, a[0].Value
	for _, p := range a[1:] {
		min = math.Min(min, p.Value)
		max = math.Max(max, p.Value)
	}
	return []FloatPoint{{Time: opt.startTime, Value: max - min}}
}

// newPercentileIterator returns an iterator for operating on a percentile() call.
func newPercentileIterator(input Iterator, opt IteratorOptions, percentile float64) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return &floatReduceSliceIterator{input: newBufFloatIterator(input), opt: opt, fn: newFloatPercentileReduceSliceFunc(percentile)}
	default:
		panic(fmt.Sprintf("unsupported percentile iterator type: %T", input))
	}
}

// newFloatPercentileReduceSliceFunc returns the percentile value within a window.
func newFloatPercentileReduceSliceFunc(percentile float64) floatReduceSliceFunc {
	return func(a []FloatPoint, opt *reduceOptions) []FloatPoint {
		sort.Sort(floatPointsByValue(a))
		i := int(math.Floor(float64(len(a))*percentile/100.0+0.5)) - 1

		if i < 0 || i >= len(a) {
			return []FloatPoint{{Time: opt.startTime, Value: math.NaN()}}
		}

		return []FloatPoint{{Time: opt.startTime, Value: a[i].Value}}
	}
}
