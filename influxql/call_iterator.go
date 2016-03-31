package influxql

import (
	"bytes"
	"container/heap"
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
func NewCallIterator(input Iterator, opt IteratorOptions) (Iterator, error) {
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
	case "mean":
		return newMeanIterator(input, opt)
	default:
		return nil, fmt.Errorf("unsupported function call: %s", name)
	}
}

// newCountIterator returns an iterator for operating on a count() call.
func newCountIterator(input Iterator, opt IteratorOptions) (Iterator, error) {
	// FIXME: Wrap iterator in int-type iterator and always output int value.

	switch input := input.(type) {
	case FloatIterator:
		createFn := func() (FloatPointAggregator, IntegerPointEmitter) {
			fn := NewFloatFuncIntegerReducer(FloatCountReduce)
			return fn, fn
		}
		return &floatReduceIntegerIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		createFn := func() (IntegerPointAggregator, IntegerPointEmitter) {
			fn := NewIntegerFuncReducer(IntegerCountReduce)
			return fn, fn
		}
		return &integerReduceIntegerIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	case StringIterator:
		createFn := func() (StringPointAggregator, IntegerPointEmitter) {
			fn := NewStringFuncIntegerReducer(StringCountReduce)
			return fn, fn
		}
		return &stringReduceIntegerIterator{input: newBufStringIterator(input), opt: opt, create: createFn}, nil
	case BooleanIterator:
		createFn := func() (BooleanPointAggregator, IntegerPointEmitter) {
			fn := NewBooleanFuncIntegerReducer(BooleanCountReduce)
			return fn, fn
		}
		return &booleanReduceIntegerIterator{input: newBufBooleanIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported count iterator type: %T", input)
	}
}

// FloatCountReduce returns the count of points.
func FloatCountReduce(prev *IntegerPoint, curr *FloatPoint) (int64, int64, []interface{}) {
	if prev == nil {
		return ZeroTime, 1, nil
	}
	return ZeroTime, prev.Value + 1, nil
}

// IntegerCountReduce returns the count of points.
func IntegerCountReduce(prev, curr *IntegerPoint) (int64, int64, []interface{}) {
	if prev == nil {
		return ZeroTime, 1, nil
	}
	return ZeroTime, prev.Value + 1, nil
}

// StringCountReduce returns the count of points.
func StringCountReduce(prev *IntegerPoint, curr *StringPoint) (int64, int64, []interface{}) {
	if prev == nil {
		return ZeroTime, 1, nil
	}
	return ZeroTime, prev.Value + 1, nil
}

// BooleanCountReduce returns the count of points.
func BooleanCountReduce(prev *IntegerPoint, curr *BooleanPoint) (int64, int64, []interface{}) {
	if prev == nil {
		return ZeroTime, 1, nil
	}
	return ZeroTime, prev.Value + 1, nil
}

// newMinIterator returns an iterator for operating on a min() call.
func newMinIterator(input Iterator, opt IteratorOptions) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatFuncReducer(FloatMinReduce)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		createFn := func() (IntegerPointAggregator, IntegerPointEmitter) {
			fn := NewIntegerFuncReducer(IntegerMinReduce)
			return fn, fn
		}
		return &integerReduceIntegerIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported min iterator type: %T", input)
	}
}

// FloatMinReduce returns the minimum value between prev & curr.
func FloatMinReduce(prev, curr *FloatPoint) (int64, float64, []interface{}) {
	if prev == nil || curr.Value < prev.Value || (curr.Value == prev.Value && curr.Time < prev.Time) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// IntegerMinReduce returns the minimum value between prev & curr.
func IntegerMinReduce(prev, curr *IntegerPoint) (int64, int64, []interface{}) {
	if prev == nil || curr.Value < prev.Value || (curr.Value == prev.Value && curr.Time < prev.Time) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// newMaxIterator returns an iterator for operating on a max() call.
func newMaxIterator(input Iterator, opt IteratorOptions) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatFuncReducer(FloatMaxReduce)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		createFn := func() (IntegerPointAggregator, IntegerPointEmitter) {
			fn := NewIntegerFuncReducer(IntegerMaxReduce)
			return fn, fn
		}
		return &integerReduceIntegerIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported max iterator type: %T", input)
	}
}

// FloatMaxReduce returns the maximum value between prev & curr.
func FloatMaxReduce(prev, curr *FloatPoint) (int64, float64, []interface{}) {
	if prev == nil || curr.Value > prev.Value || (curr.Value == prev.Value && curr.Time < prev.Time) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// IntegerMaxReduce returns the maximum value between prev & curr.
func IntegerMaxReduce(prev, curr *IntegerPoint) (int64, int64, []interface{}) {
	if prev == nil || curr.Value > prev.Value || (curr.Value == prev.Value && curr.Time < prev.Time) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// newSumIterator returns an iterator for operating on a sum() call.
func newSumIterator(input Iterator, opt IteratorOptions) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatFuncReducer(FloatSumReduce)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		createFn := func() (IntegerPointAggregator, IntegerPointEmitter) {
			fn := NewIntegerFuncReducer(IntegerSumReduce)
			return fn, fn
		}
		return &integerReduceIntegerIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported sum iterator type: %T", input)
	}
}

// FloatSumReduce returns the sum prev value & curr value.
func FloatSumReduce(prev, curr *FloatPoint) (int64, float64, []interface{}) {
	if prev == nil {
		return ZeroTime, curr.Value, nil
	}
	return prev.Time, prev.Value + curr.Value, nil
}

// IntegerSumReduce returns the sum prev value & curr value.
func IntegerSumReduce(prev, curr *IntegerPoint) (int64, int64, []interface{}) {
	if prev == nil {
		return ZeroTime, curr.Value, nil
	}
	return prev.Time, prev.Value + curr.Value, nil
}

// newFirstIterator returns an iterator for operating on a first() call.
func newFirstIterator(input Iterator, opt IteratorOptions) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatFuncReducer(FloatFirstReduce)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		createFn := func() (IntegerPointAggregator, IntegerPointEmitter) {
			fn := NewIntegerFuncReducer(IntegerFirstReduce)
			return fn, fn
		}
		return &integerReduceIntegerIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	case StringIterator:
		createFn := func() (StringPointAggregator, StringPointEmitter) {
			fn := NewStringFuncReducer(StringFirstReduce)
			return fn, fn
		}
		return &stringReduceStringIterator{input: newBufStringIterator(input), opt: opt, create: createFn}, nil
	case BooleanIterator:
		createFn := func() (BooleanPointAggregator, BooleanPointEmitter) {
			fn := NewBooleanFuncReducer(BooleanFirstReduce)
			return fn, fn
		}
		return &booleanReduceBooleanIterator{input: newBufBooleanIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported first iterator type: %T", input)
	}
}

// FloatFirstReduce returns the first point sorted by time.
func FloatFirstReduce(prev, curr *FloatPoint) (int64, float64, []interface{}) {
	if prev == nil || curr.Time < prev.Time || (curr.Time == prev.Time && curr.Value > prev.Value) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// IntegerFirstReduce returns the first point sorted by time.
func IntegerFirstReduce(prev, curr *IntegerPoint) (int64, int64, []interface{}) {
	if prev == nil || curr.Time < prev.Time || (curr.Time == prev.Time && curr.Value > prev.Value) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// StringFirstReduce returns the first point sorted by time.
func StringFirstReduce(prev, curr *StringPoint) (int64, string, []interface{}) {
	if prev == nil || curr.Time < prev.Time || (curr.Time == prev.Time && curr.Value > prev.Value) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// BooleanFirstReduce returns the first point sorted by time.
func BooleanFirstReduce(prev, curr *BooleanPoint) (int64, bool, []interface{}) {
	if prev == nil || curr.Time < prev.Time || (curr.Time == prev.Time && !curr.Value && prev.Value) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// newLastIterator returns an iterator for operating on a last() call.
func newLastIterator(input Iterator, opt IteratorOptions) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatFuncReducer(FloatLastReduce)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		createFn := func() (IntegerPointAggregator, IntegerPointEmitter) {
			fn := NewIntegerFuncReducer(IntegerLastReduce)
			return fn, fn
		}
		return &integerReduceIntegerIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	case StringIterator:
		createFn := func() (StringPointAggregator, StringPointEmitter) {
			fn := NewStringFuncReducer(StringLastReduce)
			return fn, fn
		}
		return &stringReduceStringIterator{input: newBufStringIterator(input), opt: opt, create: createFn}, nil
	case BooleanIterator:
		createFn := func() (BooleanPointAggregator, BooleanPointEmitter) {
			fn := NewBooleanFuncReducer(BooleanLastReduce)
			return fn, fn
		}
		return &booleanReduceBooleanIterator{input: newBufBooleanIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported last iterator type: %T", input)
	}
}

// FloatLastReduce returns the last point sorted by time.
func FloatLastReduce(prev, curr *FloatPoint) (int64, float64, []interface{}) {
	if prev == nil || curr.Time > prev.Time || (curr.Time == prev.Time && curr.Value > prev.Value) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// IntegerLastReduce returns the last point sorted by time.
func IntegerLastReduce(prev, curr *IntegerPoint) (int64, int64, []interface{}) {
	if prev == nil || curr.Time > prev.Time || (curr.Time == prev.Time && curr.Value > prev.Value) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// StringLastReduce returns the first point sorted by time.
func StringLastReduce(prev, curr *StringPoint) (int64, string, []interface{}) {
	if prev == nil || curr.Time > prev.Time || (curr.Time == prev.Time && curr.Value > prev.Value) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// BooleanLastReduce returns the first point sorted by time.
func BooleanLastReduce(prev, curr *BooleanPoint) (int64, bool, []interface{}) {
	if prev == nil || curr.Time > prev.Time || (curr.Time == prev.Time && curr.Value && !prev.Value) {
		return curr.Time, curr.Value, curr.Aux
	}
	return prev.Time, prev.Value, prev.Aux
}

// NewDistinctIterator returns an iterator for operating on a distinct() call.
func NewDistinctIterator(input Iterator, opt IteratorOptions) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatSliceFuncReducer(FloatDistinctReduceSlice)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		createFn := func() (IntegerPointAggregator, IntegerPointEmitter) {
			fn := NewIntegerSliceFuncReducer(IntegerDistinctReduceSlice)
			return fn, fn
		}
		return &integerReduceIntegerIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	case StringIterator:
		createFn := func() (StringPointAggregator, StringPointEmitter) {
			fn := NewStringSliceFuncReducer(StringDistinctReduceSlice)
			return fn, fn
		}
		return &stringReduceStringIterator{input: newBufStringIterator(input), opt: opt, create: createFn}, nil
	case BooleanIterator:
		createFn := func() (BooleanPointAggregator, BooleanPointEmitter) {
			fn := NewBooleanSliceFuncReducer(BooleanDistinctReduceSlice)
			return fn, fn
		}
		return &booleanReduceBooleanIterator{input: newBufBooleanIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported distinct iterator type: %T", input)
	}
}

// FloatDistinctReduceSlice returns the distinct value within a window.
func FloatDistinctReduceSlice(a []FloatPoint) []FloatPoint {
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

// IntegerDistinctReduceSlice returns the distinct value within a window.
func IntegerDistinctReduceSlice(a []IntegerPoint) []IntegerPoint {
	m := make(map[int64]IntegerPoint)
	for _, p := range a {
		if _, ok := m[p.Value]; !ok {
			m[p.Value] = p
		}
	}

	points := make([]IntegerPoint, 0, len(m))
	for _, p := range m {
		points = append(points, IntegerPoint{Time: p.Time, Value: p.Value})
	}
	sort.Sort(integerPoints(points))
	return points
}

// StringDistinctReduceSlice returns the distinct value within a window.
func StringDistinctReduceSlice(a []StringPoint) []StringPoint {
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

// BooleanDistinctReduceSlice returns the distinct value within a window.
func BooleanDistinctReduceSlice(a []BooleanPoint) []BooleanPoint {
	m := make(map[bool]BooleanPoint)
	for _, p := range a {
		if _, ok := m[p.Value]; !ok {
			m[p.Value] = p
		}
	}

	points := make([]BooleanPoint, 0, len(m))
	for _, p := range m {
		points = append(points, BooleanPoint{Time: p.Time, Value: p.Value})
	}
	sort.Sort(booleanPoints(points))
	return points
}

// newMeanIterator returns an iterator for operating on a mean() call.
func newMeanIterator(input Iterator, opt IteratorOptions) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatMeanReducer()
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		createFn := func() (IntegerPointAggregator, FloatPointEmitter) {
			fn := NewIntegerMeanReducer()
			return fn, fn
		}
		return &integerReduceFloatIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported mean iterator type: %T", input)
	}
}

// newMedianIterator returns an iterator for operating on a median() call.
func newMedianIterator(input Iterator, opt IteratorOptions) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatSliceFuncReducer(FloatMedianReduceSlice)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		createFn := func() (IntegerPointAggregator, FloatPointEmitter) {
			fn := NewIntegerSliceFuncFloatReducer(IntegerMedianReduceSlice)
			return fn, fn
		}
		return &integerReduceFloatIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported median iterator type: %T", input)
	}
}

// FloatMedianReduceSlice returns the median value within a window.
func FloatMedianReduceSlice(a []FloatPoint) []FloatPoint {
	if len(a) == 1 {
		return a
	}

	// OPTIMIZE(benbjohnson): Use getSortedRange() from v0.9.5.1.

	// Return the middle value from the points.
	// If there are an even number of points then return the mean of the two middle points.
	sort.Sort(floatPointsByValue(a))
	if len(a)%2 == 0 {
		lo, hi := a[len(a)/2-1], a[(len(a)/2)]
		return []FloatPoint{{Time: ZeroTime, Value: lo.Value + (hi.Value-lo.Value)/2}}
	}
	return []FloatPoint{{Time: ZeroTime, Value: a[len(a)/2].Value}}
}

// IntegerMedianReduceSlice returns the median value within a window.
func IntegerMedianReduceSlice(a []IntegerPoint) []FloatPoint {
	if len(a) == 1 {
		return []FloatPoint{{Time: ZeroTime, Value: float64(a[0].Value)}}
	}

	// OPTIMIZE(benbjohnson): Use getSortedRange() from v0.9.5.1.

	// Return the middle value from the points.
	// If there are an even number of points then return the mean of the two middle points.
	sort.Sort(integerPointsByValue(a))
	if len(a)%2 == 0 {
		lo, hi := a[len(a)/2-1], a[(len(a)/2)]
		return []FloatPoint{{Time: ZeroTime, Value: float64(lo.Value) + float64(hi.Value-lo.Value)/2}}
	}
	return []FloatPoint{{Time: ZeroTime, Value: float64(a[len(a)/2].Value)}}
}

// newStddevIterator returns an iterator for operating on a stddev() call.
func newStddevIterator(input Iterator, opt IteratorOptions) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatSliceFuncReducer(FloatStddevReduceSlice)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		createFn := func() (IntegerPointAggregator, FloatPointEmitter) {
			fn := NewIntegerSliceFuncFloatReducer(IntegerStddevReduceSlice)
			return fn, fn
		}
		return &integerReduceFloatIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	case StringIterator:
		createFn := func() (StringPointAggregator, StringPointEmitter) {
			fn := NewStringSliceFuncReducer(StringStddevReduceSlice)
			return fn, fn
		}
		return &stringReduceStringIterator{input: newBufStringIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported stddev iterator type: %T", input)
	}
}

// FloatStddevReduceSlice returns the stddev value within a window.
func FloatStddevReduceSlice(a []FloatPoint) []FloatPoint {
	// If there is only one point then return 0.
	if len(a) < 2 {
		return []FloatPoint{{Time: ZeroTime, Nil: true}}
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
		Time:  ZeroTime,
		Value: math.Sqrt(variance / float64(count-1)),
	}}
}

// IntegerStddevReduceSlice returns the stddev value within a window.
func IntegerStddevReduceSlice(a []IntegerPoint) []FloatPoint {
	// If there is only one point then return 0.
	if len(a) < 2 {
		return []FloatPoint{{Time: ZeroTime, Nil: true}}
	}

	// Calculate the mean.
	var mean float64
	var count int
	for _, p := range a {
		count++
		mean += (float64(p.Value) - mean) / float64(count)
	}

	// Calculate the variance.
	var variance float64
	for _, p := range a {
		variance += math.Pow(float64(p.Value)-mean, 2)
	}
	return []FloatPoint{{
		Time:  ZeroTime,
		Value: math.Sqrt(variance / float64(count-1)),
	}}
}

// StringStddevReduceSlice always returns "".
func StringStddevReduceSlice(a []StringPoint) []StringPoint {
	return []StringPoint{{Time: ZeroTime, Value: ""}}
}

// newSpreadIterator returns an iterator for operating on a spread() call.
func newSpreadIterator(input Iterator, opt IteratorOptions) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatSliceFuncReducer(FloatSpreadReduceSlice)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		createFn := func() (IntegerPointAggregator, IntegerPointEmitter) {
			fn := NewIntegerSliceFuncReducer(IntegerSpreadReduceSlice)
			return fn, fn
		}
		return &integerReduceIntegerIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported spread iterator type: %T", input)
	}
}

// FloatSpreadReduceSlice returns the spread value within a window.
func FloatSpreadReduceSlice(a []FloatPoint) []FloatPoint {
	// Find min & max values.
	min, max := a[0].Value, a[0].Value
	for _, p := range a[1:] {
		min = math.Min(min, p.Value)
		max = math.Max(max, p.Value)
	}
	return []FloatPoint{{Time: ZeroTime, Value: max - min}}
}

// IntegerSpreadReduceSlice returns the spread value within a window.
func IntegerSpreadReduceSlice(a []IntegerPoint) []IntegerPoint {
	// Find min & max values.
	min, max := a[0].Value, a[0].Value
	for _, p := range a[1:] {
		if p.Value < min {
			min = p.Value
		}
		if p.Value > max {
			max = p.Value
		}
	}
	return []IntegerPoint{{Time: ZeroTime, Value: max - min}}
}

func newTopIterator(input Iterator, opt IteratorOptions, n *IntegerLiteral, tags []int) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		aggregateFn := NewFloatTopReduceSliceFunc(int(n.Val), tags, opt.Interval)
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatSliceFuncReducer(aggregateFn)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		aggregateFn := NewIntegerTopReduceSliceFunc(int(n.Val), tags, opt.Interval)
		createFn := func() (IntegerPointAggregator, IntegerPointEmitter) {
			fn := NewIntegerSliceFuncReducer(aggregateFn)
			return fn, fn
		}
		return &integerReduceIntegerIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported top iterator type: %T", input)
	}
}

// NewFloatTopReduceSliceFunc returns the top values within a window.
func NewFloatTopReduceSliceFunc(n int, tags []int, interval Interval) FloatReduceSliceFunc {
	return func(a []FloatPoint) []FloatPoint {
		// Filter by tags if they exist.
		if len(tags) > 0 {
			a = filterFloatByUniqueTags(a, tags, func(cur, p *FloatPoint) bool {
				return p.Value > cur.Value || (p.Value == cur.Value && p.Time < cur.Time)
			})
		}

		// If we ask for more elements than exist, restrict n to be the length of the array.
		size := n
		if size > len(a) {
			size = len(a)
		}

		// Construct a heap preferring higher values and breaking ties
		// based on the earliest time for a point.
		h := floatPointsSortBy(a, func(a, b *FloatPoint) bool {
			if a.Value != b.Value {
				return a.Value > b.Value
			}
			return a.Time < b.Time
		})
		heap.Init(h)

		// Pop the first n elements and then sort by time.
		points := make([]FloatPoint, 0, size)
		for i := 0; i < size; i++ {
			p := heap.Pop(h).(FloatPoint)
			points = append(points, p)
		}

		// Either zero out all values or sort the points by time
		// depending on if a time interval was given or not.
		if !interval.IsZero() {
			for i := range points {
				points[i].Time = ZeroTime
			}
		} else {
			sort.Stable(floatPointsByTime(points))
		}
		return points
	}
}

// NewIntegerTopReduceSliceFunc returns the top values within a window.
func NewIntegerTopReduceSliceFunc(n int, tags []int, interval Interval) IntegerReduceSliceFunc {
	return func(a []IntegerPoint) []IntegerPoint {
		// Filter by tags if they exist.
		if len(tags) > 0 {
			a = filterIntegerByUniqueTags(a, tags, func(cur, p *IntegerPoint) bool {
				return p.Value > cur.Value || (p.Value == cur.Value && p.Time < cur.Time)
			})
		}

		// If we ask for more elements than exist, restrict n to be the length of the array.
		size := n
		if size > len(a) {
			size = len(a)
		}

		// Construct a heap preferring higher values and breaking ties
		// based on the earliest time for a point.
		h := integerPointsSortBy(a, func(a, b *IntegerPoint) bool {
			if a.Value != b.Value {
				return a.Value > b.Value
			}
			return a.Time < b.Time
		})
		heap.Init(h)

		// Pop the first n elements and then sort by time.
		points := make([]IntegerPoint, 0, size)
		for i := 0; i < size; i++ {
			p := heap.Pop(h).(IntegerPoint)
			points = append(points, p)
		}

		// Either zero out all values or sort the points by time
		// depending on if a time interval was given or not.
		if !interval.IsZero() {
			for i := range points {
				points[i].Time = ZeroTime
			}
		} else {
			sort.Stable(integerPointsByTime(points))
		}
		return points
	}
}

func newBottomIterator(input Iterator, opt IteratorOptions, n *IntegerLiteral, tags []int) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		aggregateFn := NewFloatBottomReduceSliceFunc(int(n.Val), tags, opt.Interval)
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatSliceFuncReducer(aggregateFn)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		aggregateFn := NewIntegerBottomReduceSliceFunc(int(n.Val), tags, opt.Interval)
		createFn := func() (IntegerPointAggregator, IntegerPointEmitter) {
			fn := NewIntegerSliceFuncReducer(aggregateFn)
			return fn, fn
		}
		return &integerReduceIntegerIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported bottom iterator type: %T", input)
	}
}

// NewFloatBottomReduceSliceFunc returns the bottom values within a window.
func NewFloatBottomReduceSliceFunc(n int, tags []int, interval Interval) FloatReduceSliceFunc {
	return func(a []FloatPoint) []FloatPoint {
		// Filter by tags if they exist.
		if len(tags) > 0 {
			a = filterFloatByUniqueTags(a, tags, func(cur, p *FloatPoint) bool {
				return p.Value < cur.Value || (p.Value == cur.Value && p.Time < cur.Time)
			})
		}

		// If we ask for more elements than exist, restrict n to be the length of the array.
		size := n
		if size > len(a) {
			size = len(a)
		}

		// Construct a heap preferring lower values and breaking ties
		// based on the earliest time for a point.
		h := floatPointsSortBy(a, func(a, b *FloatPoint) bool {
			if a.Value != b.Value {
				return a.Value < b.Value
			}
			return a.Time < b.Time
		})
		heap.Init(h)

		// Pop the first n elements and then sort by time.
		points := make([]FloatPoint, 0, size)
		for i := 0; i < size; i++ {
			p := heap.Pop(h).(FloatPoint)
			points = append(points, p)
		}

		// Either zero out all values or sort the points by time
		// depending on if a time interval was given or not.
		if !interval.IsZero() {
			for i := range points {
				points[i].Time = ZeroTime
			}
		} else {
			sort.Stable(floatPointsByTime(points))
		}
		return points
	}
}

// NewIntegerBottomReduceSliceFunc returns the bottom values within a window.
func NewIntegerBottomReduceSliceFunc(n int, tags []int, interval Interval) IntegerReduceSliceFunc {
	return func(a []IntegerPoint) []IntegerPoint {
		// Filter by tags if they exist.
		if len(tags) > 0 {
			a = filterIntegerByUniqueTags(a, tags, func(cur, p *IntegerPoint) bool {
				return p.Value < cur.Value || (p.Value == cur.Value && p.Time < cur.Time)
			})
		}

		// If we ask for more elements than exist, restrict n to be the length of the array.
		size := n
		if size > len(a) {
			size = len(a)
		}

		// Construct a heap preferring lower values and breaking ties
		// based on the earliest time for a point.
		h := integerPointsSortBy(a, func(a, b *IntegerPoint) bool {
			if a.Value != b.Value {
				return a.Value < b.Value
			}
			return a.Time < b.Time
		})
		heap.Init(h)

		// Pop the first n elements and then sort by time.
		points := make([]IntegerPoint, 0, size)
		for i := 0; i < size; i++ {
			p := heap.Pop(h).(IntegerPoint)
			points = append(points, p)
		}

		// Either zero out all values or sort the points by time
		// depending on if a time interval was given or not.
		if !interval.IsZero() {
			for i := range points {
				points[i].Time = ZeroTime
			}
		} else {
			sort.Stable(integerPointsByTime(points))
		}
		return points
	}
}

func filterFloatByUniqueTags(a []FloatPoint, tags []int, cmpFunc func(cur, p *FloatPoint) bool) []FloatPoint {
	pointMap := make(map[string]FloatPoint)
	for _, p := range a {
		keyBuf := bytes.NewBuffer(nil)
		for i, index := range tags {
			if i > 0 {
				keyBuf.WriteString(",")
			}
			fmt.Fprintf(keyBuf, "%s", p.Aux[index])
		}
		key := keyBuf.String()

		cur, ok := pointMap[key]
		if ok {
			if cmpFunc(&cur, &p) {
				pointMap[key] = p
			}
		} else {
			pointMap[key] = p
		}
	}

	// Recreate the original array with our new filtered list.
	points := make([]FloatPoint, 0, len(pointMap))
	for _, p := range pointMap {
		points = append(points, p)
	}
	return points
}

func filterIntegerByUniqueTags(a []IntegerPoint, tags []int, cmpFunc func(cur, p *IntegerPoint) bool) []IntegerPoint {
	pointMap := make(map[string]IntegerPoint)
	for _, p := range a {
		keyBuf := bytes.NewBuffer(nil)
		for i, index := range tags {
			if i > 0 {
				keyBuf.WriteString(",")
			}
			fmt.Fprintf(keyBuf, "%s", p.Aux[index])
		}
		key := keyBuf.String()

		cur, ok := pointMap[key]
		if ok {
			if cmpFunc(&cur, &p) {
				pointMap[key] = p
			}
		} else {
			pointMap[key] = p
		}
	}

	// Recreate the original array with our new filtered list.
	points := make([]IntegerPoint, 0, len(pointMap))
	for _, p := range pointMap {
		points = append(points, p)
	}
	return points
}

// newPercentileIterator returns an iterator for operating on a percentile() call.
func newPercentileIterator(input Iterator, opt IteratorOptions, percentile float64) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		floatPercentileReduceSlice := NewFloatPercentileReduceSliceFunc(percentile)
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatSliceFuncReducer(floatPercentileReduceSlice)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		integerPercentileReduceSlice := NewIntegerPercentileReduceSliceFunc(percentile)
		createFn := func() (IntegerPointAggregator, IntegerPointEmitter) {
			fn := NewIntegerSliceFuncReducer(integerPercentileReduceSlice)
			return fn, fn
		}
		return &integerReduceIntegerIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported percentile iterator type: %T", input)
	}
}

// NewFloatPercentileReduceSliceFunc returns the percentile value within a window.
func NewFloatPercentileReduceSliceFunc(percentile float64) FloatReduceSliceFunc {
	return func(a []FloatPoint) []FloatPoint {
		length := len(a)
		i := int(math.Floor(float64(length)*percentile/100.0+0.5)) - 1

		if i < 0 || i >= length {
			return nil
		}

		sort.Sort(floatPointsByValue(a))
		return []FloatPoint{{Time: ZeroTime, Value: a[i].Value}}
	}
}

// NewIntegerPercentileReduceSliceFunc returns the percentile value within a window.
func NewIntegerPercentileReduceSliceFunc(percentile float64) IntegerReduceSliceFunc {
	return func(a []IntegerPoint) []IntegerPoint {
		length := len(a)
		i := int(math.Floor(float64(length)*percentile/100.0+0.5)) - 1

		if i < 0 || i >= length {
			return nil
		}

		sort.Sort(integerPointsByValue(a))
		return []IntegerPoint{{Time: ZeroTime, Value: a[i].Value}}
	}
}

// newDerivativeIterator returns an iterator for operating on a derivative() call.
func newDerivativeIterator(input Iterator, opt IteratorOptions, interval Interval, isNonNegative bool) (Iterator, error) {
	// Derivatives do not use GROUP BY intervals or time constraints, so clear these options.
	opt.Interval = Interval{}
	opt.StartTime, opt.EndTime = MinTime, MaxTime

	switch input := input.(type) {
	case FloatIterator:
		floatDerivativeReduceSlice := NewFloatDerivativeReduceSliceFunc(interval, isNonNegative)
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatSliceFuncReducer(floatDerivativeReduceSlice)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		integerDerivativeReduceSlice := NewIntegerDerivativeReduceSliceFunc(interval, isNonNegative)
		createFn := func() (IntegerPointAggregator, FloatPointEmitter) {
			fn := NewIntegerSliceFuncFloatReducer(integerDerivativeReduceSlice)
			return fn, fn
		}
		return &integerReduceFloatIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported derivative iterator type: %T", input)
	}
}

// NewFloatDerivativeReduceSliceFunc returns the derivative value within a window.
func NewFloatDerivativeReduceSliceFunc(interval Interval, isNonNegative bool) FloatReduceSliceFunc {
	prev := FloatPoint{Nil: true}

	return func(a []FloatPoint) []FloatPoint {
		if len(a) == 0 {
			return a
		} else if len(a) == 1 {
			return []FloatPoint{{Time: a[0].Time, Nil: true}}
		}

		if prev.Nil {
			prev = a[0]
		}

		output := make([]FloatPoint, 0, len(a)-1)
		for i := 1; i < len(a); i++ {
			p := &a[i]

			// Calculate the derivative of successive points by dividing the
			// difference of each value by the elapsed time normalized to the interval.
			diff := p.Value - prev.Value
			elapsed := p.Time - prev.Time

			value := 0.0
			if elapsed > 0 {
				value = diff / (float64(elapsed) / float64(interval.Duration))
			}

			prev = *p

			// Drop negative values for non-negative derivatives.
			if isNonNegative && diff < 0 {
				continue
			}

			output = append(output, FloatPoint{Time: p.Time, Value: value})
		}
		return output
	}
}

// NewIntegerDerivativeReduceSliceFunc returns the derivative value within a window.
func NewIntegerDerivativeReduceSliceFunc(interval Interval, isNonNegative bool) IntegerReduceFloatSliceFunc {
	prev := IntegerPoint{Nil: true}

	return func(a []IntegerPoint) []FloatPoint {
		if len(a) == 0 {
			return []FloatPoint{}
		} else if len(a) == 1 {
			return []FloatPoint{{Time: a[0].Time, Nil: true}}
		}

		if prev.Nil {
			prev = a[0]
		}

		output := make([]FloatPoint, 0, len(a)-1)
		for i := 1; i < len(a); i++ {
			p := &a[i]

			// Calculate the derivative of successive points by dividing the
			// difference of each value by the elapsed time normalized to the interval.
			diff := float64(p.Value - prev.Value)
			elapsed := p.Time - prev.Time

			value := 0.0
			if elapsed > 0 {
				value = diff / (float64(elapsed) / float64(interval.Duration))
			}

			prev = *p

			// Drop negative values for non-negative derivatives.
			if isNonNegative && diff < 0 {
				continue
			}

			output = append(output, FloatPoint{Time: p.Time, Value: value})
		}
		return output
	}
}

// newDifferenceIterator returns an iterator for operating on a difference() call.
func newDifferenceIterator(input Iterator, opt IteratorOptions) (Iterator, error) {
	// Differences do not use GROUP BY intervals or time constraints, so clear these options.
	opt.Interval = Interval{}
	opt.StartTime, opt.EndTime = MinTime, MaxTime

	switch input := input.(type) {
	case FloatIterator:
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatSliceFuncReducer(FloatDifferenceReduceSlice)
			return fn, fn
		}
		return &floatReduceFloatIterator{input: newBufFloatIterator(input), opt: opt, create: createFn}, nil
	case IntegerIterator:
		createFn := func() (IntegerPointAggregator, IntegerPointEmitter) {
			fn := NewIntegerSliceFuncReducer(IntegerDifferenceReduceSlice)
			return fn, fn
		}
		return &integerReduceIntegerIterator{input: newBufIntegerIterator(input), opt: opt, create: createFn}, nil
	default:
		return nil, fmt.Errorf("unsupported difference iterator type: %T", input)
	}
}

// FloatDifferenceReduceSlice returns the difference values within a window.
func FloatDifferenceReduceSlice(a []FloatPoint) []FloatPoint {
	if len(a) < 2 {
		return []FloatPoint{}
	}
	prev := a[0]

	output := make([]FloatPoint, 0, len(a)-1)
	for i := 1; i < len(a); i++ {
		p := &a[i]

		// Calculate the difference of successive points.
		value := p.Value - prev.Value
		prev = *p

		output = append(output, FloatPoint{Time: p.Time, Value: value})
	}
	return output
}

// IntegerDifferenceReduceSlice returns the difference values within a window.
func IntegerDifferenceReduceSlice(a []IntegerPoint) []IntegerPoint {
	if len(a) < 2 {
		return []IntegerPoint{}
	}
	prev := a[0]

	output := make([]IntegerPoint, 0, len(a)-1)
	for i := 1; i < len(a); i++ {
		p := &a[i]

		// Calculate the difference of successive points.
		value := p.Value - prev.Value
		prev = *p

		output = append(output, IntegerPoint{Time: p.Time, Value: value})
	}
	return output
}

// newMovingAverageIterator returns an iterator for operating on a moving_average() call.
func newMovingAverageIterator(input Iterator, n int, opt IteratorOptions) (Iterator, error) {
	switch input := input.(type) {
	case FloatIterator:
		createFn := func() (FloatPointAggregator, FloatPointEmitter) {
			fn := NewFloatMovingAverageReducer(n)
			return fn, fn
		}
		return newFloatStreamFloatIterator(input, createFn, opt), nil
	case IntegerIterator:
		createFn := func() (IntegerPointAggregator, FloatPointEmitter) {
			fn := NewIntegerMovingAverageReducer(n)
			return fn, fn
		}
		return newIntegerStreamFloatIterator(input, createFn, opt), nil
	default:
		return nil, fmt.Errorf("unsupported moving average iterator type: %T", input)
	}
}
