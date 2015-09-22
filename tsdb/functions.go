package tsdb

// All aggregate and query functions are defined in this file along with any intermediate data objects they need to process.
// Query functions are represented as two discreet functions: Map and Reduce. These roughly follow the MapReduce
// paradigm popularized by Google and Hadoop.
//
// When adding an aggregate function, define a mapper, a reducer, and add them in the switch statement in the MapreduceFuncs function

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"

	"github.com/influxdb/influxdb/influxql"
)

// iterator represents a forward-only iterator over a set of points.
// These are used by the mapFunctions in this file
type Iterator interface {
	Next() (time int64, value interface{})
	Tags() map[string]string
	TMin() int64
}

// mapFunc represents a function used for mapping over a sequential series of data.
// The iterator represents a single group by interval
type mapFunc func(Iterator) interface{}

// reduceFunc represents a function used for reducing mapper output.
type reduceFunc func([]interface{}) interface{}

// UnmarshalFunc represents a function that can take bytes from a mapper from remote
// server and marshal it into an interface the reducer can use
type UnmarshalFunc func([]byte) (interface{}, error)

// initializemapFunc takes an aggregate call from the query and returns the mapFunc
func initializeMapFunc(c *influxql.Call) (mapFunc, error) {
	// see if it's a query for raw data
	if c == nil {
		return MapRawQuery, nil
	}

	// Retrieve map function by name.
	switch c.Name {
	case "count":
		if _, ok := c.Args[0].(*influxql.Distinct); ok {
			return MapCountDistinct, nil
		}
		if c, ok := c.Args[0].(*influxql.Call); ok {
			if c.Name == "distinct" {
				return MapCountDistinct, nil
			}
		}
		return MapCount, nil
	case "distinct":
		return MapDistinct, nil
	case "sum":
		return MapSum, nil
	case "mean":
		return MapMean, nil
	case "median":
		return MapStddev, nil
	case "min":
		return MapMin, nil
	case "max":
		return MapMax, nil
	case "spread":
		return MapSpread, nil
	case "stddev":
		return MapStddev, nil
	case "first":
		return MapFirst, nil
	case "last":
		return MapLast, nil
	case "top", "bottom":
		return func(itr Iterator) interface{} {
			return MapTopBottom(itr, c)
		}, nil
	case "percentile":
		return MapEcho, nil
	case "derivative", "non_negative_derivative":
		// If the arg is another aggregate e.g. derivative(mean(value)), then
		// use the map func for that nested aggregate
		if fn, ok := c.Args[0].(*influxql.Call); ok {
			return initializeMapFunc(fn)
		}
		return MapRawQuery, nil
	default:
		return nil, fmt.Errorf("function not found: %q", c.Name)
	}
}

// InitializereduceFunc takes an aggregate call from the query and returns the reduceFunc
func initializeReduceFunc(c *influxql.Call) (reduceFunc, error) {
	// Retrieve reduce function by name.
	switch c.Name {
	case "count":
		if _, ok := c.Args[0].(*influxql.Distinct); ok {
			return ReduceCountDistinct, nil
		}
		if c, ok := c.Args[0].(*influxql.Call); ok {
			if c.Name == "distinct" {
				return ReduceCountDistinct, nil
			}
		}
		return ReduceSum, nil
	case "distinct":
		return ReduceDistinct, nil
	case "sum":
		return ReduceSum, nil
	case "mean":
		return ReduceMean, nil
	case "median":
		return ReduceMedian, nil
	case "min":
		return ReduceMin, nil
	case "max":
		return ReduceMax, nil
	case "spread":
		return ReduceSpread, nil
	case "stddev":
		return ReduceStddev, nil
	case "first":
		return ReduceFirst, nil
	case "last":
		return ReduceLast, nil
	case "top", "bottom":
		return func(values []interface{}) interface{} {
			return ReduceTopBottom(values, c)
		}, nil
	case "percentile":
		return func(values []interface{}) interface{} {
			return ReducePercentile(values, c)
		}, nil
	case "derivative", "non_negative_derivative":
		// If the arg is another aggregate e.g. derivative(mean(value)), then
		// use the map func for that nested aggregate
		if fn, ok := c.Args[0].(*influxql.Call); ok {
			return initializeReduceFunc(fn)
		}
		return nil, fmt.Errorf("expected function argument to %s", c.Name)
	default:
		return nil, fmt.Errorf("function not found: %q", c.Name)
	}
}

func InitializeUnmarshaller(c *influxql.Call) (UnmarshalFunc, error) {
	// if c is nil it's a raw data query
	if c == nil {
		return func(b []byte) (interface{}, error) {
			a := make([]*rawQueryMapOutput, 0)
			err := json.Unmarshal(b, &a)
			return a, err
		}, nil
	}

	// Retrieve marshal function by name
	switch c.Name {
	case "mean":
		return func(b []byte) (interface{}, error) {
			var o meanMapOutput
			err := json.Unmarshal(b, &o)
			return &o, err
		}, nil
	case "spread":
		return func(b []byte) (interface{}, error) {
			var o spreadMapOutput
			err := json.Unmarshal(b, &o)
			return &o, err
		}, nil
	case "distinct":
		return func(b []byte) (interface{}, error) {
			var val interfaceValues
			err := json.Unmarshal(b, &val)
			return val, err
		}, nil
	case "first":
		return func(b []byte) (interface{}, error) {
			var o firstLastMapOutput
			err := json.Unmarshal(b, &o)
			return &o, err
		}, nil
	case "last":
		return func(b []byte) (interface{}, error) {
			var o firstLastMapOutput
			err := json.Unmarshal(b, &o)
			return &o, err
		}, nil
	case "stddev":
		return func(b []byte) (interface{}, error) {
			val := make([]float64, 0)
			err := json.Unmarshal(b, &val)
			return val, err
		}, nil
	case "median":
		return func(b []byte) (interface{}, error) {
			a := make([]float64, 0)
			err := json.Unmarshal(b, &a)
			return a, err
		}, nil
	default:
		return func(b []byte) (interface{}, error) {
			var val interface{}
			err := json.Unmarshal(b, &val)
			return val, err
		}, nil
	}
}

// MapCount computes the number of values in an iterator.
func MapCount(itr Iterator) interface{} {
	n := float64(0)
	for k, _ := itr.Next(); k != -1; k, _ = itr.Next() {
		n++
	}
	if n > 0 {
		return n
	}
	return nil
}

type interfaceValues []interface{}

func (d interfaceValues) Len() int      { return len(d) }
func (d interfaceValues) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d interfaceValues) Less(i, j int) bool {
	cmpt, a, b := typeCompare(d[i], d[j])
	cmpv := valueCompare(a, b)
	if cmpv == 0 {
		return cmpt < 0
	}
	return cmpv < 0
}

// MapDistinct computes the unique values in an iterator.
func MapDistinct(itr Iterator) interface{} {
	var index = make(map[interface{}]struct{})

	for time, value := itr.Next(); time != -1; time, value = itr.Next() {
		index[value] = struct{}{}
	}

	if len(index) == 0 {
		return nil
	}

	results := make(interfaceValues, len(index))
	var i int
	for value, _ := range index {
		results[i] = value
		i++
	}
	return results
}

// ReduceDistinct finds the unique values for each key.
func ReduceDistinct(values []interface{}) interface{} {
	var index = make(map[interface{}]struct{})

	// index distinct values from each mapper
	for _, v := range values {
		if v == nil {
			continue
		}
		d, ok := v.(interfaceValues)
		if !ok {
			msg := fmt.Sprintf("expected distinctValues, got: %T", v)
			panic(msg)
		}
		for _, distinctValue := range d {
			index[distinctValue] = struct{}{}
		}
	}

	// convert map keys to an array
	results := make(interfaceValues, len(index))
	var i int
	for k, _ := range index {
		results[i] = k
		i++
	}
	if len(results) > 0 {
		sort.Sort(results)
		return results
	}
	return nil
}

// MapCountDistinct computes the unique count of values in an iterator.
func MapCountDistinct(itr Iterator) interface{} {
	var index = make(map[interface{}]struct{})

	for time, value := itr.Next(); time != -1; time, value = itr.Next() {
		index[value] = struct{}{}
	}

	if len(index) == 0 {
		return nil
	}

	return index
}

// ReduceCountDistinct finds the unique counts of values.
func ReduceCountDistinct(values []interface{}) interface{} {
	var index = make(map[interface{}]struct{})

	// index distinct values from each mapper
	for _, v := range values {
		if v == nil {
			continue
		}
		d, ok := v.(map[interface{}]struct{})
		if !ok {
			msg := fmt.Sprintf("expected map[interface{}]struct{}, got: %T", v)
			panic(msg)
		}
		for distinctCountValue, _ := range d {
			index[distinctCountValue] = struct{}{}
		}
	}

	return len(index)
}

type NumberType int8

const (
	Float64Type NumberType = iota
	Int64Type
)

// MapSum computes the summation of values in an iterator.
func MapSum(itr Iterator) interface{} {
	n := float64(0)
	count := 0
	var resultType NumberType
	for k, v := itr.Next(); k != -1; k, v = itr.Next() {
		count++
		switch n1 := v.(type) {
		case float64:
			n += n1
		case int64:
			n += float64(n1)
			resultType = Int64Type
		}
	}
	if count > 0 {
		switch resultType {
		case Float64Type:
			return n
		case Int64Type:
			return int64(n)
		}
	}
	return nil
}

// ReduceSum computes the sum of values for each key.
func ReduceSum(values []interface{}) interface{} {
	var n float64
	count := 0
	var resultType NumberType
	for _, v := range values {
		if v == nil {
			continue
		}
		count++
		switch n1 := v.(type) {
		case float64:
			n += n1
		case int64:
			n += float64(n1)
			resultType = Int64Type
		}
	}
	if count > 0 {
		switch resultType {
		case Float64Type:
			return n
		case Int64Type:
			return int64(n)
		}
	}
	return nil
}

// MapMean computes the count and sum of values in an iterator to be combined by the reducer.
func MapMean(itr Iterator) interface{} {
	out := &meanMapOutput{}

	for k, v := itr.Next(); k != -1; k, v = itr.Next() {
		out.Count++
		switch n1 := v.(type) {
		case float64:
			out.Mean += (n1 - out.Mean) / float64(out.Count)
		case int64:
			out.Mean += (float64(n1) - out.Mean) / float64(out.Count)
			out.ResultType = Int64Type
		}
	}

	if out.Count > 0 {
		return out
	}

	return nil
}

type meanMapOutput struct {
	Count      int
	Mean       float64
	ResultType NumberType
}

// ReduceMean computes the mean of values for each key.
func ReduceMean(values []interface{}) interface{} {
	out := &meanMapOutput{}
	var countSum int
	for _, v := range values {
		if v == nil {
			continue
		}
		val := v.(*meanMapOutput)
		countSum = out.Count + val.Count
		out.Mean = val.Mean*(float64(val.Count)/float64(countSum)) + out.Mean*(float64(out.Count)/float64(countSum))
		out.Count = countSum
	}
	if out.Count > 0 {
		return out.Mean
	}
	return nil
}

// ReduceMedian computes the median of values
func ReduceMedian(values []interface{}) interface{} {
	var data []float64
	// Collect all the data points
	for _, value := range values {
		if value == nil {
			continue
		}
		data = append(data, value.([]float64)...)
	}

	length := len(data)
	if length < 2 {
		if length == 0 {
			return nil
		}
		return data[0]
	}
	middle := length / 2
	var sortedRange []float64
	if length%2 == 0 {
		sortedRange = getSortedRange(data, middle-1, 2)
		var low, high = sortedRange[0], sortedRange[1]
		return low + (high-low)/2
	}
	sortedRange = getSortedRange(data, middle, 1)
	return sortedRange[0]
}

// getSortedRange returns a sorted subset of data. By using discardLowerRange and discardUpperRange to get the target
// subset (unsorted) and then just sorting that subset, the work can be reduced from O(N lg N), where N is len(data), to
// O(N + count lg count) for the average case
// - O(N) to discard the unwanted items
// - O(count lg count) to sort the count number of extracted items
// This can be useful for:
// - finding the median: getSortedRange(data, middle, 1)
// - finding the top N: getSortedRange(data, len(data) - N, N)
// - finding the bottom N: getSortedRange(data, 0, N)
func getSortedRange(data []float64, start int, count int) []float64 {
	out := discardLowerRange(data, start)
	k := len(out) - count
	if k > 0 {
		out = discardUpperRange(out, k)
	}
	sort.Float64s(out)

	return out
}

// discardLowerRange discards the lower k elements of the sorted data set without sorting all the data. Sorting all of
// the data would take O(NlgN), where N is len(data), but partitioning to find the kth largest number is O(N) in the
// average case. The remaining N-k unsorted elements are returned - no kind of ordering is guaranteed on these elements.
func discardLowerRange(data []float64, k int) []float64 {
	out := make([]float64, len(data)-k)
	i := 0

	// discard values lower than the desired range
	for k > 0 {
		lows, pivotValue, highs := partition(data)

		lowLength := len(lows)
		if lowLength > k {
			// keep all the highs and the pivot
			out[i] = pivotValue
			i++
			copy(out[i:], highs)
			i += len(highs)
			// iterate over the lows again
			data = lows
		} else {
			// discard all the lows
			data = highs
			k -= lowLength
			if k == 0 {
				// if discarded enough lows, keep the pivot
				out[i] = pivotValue
				i++
			} else {
				// able to discard the pivot too
				k--
			}
		}
	}
	copy(out[i:], data)
	return out
}

// discardUpperRange discards the upper k elements of the sorted data set without sorting all the data. Sorting all of
// the data would take O(NlgN), where N is len(data), but partitioning to find the kth largest number is O(N) in the
// average case. The remaining N-k unsorted elements are returned - no kind of ordering is guaranteed on these elements.
func discardUpperRange(data []float64, k int) []float64 {
	out := make([]float64, len(data)-k)
	i := 0

	// discard values higher than the desired range
	for k > 0 {
		lows, pivotValue, highs := partition(data)

		highLength := len(highs)
		if highLength > k {
			// keep all the lows and the pivot
			out[i] = pivotValue
			i++
			copy(out[i:], lows)
			i += len(lows)
			// iterate over the highs again
			data = highs
		} else {
			// discard all the highs
			data = lows
			k -= highLength
			if k == 0 {
				// if discarded enough highs, keep the pivot
				out[i] = pivotValue
				i++
			} else {
				// able to discard the pivot too
				k--
			}
		}
	}
	copy(out[i:], data)
	return out
}

// partition takes a list of data, chooses a random pivot index and returns a list of elements lower than the
// pivotValue, the pivotValue, and a list of elements higher than the pivotValue.  partition mutates data.
func partition(data []float64) (lows []float64, pivotValue float64, highs []float64) {
	length := len(data)
	// there are better (more complex) ways to calculate pivotIndex (e.g. median of 3, median of 3 medians) if this
	// proves to be inadequate.
	pivotIndex := rand.Int() % length
	pivotValue = data[pivotIndex]
	low, high := 1, length-1

	// put the pivot in the first position
	data[pivotIndex], data[0] = data[0], data[pivotIndex]

	// partition the data around the pivot
	for low <= high {
		for low <= high && data[low] <= pivotValue {
			low++
		}
		for high >= low && data[high] >= pivotValue {
			high--
		}
		if low < high {
			data[low], data[high] = data[high], data[low]
		}
	}

	return data[1:low], pivotValue, data[high+1:]
}

type minMaxMapOut struct {
	Val  float64
	Type NumberType
}

// MapMin collects the values to pass to the reducer
func MapMin(itr Iterator) interface{} {
	min := &minMaxMapOut{}

	pointsYielded := false
	var val float64

	for k, v := itr.Next(); k != -1; k, v = itr.Next() {
		switch n := v.(type) {
		case float64:
			val = n
		case int64:
			val = float64(n)
			min.Type = Int64Type
		}

		// Initialize min
		if !pointsYielded {
			min.Val = val
			pointsYielded = true
		}
		min.Val = math.Min(min.Val, val)
	}
	if pointsYielded {
		return min
	}
	return nil
}

// ReduceMin computes the min of value.
func ReduceMin(values []interface{}) interface{} {
	min := &minMaxMapOut{}
	pointsYielded := false

	for _, value := range values {
		if value == nil {
			continue
		}

		v, ok := value.(*minMaxMapOut)
		if !ok {
			continue
		}

		// Initialize min
		if !pointsYielded {
			min.Val = v.Val
			min.Type = v.Type
			pointsYielded = true
		}
		min.Val = math.Min(min.Val, v.Val)
	}
	if pointsYielded {
		switch min.Type {
		case Float64Type:
			return min.Val
		case Int64Type:
			return int64(min.Val)
		}
	}
	return nil
}

// MapMax collects the values to pass to the reducer
func MapMax(itr Iterator) interface{} {
	max := &minMaxMapOut{}

	pointsYielded := false
	var val float64

	for k, v := itr.Next(); k != -1; k, v = itr.Next() {
		switch n := v.(type) {
		case float64:
			val = n
		case int64:
			val = float64(n)
			max.Type = Int64Type
		}

		// Initialize max
		if !pointsYielded {
			max.Val = val
			pointsYielded = true
		}
		max.Val = math.Max(max.Val, val)
	}
	if pointsYielded {
		return max
	}
	return nil
}

// ReduceMax computes the max of value.
func ReduceMax(values []interface{}) interface{} {
	max := &minMaxMapOut{}
	pointsYielded := false

	for _, value := range values {
		if value == nil {
			continue
		}

		v, ok := value.(*minMaxMapOut)
		if !ok {
			continue
		}

		// Initialize max
		if !pointsYielded {
			max.Val = v.Val
			max.Type = v.Type
			pointsYielded = true
		}
		max.Val = math.Max(max.Val, v.Val)
	}
	if pointsYielded {
		switch max.Type {
		case Float64Type:
			return max.Val
		case Int64Type:
			return int64(max.Val)
		}
	}
	return nil
}

type spreadMapOutput struct {
	Min, Max float64
	Type     NumberType
}

// MapSpread collects the values to pass to the reducer
func MapSpread(itr Iterator) interface{} {
	out := &spreadMapOutput{}
	pointsYielded := false
	var val float64

	for k, v := itr.Next(); k != -1; k, v = itr.Next() {
		switch n := v.(type) {
		case float64:
			val = n
		case int64:
			val = float64(n)
			out.Type = Int64Type
		}

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
		return out
	}
	return nil
}

// ReduceSpread computes the spread of values.
func ReduceSpread(values []interface{}) interface{} {
	result := &spreadMapOutput{}
	pointsYielded := false

	for _, v := range values {
		if v == nil {
			continue
		}
		val := v.(*spreadMapOutput)
		// Initialize
		if !pointsYielded {
			result.Max = val.Max
			result.Min = val.Min
			result.Type = val.Type
			pointsYielded = true
		}
		result.Max = math.Max(result.Max, val.Max)
		result.Min = math.Min(result.Min, val.Min)
	}
	if pointsYielded {
		switch result.Type {
		case Float64Type:
			return result.Max - result.Min
		case Int64Type:
			return int64(result.Max - result.Min)
		}
	}
	return nil
}

// MapStddev collects the values to pass to the reducer
func MapStddev(itr Iterator) interface{} {
	var values []float64

	for k, v := itr.Next(); k != -1; k, v = itr.Next() {
		switch n := v.(type) {
		case float64:
			values = append(values, n)
		case int64:
			values = append(values, float64(n))
		}
	}

	return values
}

// ReduceStddev computes the stddev of values.
func ReduceStddev(values []interface{}) interface{} {
	var data []float64
	// Collect all the data points
	for _, value := range values {
		if value == nil {
			continue
		}
		data = append(data, value.([]float64)...)
	}

	// If no data or we only have one point, it's nil or undefined
	if len(data) < 2 {
		return nil
	}

	// Get the mean
	var mean float64
	var count int
	for _, v := range data {
		count++
		mean += (v - mean) / float64(count)
	}
	// Get the variance
	var variance float64
	for _, v := range data {
		dif := v - mean
		sq := math.Pow(dif, 2)
		variance += sq
	}
	variance = variance / float64(count-1)
	stddev := math.Sqrt(variance)

	return stddev
}

type firstLastMapOutput struct {
	Time int64
	Val  interface{}
}

// MapFirst collects the values to pass to the reducer
// This function assumes time ordered input
func MapFirst(itr Iterator) interface{} {
	k, v := itr.Next()
	if k == -1 {
		return nil
	}
	nextk, nextv := itr.Next()
	for nextk == k {
		if greaterThan(nextv, v) {
			v = nextv
		}
		nextk, nextv = itr.Next()
	}
	return &firstLastMapOutput{k, v}
}

// ReduceFirst computes the first of value.
func ReduceFirst(values []interface{}) interface{} {
	out := &firstLastMapOutput{}
	pointsYielded := false

	for _, v := range values {
		if v == nil {
			continue
		}
		val := v.(*firstLastMapOutput)
		// Initialize first
		if !pointsYielded {
			out.Time = val.Time
			out.Val = val.Val
			pointsYielded = true
		}
		if val.Time < out.Time {
			out.Time = val.Time
			out.Val = val.Val
		} else if val.Time == out.Time && greaterThan(val.Val, out.Val) {
			out.Val = val.Val
		}
	}
	if pointsYielded {
		return out.Val
	}
	return nil
}

// MapLast collects the values to pass to the reducer
func MapLast(itr Iterator) interface{} {
	out := &firstLastMapOutput{}
	pointsYielded := false

	for k, v := itr.Next(); k != -1; k, v = itr.Next() {
		// Initialize last
		if !pointsYielded {
			out.Time = k
			out.Val = v
			pointsYielded = true
		}
		if k > out.Time {
			out.Time = k
			out.Val = v
		} else if k == out.Time && greaterThan(v, out.Val) {
			out.Val = v
		}
	}
	if pointsYielded {
		return out
	}
	return nil
}

// ReduceLast computes the last of value.
func ReduceLast(values []interface{}) interface{} {
	out := &firstLastMapOutput{}
	pointsYielded := false

	for _, v := range values {
		if v == nil {
			continue
		}

		val := v.(*firstLastMapOutput)
		// Initialize last
		if !pointsYielded {
			out.Time = val.Time
			out.Val = val.Val
			pointsYielded = true
		}
		if val.Time > out.Time {
			out.Time = val.Time
			out.Val = val.Val
		} else if val.Time == out.Time && greaterThan(val.Val, out.Val) {
			out.Val = val.Val
		}
	}
	if pointsYielded {
		return out.Val
	}
	return nil
}

type positionOut struct {
	points   PositionPoints
	callArgs []string // ordered args in the call
}

func (p *positionOut) lessKey(a, b *PositionPoint) bool {
	t1, t2 := a.Tags, b.Tags
	for _, k := range p.callArgs {
		if t1[k] != t2[k] {
			return t1[k] < t2[k]
		}
	}
	return false
}

// typeCompare compares the types of a and b and returns an arbitrary ordering.
// It returns -1 if type(a) < type(b) , 0 if type(a) == type(b), or 1 if type(a) > type(b), following the strcmp convention
// from C.
//
// If the types are not equal, then it will attempt to coerce them to floating point and return them in the last 2 arguments.
// If the type cannot be coerced to floating point, it is returned unaltered.
func typeCompare(a, b interface{}) (int, interface{}, interface{}) {
	const (
		stringWeight = iota
		boolWeight
		intWeight
		floatWeight
	)

	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)

	vakind := va.Type().Kind()
	vbkind := vb.Type().Kind()

	// same kind. Ordering is dependent on value
	if vakind == vbkind {
		return 0, a, b
	}
	wa, a := inferFloat(va)
	wb, b := inferFloat(vb)
	if wa < wb {
		return -1, a, b
	} else if wa == wb {
		return 0, a, b
	}
	return 1, a, b
}

// returns a weighting and if applicable, the value coerced to a float
func inferFloat(v reflect.Value) (weight int, value interface{}) {
	const (
		stringWeight = iota
		boolWeight
		intWeight
		floatWeight
	)
	kind := v.Kind()
	switch kind {
	case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return intWeight, float64(v.Uint())
	case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return intWeight, float64(v.Int())
	case reflect.Float64, reflect.Float32:
		return floatWeight, v.Float()
	case reflect.Bool:
		return boolWeight, v.Interface()
	case reflect.String:
		return stringWeight, v.Interface()
	}
	panic(fmt.Sprintf("interfaceValues.Less - unreachable code; type was %T", v.Interface()))
}

func cmpFloat(a, b float64) int {
	if a == b {
		return 0
	} else if a < b {
		return -1
	}
	return 1
}

func cmpInt(a, b int64) int {
	if a == b {
		return 0
	} else if a < b {
		return -1
	}
	return 1
}

func cmpUint(a, b uint64) int {
	if a == b {
		return 0
	} else if a < b {
		return -1
	}
	return 1
}

// valueCompare returns -1 if a < b , 0 if a == b, or 1 if a > b
// If the interfaces are 2 different types, then 0 is returned
func valueCompare(a, b interface{}) int {
	if reflect.TypeOf(a).Kind() != reflect.TypeOf(b).Kind() {
		return 0
	}
	// compare by float64/int64 first as that is the most likely match
	{
		d1, ok1 := a.(float64)
		d2, ok2 := b.(float64)
		if ok1 && ok2 {
			return cmpFloat(d1, d2)
		}
	}

	{
		d1, ok1 := a.(int64)
		d2, ok2 := b.(int64)
		if ok1 && ok2 {
			return cmpInt(d1, d2)
		}
	}

	// compare by every numeric type left
	{
		d1, ok1 := a.(float32)
		d2, ok2 := b.(float32)
		if ok1 && ok2 {
			return cmpFloat(float64(d1), float64(d2))
		}
	}

	{
		d1, ok1 := a.(uint64)
		d2, ok2 := b.(uint64)
		if ok1 && ok2 {
			return cmpUint(d1, d2)
		}
	}

	{
		d1, ok1 := a.(uint32)
		d2, ok2 := b.(uint32)
		if ok1 && ok2 {
			return cmpUint(uint64(d1), uint64(d2))
		}
	}

	{
		d1, ok1 := a.(uint16)
		d2, ok2 := b.(uint16)
		if ok1 && ok2 {
			return cmpUint(uint64(d1), uint64(d2))
		}
	}

	{
		d1, ok1 := a.(uint8)
		d2, ok2 := b.(uint8)
		if ok1 && ok2 {
			return cmpUint(uint64(d1), uint64(d2))
		}
	}

	{
		d1, ok1 := a.(int32)
		d2, ok2 := b.(int32)
		if ok1 && ok2 {
			return cmpInt(int64(d1), int64(d2))
		}
	}

	{
		d1, ok1 := a.(int16)
		d2, ok2 := b.(int16)
		if ok1 && ok2 {
			return cmpInt(int64(d1), int64(d2))
		}
	}

	{
		d1, ok1 := a.(int8)
		d2, ok2 := b.(int8)
		if ok1 && ok2 {
			return cmpInt(int64(d1), int64(d2))
		}
	}

	{
		d1, ok1 := a.(bool)
		d2, ok2 := b.(bool)
		if ok1 && ok2 {
			if d1 == d2 {
				return 0
			} else if d1 == true && d2 == false {
				return 1
			}
			return -1
		}
	}

	{
		d1, ok1 := a.(string)
		d2, ok2 := b.(string)
		if ok1 && ok2 {
			return strings.Compare(d1, d2)
		}
	}
	panic(fmt.Sprintf("unreachable code; types were %T, %T", a, b))
}

type PositionPoints []PositionPoint
type PositionPoint struct {
	Time  int64
	Value interface{}
	Tags  map[string]string
}

type topBottomMapOut struct {
	*positionOut
	bottom bool
}

func (t *topBottomMapOut) Len() int      { return len(t.points) }
func (t *topBottomMapOut) Swap(i, j int) { t.points[i], t.points[j] = t.points[j], t.points[i] }
func (t *topBottomMapOut) Less(i, j int) bool {
	return t.positionPointLess(&t.points[i], &t.points[j])
}

func (t *topBottomMapOut) positionPointLess(pa, pb *PositionPoint) bool {
	// old C trick makes this code easier to read. Imagine
	// that the OP in "cmp(i, j) OP 0" is the comparison you want
	// between i and j
	cmpt, a, b := typeCompare(pa.Value, pb.Value)
	cmpv := valueCompare(a, b)
	if cmpv != 0 {
		if t.bottom {
			return cmpv > 0
		}
		return cmpv < 0
	}
	if cmpt != 0 {
		return cmpt < 0
	}
	k1, k2 := pa.Time, pb.Time
	if k1 != k2 {
		return k1 > k2
	}
	return !t.lessKey(pa, pb)
}

// We never use this function, so make it a no-op.
func (t *topBottomMapOut) Push(i interface{}) {
	panic("someone used the function")
}

// this function doesn't return anything meaningful, since we don't look at the
// return value and we don't want to allocate for generating an interface.
func (t *topBottomMapOut) Pop() interface{} {
	t.points = t.points[:len(t.points)-1]
	return nil
}

func (t *topBottomMapOut) insert(p PositionPoint) {
	t.points[0] = p
	heap.Fix(t, 0)
}

type topBottomReduceOut struct {
	positionOut
	bottom bool
}

func (t topBottomReduceOut) Len() int      { return len(t.points) }
func (t topBottomReduceOut) Swap(i, j int) { t.points[i], t.points[j] = t.points[j], t.points[i] }
func (t topBottomReduceOut) Less(i, j int) bool {
	// Now sort by time first, not value

	k1, k2 := t.points[i].Time, t.points[j].Time
	if k1 != k2 {
		return k1 < k2
	}
	cmpt, a, b := typeCompare(t.points[i].Value, t.points[j].Value)
	cmpv := valueCompare(a, b)
	if cmpv != 0 {
		if t.bottom {
			return cmpv < 0
		}
		return cmpv > 0
	}
	if cmpt != 0 {
		return cmpt < 0
	}
	return t.lessKey(&t.points[i], &t.points[j])
}

// callArgs will get any additional field/tag names that may be needed to sort with
// it is important to maintain the order of these that they were asked for in the call
// for sorting purposes
func topCallArgs(c *influxql.Call) []string {
	var names []string
	for _, v := range c.Args[1 : len(c.Args)-1] {
		if f, ok := v.(*influxql.VarRef); ok {
			names = append(names, f.Val)
		}
	}
	return names
}

func tagkeytop(args []string, fields map[string]interface{}, keys map[string]string) string {
	key := ""
	for _, a := range args {
		if v, ok := fields[a]; ok {
			key += a + ":" + fmt.Sprintf("%v", v) + ","
			continue
		}
		if v, ok := keys[a]; ok {
			key += a + ":" + v + ","
			continue
		}
	}
	return key
}

// map iterator. We need this for the top
// query, but luckily that doesn't require ordered
// iteration, so we can fake it
type mapIter struct {
	m        map[string]PositionPoint
	currTags map[string]string
	tmin     int64
}

func (m *mapIter) TMin() int64 {
	return m.tmin
}

func (m *mapIter) Tags() map[string]string {
	return m.currTags
}

func (m *mapIter) Next() (time int64, value interface{}) {
	// this is a bit ugly, but can't think of  any other way that doesn't involve dumping
	// the entire map to an array
	for key, p := range m.m {
		m.currTags = p.Tags
		time = p.Time
		value = p.Value
		delete(m.m, key)
		return
	}
	return -1, nil
}

// MapTopBottom emits the top/bottom data points for each group by interval
func MapTopBottom(itr Iterator, c *influxql.Call) interface{} {
	// Capture the limit if it was specified in the call
	lit, _ := c.Args[len(c.Args)-1].(*influxql.NumberLiteral)
	limit := int(lit.Val)

	out := positionOut{callArgs: topCallArgs(c)}
	out.points = make([]PositionPoint, 0, limit)
	minheap := topBottomMapOut{
		&out,
		c.Name == "bottom",
	}
	tagmap := make(map[string]PositionPoint)

	// throughout this function, we refer to max and top. This is by the ordering specified by
	// minheap, not the ordering based on value. Since this function handles both top and bottom
	// max can be the lowest valued entry.

	// buffer so we don't allocate every time through
	var pp PositionPoint
	if len(c.Args) > 2 {
		// this is a tag aggregating query.
		// For each unique permutation of the tags given,
		// select the max and then fall through to select top of those
		// points
		for k, v := itr.Next(); k != -1; k, v = itr.Next() {
			pp = PositionPoint{k, v, itr.Tags()}
			callArgs := c.Fields()
			tags := itr.Tags()
			// TODO in the future we need to send in fields as well
			// this will allow a user to query on both fields and tags
			// fields will take the priority over tags if there is a name collision
			key := tagkeytop(callArgs, nil, tags)
			p, ok := tagmap[key]
			if !ok || minheap.positionPointLess(&p, &pp) {
				tagmap[key] = pp
			}
		}
		itr = &mapIter{
			m:    tagmap,
			tmin: itr.TMin(),
		}
	}
	for k, v := itr.Next(); k != -1; k, v = itr.Next() {
		t := k
		if bt := itr.TMin(); bt > -1 {
			t = bt
		}
		if len(out.points) < limit {
			out.points = append(out.points, PositionPoint{t, v, itr.Tags()})
			if len(out.points) == limit {
				heap.Init(&minheap)
			}
		} else {
			// we're over the limit, so find out if we're bigger than the
			// smallest point in the set and eject it if we are
			minval := &out.points[0]
			pp = PositionPoint{t, v, itr.Tags()}
			if minheap.positionPointLess(minval, &pp) {
				minheap.insert(pp)
			}
		}
	}
	// should only happen on empty iterator.
	if len(out.points) == 0 {
		return nil
	} else if len(out.points) < limit {
		// it would be as fast to just sort regularly here,
		// but falling down to the heapsort will mean we can get
		// rid of another sort order.
		heap.Init(&minheap)
	}
	// minheap should now contain the largest/smallest values that were encountered
	// during iteration.
	//
	// we want these values in ascending sorted order. We can achieve this by iteratively
	// removing the lowest element and putting it at the end of the array. This is analogous
	// to a heap sort.
	//
	// computer science is fun!
	result := out.points
	for len(out.points) > 0 {
		p := out.points[0]
		heap.Pop(&minheap)
		// reslice so that we can get to the element just after the heap
		endslice := out.points[:len(out.points)+1]
		endslice[len(endslice)-1] = p
	}
	// the ascending order is now in the result slice
	return result
}

// ReduceTop computes the top values for each key.
// This function assumes that its inputs are in sorted ascending order.
func ReduceTopBottom(values []interface{}, c *influxql.Call) interface{} {
	lit, _ := c.Args[len(c.Args)-1].(*influxql.NumberLiteral)
	limit := int(lit.Val)

	out := positionOut{callArgs: topCallArgs(c)}
	minheap := topBottomMapOut{&out, c.Name == "bottom"}
	results := make([]PositionPoints, 0, len(values))
	out.points = make([]PositionPoint, 0, limit)
	for _, v := range values {
		if v == nil {
			continue
		}
		o, ok := v.(PositionPoints)
		if ok {
			results = append(results, o)
		}
	}
	// These ranges are all in sorted ascending order
	// so we can grab the top value out of all of them
	// to figure out the top X ones.
	for i := 0; i < limit; i++ {
		var max *PositionPoint
		whichselected := -1
		for iter, v := range results {
			if len(v) > 0 && (max == nil || minheap.positionPointLess(max, &v[0])) {
				max = &v[0]
				whichselected = iter
			}
		}
		if whichselected == -1 {
			// none of the points have any values
			// so we can return what we have now
			sort.Sort(topBottomReduceOut{out, c.Name == "bottom"})
			return out.points
		}
		v := results[whichselected]
		out.points = append(out.points, v[0])
		results[whichselected] = v[1:]
	}

	// now we need to resort the tops by time
	sort.Sort(topBottomReduceOut{out, c.Name == "bottom"})
	return out.points
}

// MapEcho emits the data points for each group by interval
func MapEcho(itr Iterator) interface{} {
	var values []interface{}

	for k, v := itr.Next(); k != -1; k, v = itr.Next() {
		values = append(values, v)
	}
	return values
}

// ReducePercentile computes the percentile of values for each key.
func ReducePercentile(values []interface{}, c *influxql.Call) interface{} {
	// Checks that this arg exists and is a valid type are done in the parsing validation
	// and have test coverage there
	lit, _ := c.Args[1].(*influxql.NumberLiteral)
	percentile := lit.Val

	var allValues []float64

	for _, v := range values {
		if v == nil {
			continue
		}

		vals := v.([]interface{})
		for _, v := range vals {
			switch v.(type) {
			case int64:
				allValues = append(allValues, float64(v.(int64)))
			case float64:
				allValues = append(allValues, v.(float64))
			}
		}
	}

	sort.Float64s(allValues)
	length := len(allValues)
	index := int(math.Floor(float64(length)*percentile/100.0+0.5)) - 1

	if index < 0 || index >= len(allValues) {
		return nil
	}

	return allValues[index]
}

// IsNumeric returns whether a given aggregate can only be run on numeric fields.
func IsNumeric(c *influxql.Call) bool {
	switch c.Name {
	case "count", "first", "last", "distinct":
		return false
	default:
		return true
	}
}

// MapRawQuery is for queries without aggregates
func MapRawQuery(itr Iterator) interface{} {
	var values []*rawQueryMapOutput
	for k, v := itr.Next(); k != -1; k, v = itr.Next() {
		val := &rawQueryMapOutput{k, v}
		values = append(values, val)
	}
	return values
}

type rawQueryMapOutput struct {
	Time   int64
	Values interface{}
}

func (r *rawQueryMapOutput) String() string {
	return fmt.Sprintf("{%#v %#v}", r.Time, r.Values)
}

type rawOutputs []*rawQueryMapOutput

func (a rawOutputs) Len() int           { return len(a) }
func (a rawOutputs) Less(i, j int) bool { return a[i].Time < a[j].Time }
func (a rawOutputs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func greaterThan(a, b interface{}) bool {
	switch t := a.(type) {
	case int64:
		return t > b.(int64)
	case float64:
		return t > b.(float64)
	case string:
		return t > b.(string)
	case bool:
		return t == true
	}
	return false
}
