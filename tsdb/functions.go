package tsdb

// All aggregate and query functions are defined in this file along with any intermediate data objects they need to process.
// Query functions are represented as two discreet functions: Map and Reduce. These roughly follow the MapReduce
// paradigm popularized by Google and Hadoop.
//
// When adding an aggregate function, define a mapper, a reducer, and add them in the switch statement in the MapreduceFuncs function

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"

	"github.com/influxdb/influxdb/influxql"
)

// iterator represents a forward-only iterator over a set of points.
// These are used by the mapFunctions in this file
type iterator interface {
	Next() (time int64, value interface{})
	Tags() map[string]string
	TMin() int64
}

// mapFunc represents a function used for mapping over a sequential series of data.
// The iterator represents a single group by interval
type mapFunc func(iterator) interface{}

// reduceFunc represents a function used for reducing mapper output.
type reduceFunc func([]interface{}) interface{}

// UnmarshalFunc represents a function that can take bytes from a mapper from remote
// server and marshal it into an interface the reducer can use
type unmarshalFunc func([]byte) (interface{}, error)

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
	case "top":
		return func(itr iterator) interface{} {
			return MapTop(itr, c)
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
	case "top":
		return func(values []interface{}) interface{} {
			return ReduceTop(values, c)
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

func initializeUnmarshaller(c *influxql.Call) (unmarshalFunc, error) {
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
func MapCount(itr iterator) interface{} {
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
	// Sort by type if types match

	// Sort by float64/int64 first as that is the most likely match
	{
		d1, ok1 := d[i].(float64)
		d2, ok2 := d[j].(float64)
		if ok1 && ok2 {
			return d1 < d2
		}
	}

	{
		d1, ok1 := d[i].(int64)
		d2, ok2 := d[j].(int64)
		if ok1 && ok2 {
			return d1 < d2
		}
	}

	// Sort by every numeric type left
	{
		d1, ok1 := d[i].(float32)
		d2, ok2 := d[j].(float32)
		if ok1 && ok2 {
			return d1 < d2
		}
	}

	{
		d1, ok1 := d[i].(uint64)
		d2, ok2 := d[j].(uint64)
		if ok1 && ok2 {
			return d1 < d2
		}
	}

	{
		d1, ok1 := d[i].(uint32)
		d2, ok2 := d[j].(uint32)
		if ok1 && ok2 {
			return d1 < d2
		}
	}

	{
		d1, ok1 := d[i].(uint16)
		d2, ok2 := d[j].(uint16)
		if ok1 && ok2 {
			return d1 < d2
		}
	}

	{
		d1, ok1 := d[i].(uint8)
		d2, ok2 := d[j].(uint8)
		if ok1 && ok2 {
			return d1 < d2
		}
	}

	{
		d1, ok1 := d[i].(int32)
		d2, ok2 := d[j].(int32)
		if ok1 && ok2 {
			return d1 < d2
		}
	}

	{
		d1, ok1 := d[i].(int16)
		d2, ok2 := d[j].(int16)
		if ok1 && ok2 {
			return d1 < d2
		}
	}

	{
		d1, ok1 := d[i].(int8)
		d2, ok2 := d[j].(int8)
		if ok1 && ok2 {
			return d1 < d2
		}
	}

	{
		d1, ok1 := d[i].(bool)
		d2, ok2 := d[j].(bool)
		if ok1 && ok2 {
			return d1 == false && d2 == true
		}
	}

	{
		d1, ok1 := d[i].(string)
		d2, ok2 := d[j].(string)
		if ok1 && ok2 {
			return d1 < d2
		}
	}

	// Types did not match, need to sort based on arbitrary weighting of type
	const (
		intWeight = iota
		floatWeight
		boolWeight
		stringWeight
	)

	infer := func(val interface{}) (int, float64) {
		switch v := val.(type) {
		case uint64:
			return intWeight, float64(v)
		case uint32:
			return intWeight, float64(v)
		case uint16:
			return intWeight, float64(v)
		case uint8:
			return intWeight, float64(v)
		case int64:
			return intWeight, float64(v)
		case int32:
			return intWeight, float64(v)
		case int16:
			return intWeight, float64(v)
		case int8:
			return intWeight, float64(v)
		case float64:
			return floatWeight, float64(v)
		case float32:
			return floatWeight, float64(v)
		case bool:
			return boolWeight, 0
		case string:
			return stringWeight, 0
		}
		panic("interfaceValues.Less - unreachable code")
	}

	w1, n1 := infer(d[i])
	w2, n2 := infer(d[j])

	// If we had "numeric" data, use that for comparison
	if n1 != n2 && (w1 == intWeight && w2 == floatWeight) || (w1 == floatWeight && w2 == intWeight) {
		return n1 < n2
	}

	return w1 < w2
}

// MapDistinct computes the unique values in an iterator.
func MapDistinct(itr iterator) interface{} {
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
func MapCountDistinct(itr iterator) interface{} {
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
func MapSum(itr iterator) interface{} {
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
func MapMean(itr iterator) interface{} {
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
func MapMin(itr iterator) interface{} {
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
func MapMax(itr iterator) interface{} {
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
func MapSpread(itr iterator) interface{} {
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
func MapStddev(itr iterator) interface{} {
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
func MapFirst(itr iterator) interface{} {
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
func MapLast(itr iterator) interface{} {
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

func (p *positionOut) lessKey(i, j int) bool {
	t1, t2 := p.points[i].Tags, p.points[j].Tags
	for _, k := range p.callArgs {
		if t1[k] != t2[k] {
			return t1[k] < t2[k]
		}
	}
	return false
}

func (p *positionOut) less(i, j int, sortFloat func(d1, d2 float64) bool, sortInt64 func(d1, d2 int64) bool, sortUint64 func(d1, d2 uint64) bool) bool {
	// Sort by float64/int64 first as that is the most likely match
	{
		d1, ok1 := p.points[i].Value.(float64)
		d2, ok2 := p.points[j].Value.(float64)
		if ok1 && ok2 {
			return sortFloat(d1, d2)
		}
	}

	{
		d1, ok1 := p.points[i].Value.(int64)
		d2, ok2 := p.points[j].Value.(int64)
		if ok1 && ok2 {
			return sortInt64(d1, d2)
		}
	}

	// Sort by every numeric type left
	{
		d1, ok1 := p.points[i].Value.(float32)
		d2, ok2 := p.points[j].Value.(float32)
		if ok1 && ok2 {
			return sortFloat(float64(d1), float64(d2))
		}
	}

	{
		d1, ok1 := p.points[i].Value.(uint64)
		d2, ok2 := p.points[j].Value.(uint64)
		if ok1 && ok2 {
			return sortUint64(d1, d2)
		}
	}

	{
		d1, ok1 := p.points[i].Value.(uint32)
		d2, ok2 := p.points[j].Value.(uint32)
		if ok1 && ok2 {
			return sortUint64(uint64(d1), uint64(d2))
		}
	}

	{
		d1, ok1 := p.points[i].Value.(uint16)
		d2, ok2 := p.points[j].Value.(uint16)
		if ok1 && ok2 {
			return sortUint64(uint64(d1), uint64(d2))
		}
	}

	{
		d1, ok1 := p.points[i].Value.(uint8)
		d2, ok2 := p.points[j].Value.(uint8)
		if ok1 && ok2 {
			return sortUint64(uint64(d1), uint64(d2))
		}
	}

	{
		d1, ok1 := p.points[i].Value.(int32)
		d2, ok2 := p.points[j].Value.(int32)
		if ok1 && ok2 {
			return sortInt64(int64(d1), int64(d2))
		}
	}

	{
		d1, ok1 := p.points[i].Value.(int16)
		d2, ok2 := p.points[j].Value.(int16)
		if ok1 && ok2 {
			return sortInt64(int64(d1), int64(d2))
		}
	}

	{
		d1, ok1 := p.points[i].Value.(int8)
		d2, ok2 := p.points[j].Value.(int8)
		if ok1 && ok2 {
			return sortInt64(int64(d1), int64(d2))
		}
	}

	{
		d1, ok1 := p.points[i].Value.(bool)
		d2, ok2 := p.points[j].Value.(bool)
		if ok1 && ok2 {
			return d1 == true && d2 == false
		}
	}

	{
		d1, ok1 := p.points[i].Value.(string)
		d2, ok2 := p.points[j].Value.(string)
		if ok1 && ok2 {
			return d1 < d2
		}
	}

	// Types did not match, need to sort based on arbitrary weighting of type
	const (
		intWeight = iota
		floatWeight
		boolWeight
		stringWeight
	)

	infer := func(val interface{}) (int, float64) {
		switch v := val.(type) {
		case uint64:
			return intWeight, float64(v)
		case uint32:
			return intWeight, float64(v)
		case uint16:
			return intWeight, float64(v)
		case uint8:
			return intWeight, float64(v)
		case int64:
			return intWeight, float64(v)
		case int32:
			return intWeight, float64(v)
		case int16:
			return intWeight, float64(v)
		case int8:
			return intWeight, float64(v)
		case float64:
			return floatWeight, float64(v)
		case float32:
			return floatWeight, float64(v)
		case bool:
			return boolWeight, 0
		case string:
			return stringWeight, 0
		}
		panic("interfaceValues.Less - unreachable code")
	}

	w1, n1 := infer(p.points[i].Value)
	w2, n2 := infer(p.points[j].Value)

	// If we had "numeric" data, use that for comparison
	if (w1 == floatWeight || w1 == intWeight) && (w2 == floatWeight || w2 == intWeight) {
		return sortFloat(n1, n2)
	}

	return w1 < w2

}

type PositionPoints []PositionPoint
type PositionPoint struct {
	Time  int64
	Value interface{}
	Tags  map[string]string
}

type topMapOut struct {
	positionOut
}

func (t topMapOut) Len() int      { return len(t.points) }
func (t topMapOut) Swap(i, j int) { t.points[i], t.points[j] = t.points[j], t.points[i] }
func (t topMapOut) Less(i, j int) bool {
	sortFloat := func(d1, d2 float64) bool {
		if d1 != d2 {
			return d1 > d2
		}
		k1, k2 := t.points[i].Time, t.points[j].Time
		if k1 != k2 {
			return k1 < k2
		}
		return t.lessKey(i, j)
	}

	sortInt64 := func(d1, d2 int64) bool {
		if d1 != d2 {
			return d1 > d2
		}
		k1, k2 := t.points[i].Time, t.points[j].Time
		if k1 != k2 {
			return k1 < k2
		}
		return t.lessKey(i, j)
	}

	sortUint64 := func(d1, d2 uint64) bool {
		if d1 != d2 {
			return d1 > d2
		}
		k1, k2 := t.points[i].Time, t.points[j].Time
		if k1 != k2 {
			return k1 < k2
		}
		return t.lessKey(i, j)
	}
	return t.less(i, j, sortFloat, sortInt64, sortUint64)
}

type topReduceOut struct {
	positionOut
}

func (t topReduceOut) Len() int      { return len(t.points) }
func (t topReduceOut) Swap(i, j int) { t.points[i], t.points[j] = t.points[j], t.points[i] }
func (t topReduceOut) Less(i, j int) bool {
	// Now sort by time first, not value
	sortFloat := func(d1, d2 float64) bool {
		k1, k2 := t.points[i].Time, t.points[j].Time
		if k1 != k2 {
			return k1 < k2
		}
		if d1 != d2 {
			return d1 > d2
		}
		return t.lessKey(i, j)
	}

	sortInt64 := func(d1, d2 int64) bool {
		k1, k2 := t.points[i].Time, t.points[j].Time
		if k1 != k2 {
			return k1 < k2
		}
		if d1 != d2 {
			return d1 > d2
		}
		return t.lessKey(i, j)
	}

	sortUint64 := func(d1, d2 uint64) bool {
		k1, k2 := t.points[i].Time, t.points[j].Time
		if k1 != k2 {
			return k1 < k2
		}
		if d1 != d2 {
			return d1 > d2
		}
		return t.lessKey(i, j)
	}
	return t.less(i, j, sortFloat, sortInt64, sortUint64)
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

// MapTop emits the top data points for each group by interval
func MapTop(itr iterator, c *influxql.Call) interface{} {
	// Capture the limit if it was specified in the call
	lit, _ := c.Args[len(c.Args)-1].(*influxql.NumberLiteral)
	limit := int64(lit.Val)

	// Simple case where only value and limit are specified.
	if len(c.Args) == 2 {
		out := positionOut{callArgs: topCallArgs(c)}

		for k, v := itr.Next(); k != -1; k, v = itr.Next() {
			t := k
			if bt := itr.TMin(); bt > -1 {
				t = bt
			}
			out.points = append(out.points, PositionPoint{t, v, itr.Tags()})
		}

		// If we have more than we asked for, only send back the top values
		if int64(len(out.points)) > limit {
			sort.Sort(topMapOut{out})
			out.points = out.points[:limit]
		}
		if len(out.points) > 0 {
			return out.points
		}
		return nil
	}
	// They specified tags in the call to get unique sets, so we need to map them as we accumulate them
	outMap := make(map[string]positionOut)

	mapKey := func(args []string, fields map[string]interface{}, keys map[string]string) string {
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

	for k, v := itr.Next(); k != -1; k, v = itr.Next() {
		t := k
		if bt := itr.TMin(); bt > -1 {
			t = bt
		}
		callArgs := c.Fields()
		tags := itr.Tags()
		// TODO in the future we need to send in fields as well
		// this will allow a user to query on both fields and tags
		// fields will take the priority over tags if there is a name collision
		key := mapKey(callArgs, nil, tags)
		if out, ok := outMap[key]; ok {
			out.points = append(out.points, PositionPoint{t, v, itr.Tags()})
			outMap[key] = out
		} else {
			out = positionOut{callArgs: topCallArgs(c)}
			out.points = append(out.points, PositionPoint{t, v, itr.Tags()})
			outMap[key] = out
		}
	}
	// Sort all the maps
	for k, v := range outMap {
		sort.Sort(topMapOut{v})
		outMap[k] = v
	}

	slice := func(needed int64, m map[string]positionOut) PositionPoints {
		points := PositionPoints{}
		var collected int64
		for k, v := range m {
			if len(v.points) > 0 {
				points = append(points, v.points[0])
				v.points = v.points[1:]
				m[k] = v
				collected++
			}
		}
		o := positionOut{callArgs: topCallArgs(c), points: points}
		sort.Sort(topMapOut{o})
		points = o.points
		// If we got more than we needed, sort them and return the top
		if collected > needed {
			points = o.points[:needed]
		}

		return points
	}

	points := PositionPoints{}
	var collected int64
	for collected < limit {
		p := slice(limit-collected, outMap)
		if len(p) == 0 {
			break
		}
		points = append(points, p...)
		collected += int64(len(p))
	}
	if len(points) > 0 {
		return points
	}
	return nil
}

// ReduceTop computes the top values for each key.
func ReduceTop(values []interface{}, c *influxql.Call) interface{} {
	lit, _ := c.Args[len(c.Args)-1].(*influxql.NumberLiteral)
	limit := int64(lit.Val)

	out := positionOut{callArgs: topCallArgs(c)}
	for _, v := range values {
		if v == nil {
			continue
		}
		o, _ := v.(PositionPoints)
		out.points = append(out.points, o...)
	}

	// Get the top of the top values
	sort.Sort(topMapOut{out})
	// If we have more than we asked for, only send back the top values
	if int64(len(out.points)) > limit {
		out.points = out.points[:limit]
	}

	// now we need to resort the tops by time
	sort.Sort(topReduceOut{out})
	if len(out.points) > 0 {
		return out.points
	}
	return nil
}

// MapEcho emits the data points for each group by interval
func MapEcho(itr iterator) interface{} {
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
func MapRawQuery(itr iterator) interface{} {
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
