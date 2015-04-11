package influxql

// All aggregate and query functions are defined in this file along with any intermediate data objects they need to process.
// Query functions are represented as two discreet functions: Map and Reduce. These roughly follow the MapReduce
// paradigm popularized by Google and Hadoop.
//
// When adding an aggregate function, define a mapper, a reducer, and add them in the switch statement in the MapReduceFuncs function

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
)

// Iterator represents a forward-only iterator over a set of points.
// These are used by the MapFunctions in this file
type Iterator interface {
	Next() (seriesID uint64, timestamp int64, value interface{})
}

// MapFunc represents a function used for mapping over a sequential series of data.
// The iterator represents a single group by interval
type MapFunc func(Iterator) interface{}

// ReduceFunc represents a function used for reducing mapper output.
type ReduceFunc func([]interface{}) interface{}

// UnmarshalFunc represents a function that can take bytes from a mapper from remote
// server and marshal it into an interface the reduer can use
type UnmarshalFunc func([]byte) (interface{}, error)

// InitializeMapFunc takes an aggregate call from the query and returns the MapFunc
func InitializeMapFunc(c *Call) (MapFunc, error) {
	// see if it's a query for raw data
	if c == nil {
		return MapRawQuery, nil
	}

	// Ensure that there is either a single argument or if for percentile, two
	if c.Name == "percentile" {
		if len(c.Args) != 2 {
			return nil, fmt.Errorf("expected two arguments for percentile()")
		}
	} else if len(c.Args) != 1 {
		return nil, fmt.Errorf("expected one argument for %s()", c.Name)
	}

	// Ensure the argument is a variable reference.
	_, ok := c.Args[0].(*VarRef)
	if !ok {
		return nil, fmt.Errorf("expected field argument in %s()", c.Name)
	}

	// Retrieve map function by name.
	switch strings.ToLower(c.Name) {
	case "count":
		return MapCount, nil
	case "sum":
		return MapSum, nil
	case "mean":
		return MapMean, nil
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
	case "percentile":
		_, ok := c.Args[1].(*NumberLiteral)
		if !ok {
			return nil, fmt.Errorf("expected float argument in percentile()")
		}
		return MapEcho, nil
	default:
		return nil, fmt.Errorf("function not found: %q", c.Name)
	}
}

// InitializeReduceFunc takes an aggregate call from the query and returns the ReduceFunc
func InitializeReduceFunc(c *Call) (ReduceFunc, error) {
	// Retrieve reduce function by name.
	switch strings.ToLower(c.Name) {
	case "count":
		return ReduceSum, nil
	case "sum":
		return ReduceSum, nil
	case "mean":
		return ReduceMean, nil
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
	case "percentile":
		lit, ok := c.Args[1].(*NumberLiteral)
		if !ok {
			return nil, fmt.Errorf("expected float argument in percentile()")
		}
		return ReducePercentile(lit.Val), nil
	default:
		return nil, fmt.Errorf("function not found: %q", c.Name)
	}
}

func InitializeUnmarshaller(c *Call) (UnmarshalFunc, error) {
	// if c is nil it's a raw data query
	if c == nil {
		return func(b []byte) (interface{}, error) {
			a := make([]*rawQueryMapOutput, 0)
			err := json.Unmarshal(b, &a)
			return a, err
		}, nil
	}

	// Retrieve marshal function by name
	switch strings.ToLower(c.Name) {
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
	for _, k, _ := itr.Next(); k != 0; _, k, _ = itr.Next() {
		n++
	}
	if n > 0 {
		return n
	}
	return nil
}

// MapSum computes the summation of values in an iterator.
func MapSum(itr Iterator) interface{} {
	n := float64(0)
	count := 0
	for _, k, v := itr.Next(); k != 0; _, k, v = itr.Next() {
		count++
		n += v.(float64)
	}
	if count > 0 {
		return n
	}
	return nil
}

// ReduceSum computes the sum of values for each key.
func ReduceSum(values []interface{}) interface{} {
	var n float64
	count := 0
	for _, v := range values {
		if v == nil {
			continue
		}
		count++
		n += v.(float64)
	}
	if count > 0 {
		return n
	}
	return nil
}

// MapMean computes the count and sum of values in an iterator to be combined by the reducer.
func MapMean(itr Iterator) interface{} {
	out := &meanMapOutput{}

	for _, k, v := itr.Next(); k != 0; _, k, v = itr.Next() {
		out.Count++
		out.Sum += v.(float64)
	}
	return out
}

type meanMapOutput struct {
	Count int
	Sum   float64
}

// ReduceMean computes the mean of values for each key.
func ReduceMean(values []interface{}) interface{} {
	out := &meanMapOutput{}
	for _, v := range values {
		if v == nil {
			continue
		}
		val := v.(*meanMapOutput)
		out.Count += val.Count
		out.Sum += val.Sum
	}
	if out.Count > 0 {
		return out.Sum / float64(out.Count)
	}
	return nil
}

// MapMin collects the values to pass to the reducer
func MapMin(itr Iterator) interface{} {
	var min float64
	pointsYielded := false

	for _, k, v := itr.Next(); k != 0; _, k, v = itr.Next() {
		val := v.(float64)
		// Initialize min
		if !pointsYielded {
			min = val
			pointsYielded = true
		}
		min = math.Min(min, val)
	}
	if pointsYielded {
		return min
	}
	return nil
}

// ReduceMin computes the min of value.
func ReduceMin(values []interface{}) interface{} {
	var min float64
	pointsYielded := false

	for _, v := range values {
		if v == nil {
			continue
		}
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
		return min
	}
	return nil
}

// MapMax collects the values to pass to the reducer
func MapMax(itr Iterator) interface{} {
	var max float64
	pointsYielded := false

	for _, k, v := itr.Next(); k != 0; _, k, v = itr.Next() {
		val := v.(float64)
		// Initialize max
		if !pointsYielded {
			max = val
			pointsYielded = true
		}
		max = math.Max(max, val)
	}
	if pointsYielded {
		return max
	}
	return nil
}

// ReduceMax computes the max of value.
func ReduceMax(values []interface{}) interface{} {
	var max float64
	pointsYielded := false

	for _, v := range values {
		if v == nil {
			continue
		}
		val := v.(float64)
		// Initialize max
		if !pointsYielded {
			max = val
			pointsYielded = true
		}
		max = math.Max(max, val)
	}
	if pointsYielded {
		return max
	}
	return nil
}

type spreadMapOutput struct {
	Min, Max float64
}

// MapSpread collects the values to pass to the reducer
func MapSpread(itr Iterator) interface{} {
	var out spreadMapOutput
	pointsYielded := false

	for _, k, v := itr.Next(); k != 0; _, k, v = itr.Next() {
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
		return out
	}
	return nil
}

// ReduceSpread computes the spread of values.
func ReduceSpread(values []interface{}) interface{} {
	var result spreadMapOutput
	pointsYielded := false

	for _, v := range values {
		if v == nil {
			continue
		}
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
		return result.Max - result.Min
	}
	return nil
}

// MapStddev collects the values to pass to the reducer
func MapStddev(itr Iterator) interface{} {
	var values []float64

	for _, k, v := itr.Next(); k != 0; _, k, v = itr.Next() {
		values = append(values, v.(float64))
	}

	return nil
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

	return stddev
}

type firstLastMapOutput struct {
	Time int64
	Val  interface{}
}

// MapFirst collects the values to pass to the reducer
func MapFirst(itr Iterator) interface{} {
	out := firstLastMapOutput{}
	pointsYielded := false

	for _, k, v := itr.Next(); k != 0; _, k, v = itr.Next() {
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
		return out
	}
	return nil
}

// ReduceFirst computes the first of value.
func ReduceFirst(values []interface{}) interface{} {
	out := firstLastMapOutput{}
	pointsYielded := false

	for _, v := range values {
		if v == nil {
			continue
		}
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
		return out.Val
	}
	return nil
}

// MapLast collects the values to pass to the reducer
func MapLast(itr Iterator) interface{} {
	out := firstLastMapOutput{}
	pointsYielded := false

	for _, k, v := itr.Next(); k != 0; _, k, v = itr.Next() {
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
		return out
	}
	return nil
}

// ReduceLast computes the last of value.
func ReduceLast(values []interface{}) interface{} {
	out := firstLastMapOutput{}
	pointsYielded := false

	for _, v := range values {
		if v == nil {
			continue
		}

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
		return out.Val
	}
	return nil
}

// MapEcho emits the data points for each group by interval
func MapEcho(itr Iterator) interface{} {
	var values []interface{}

	for _, k, v := itr.Next(); k != 0; _, k, v = itr.Next() {
		values = append(values, v)
	}
	return values
}

// ReducePercentile computes the percentile of values for each key.
func ReducePercentile(percentile float64) ReduceFunc {
	return func(values []interface{}) interface{} {
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
			return nil
		}

		return allValues[index]
	}
}

// MapRawQuery is for queries without aggregates
func MapRawQuery(itr Iterator) interface{} {
	var values []*rawQueryMapOutput
	for _, k, v := itr.Next(); k != 0; _, k, v = itr.Next() {
		val := &rawQueryMapOutput{k, v}
		values = append(values, val)
	}
	return values
}

type rawQueryMapOutput struct {
	Timestamp int64
	Values    interface{}
}

type rawOutputs []*rawQueryMapOutput

func (a rawOutputs) Len() int           { return len(a) }
func (a rawOutputs) Less(i, j int) bool { return a[i].Timestamp < a[j].Timestamp }
func (a rawOutputs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
