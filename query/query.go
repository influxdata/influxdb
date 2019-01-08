package query

import (
	"errors"
	"math"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

var (
	// ErrQueryInterrupted is an error returned when the query is interrupted.
	ErrQueryInterrupted = errors.New("query interrupted")
)

// ZeroTime is the Unix nanosecond timestamp for no time.
// This time is not used by the query engine or the storage engine as a valid time.
const ZeroTime = int64(math.MinInt64)

// IteratorOptions is an object passed to CreateIterator to specify creation options.
type IteratorOptions struct {
	// Expression to iterate for.
	// This can be VarRef or a Call.
	Expr influxql.Expr

	// Auxilary tags or values to also retrieve for the point.
	Aux []influxql.VarRef

	// Data sources from which to receive data. This is only used for encoding
	// measurements over RPC and is no longer used in the open source version.
	Sources []influxql.Source

	// Group by interval and tags.
	Interval   Interval
	Dimensions []string            // The final dimensions of the query (stays the same even in subqueries).
	GroupBy    map[string]struct{} // Dimensions to group points by in intermediate iterators.
	Location   *time.Location

	// Fill options.
	Fill      influxql.FillOption
	FillValue interface{}

	// Condition to filter by.
	Condition influxql.Expr

	// Time range for the iterator.
	StartTime int64
	EndTime   int64

	// Sorted in time ascending order if true.
	Ascending bool

	// Limits the number of points per series.
	Limit, Offset int

	// Limits the number of series.
	SLimit, SOffset int

	// Removes the measurement name. Useful for meta queries.
	StripName bool

	// Removes duplicate rows from raw queries.
	Dedupe bool

	// Determines if this is a query for raw data or an aggregate/selector.
	Ordered bool

	// Limits on the creation of iterators.
	MaxSeriesN int

	// If this channel is set and is closed, the iterator should try to exit
	// and close as soon as possible.
	InterruptCh <-chan struct{}

	// Authorizer can limit access to data
	Authorizer Authorizer
}

// SeekTime returns the time the iterator should start from.
// For ascending iterators this is the start time, for descending iterators it's the end time.
func (opt IteratorOptions) SeekTime() int64 {
	if opt.Ascending {
		return opt.StartTime
	}
	return opt.EndTime
}

// StopTime returns the time the iterator should end at.
// For ascending iterators this is the end time, for descending iterators it's the start time.
func (opt IteratorOptions) StopTime() int64 {
	if opt.Ascending {
		return opt.EndTime
	}
	return opt.StartTime
}

// Interval represents a repeating interval for a query.
type Interval struct {
	Duration time.Duration
	Offset   time.Duration
}

// Authorizer determines if certain operations are authorized.
type Authorizer interface {
	// AuthorizeDatabase indicates whether the given Privilege is authorized on the database with the given name.
	AuthorizeDatabase(p influxql.Privilege, name string) bool

	// AuthorizeQuery returns an error if the query cannot be executed
	AuthorizeQuery(database string, query *influxql.Query) error

	// AuthorizeSeriesRead determines if a series is authorized for reading
	AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool

	// AuthorizeSeriesWrite determines if a series is authorized for writing
	AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool
}

// FloatPoint represents a point with a float64 value.
// DO NOT ADD ADDITIONAL FIELDS TO THIS STRUCT.
// See TestPoint_Fields in influxql/point_test.go for more details.
type FloatPoint struct {
	Name string
	Tags Tags

	Time  int64
	Value float64
	Aux   []interface{}

	// Total number of points that were combined into this point from an aggregate.
	// If this is zero, the point is not the result of an aggregate function.
	Aggregated uint32
	Nil        bool
}

// Tags represent a map of keys and values.
// It memoizes its key so it can be used efficiently during query execution.
type Tags struct{}

// Iterator represents a generic interface for all Iterators.
// Most iterator operations are done on the typed sub-interfaces.
type Iterator interface {
	Stats() IteratorStats
	Close() error
}

// IteratorStats represents statistics about an iterator.
// Some statistics are available immediately upon iterator creation while
// some are derived as the iterator processes data.
type IteratorStats struct {
	SeriesN int // series represented
	PointN  int // points returned
}

// TagSet is a fundamental concept within the query system. It represents a composite series,
// composed of multiple individual series that share a set of tag attributes.
type TagSet struct {
	Tags       map[string]string
	Filters    []influxql.Expr
	SeriesKeys []string
	Key        []byte
}

// AddFilter adds a series-level filter to the Tagset.
func (t *TagSet) AddFilter(key string, filter influxql.Expr) {
	t.SeriesKeys = append(t.SeriesKeys, key)
	t.Filters = append(t.Filters, filter)
}

func (t *TagSet) Len() int           { return len(t.SeriesKeys) }
func (t *TagSet) Less(i, j int) bool { return t.SeriesKeys[i] < t.SeriesKeys[j] }
func (t *TagSet) Swap(i, j int) {
	t.SeriesKeys[i], t.SeriesKeys[j] = t.SeriesKeys[j], t.SeriesKeys[i]
	t.Filters[i], t.Filters[j] = t.Filters[j], t.Filters[i]
}

// Reverse reverses the order of series keys and filters in the TagSet.
func (t *TagSet) Reverse() {
	for i, j := 0, len(t.Filters)-1; i < j; i, j = i+1, j-1 {
		t.Filters[i], t.Filters[j] = t.Filters[j], t.Filters[i]
		t.SeriesKeys[i], t.SeriesKeys[j] = t.SeriesKeys[j], t.SeriesKeys[i]
	}
}

// IteratorCost contains statistics retrieved for explaining what potential
// cost may be incurred by instantiating an iterator.
type IteratorCost struct {
	// The total number of shards that are touched by this query.
	NumShards int64

	// The total number of non-unique series that are accessed by this query.
	// This number matches the number of cursors created by the query since
	// one cursor is created for every series.
	NumSeries int64

	// CachedValues returns the number of cached values that may be read by this
	// query.
	CachedValues int64

	// The total number of non-unique files that may be accessed by this query.
	// This will count the number of files accessed by each series so files
	// will likely be double counted.
	NumFiles int64

	// The number of blocks that had the potential to be accessed.
	BlocksRead int64

	// The amount of data that can be potentially read.
	BlockSize int64
}

// Combine combines the results of two IteratorCost structures into one.
func (c IteratorCost) Combine(other IteratorCost) IteratorCost {
	return IteratorCost{
		NumShards:    c.NumShards + other.NumShards,
		NumSeries:    c.NumSeries + other.NumSeries,
		CachedValues: c.CachedValues + other.CachedValues,
		NumFiles:     c.NumFiles + other.NumFiles,
		BlocksRead:   c.BlocksRead + other.BlocksRead,
		BlockSize:    c.BlockSize + other.BlockSize,
	}
}

// FloatIterator represents a stream of float points.
type FloatIterator interface {
	Iterator
	Next() (*FloatPoint, error)
}

// IntegerIterator represents a stream of integer points.
type IntegerIterator interface {
	Iterator
	Next() (*IntegerPoint, error)
}

// IntegerPoint represents a point with a int64 value.
// DO NOT ADD ADDITIONAL FIELDS TO THIS STRUCT.
// See TestPoint_Fields in influxql/point_test.go for more details.
type IntegerPoint struct {
	Name string
	Tags Tags

	Time  int64
	Value int64
	Aux   []interface{}

	// Total number of points that were combined into this point from an aggregate.
	// If this is zero, the point is not the result of an aggregate function.
	Aggregated uint32
	Nil        bool
}

// UnsignedIterator represents a stream of unsigned points.
type UnsignedIterator interface {
	Iterator
	Next() (*UnsignedPoint, error)
}

// UnsignedPoint represents a point with a uint64 value.
// DO NOT ADD ADDITIONAL FIELDS TO THIS STRUCT.
// See TestPoint_Fields in influxql/point_test.go for more details.
type UnsignedPoint struct {
	Name string
	Tags Tags

	Time  int64
	Value uint64
	Aux   []interface{}

	// Total number of points that were combined into this point from an aggregate.
	// If this is zero, the point is not the result of an aggregate function.
	Aggregated uint32
	Nil        bool
}

// StringIterator represents a stream of string points.
type StringIterator interface {
	Iterator
	Next() (*StringPoint, error)
}

// StringPoint represents a point with a string value.
// DO NOT ADD ADDITIONAL FIELDS TO THIS STRUCT.
// See TestPoint_Fields in influxql/point_test.go for more details.
type StringPoint struct {
	Name string
	Tags Tags

	Time  int64
	Value string
	Aux   []interface{}

	// Total number of points that were combined into this point from an aggregate.
	// If this is zero, the point is not the result of an aggregate function.
	Aggregated uint32
	Nil        bool
}

// BooleanIterator represents a stream of boolean points.
type BooleanIterator interface {
	Iterator
	Next() (*BooleanPoint, error)
}

// BooleanPoint represents a point with a bool value.
// DO NOT ADD ADDITIONAL FIELDS TO THIS STRUCT.
// See TestPoint_Fields in influxql/point_test.go for more details.
type BooleanPoint struct {
	Name string
	Tags Tags

	Time  int64
	Value bool
	Aux   []interface{}

	// Total number of points that were combined into this point from an aggregate.
	// If this is zero, the point is not the result of an aggregate function.
	Aggregated uint32
	Nil        bool
}

type MathValuer struct{}

var _ influxql.CallValuer = MathValuer{}

func (MathValuer) Value(key string) (interface{}, bool) {
	return nil, false
}

func (v MathValuer) Call(name string, args []interface{}) (interface{}, bool) {
	if len(args) == 1 {
		arg0 := args[0]
		switch name {
		case "abs":
			switch arg0 := arg0.(type) {
			case float64:
				return math.Abs(arg0), true
			case int64, uint64:
				return arg0, true
			default:
				return nil, true
			}
		case "sin":
			if arg0, ok := asFloat(arg0); ok {
				return math.Sin(arg0), true
			}
			return nil, true
		case "cos":
			if arg0, ok := asFloat(arg0); ok {
				return math.Cos(arg0), true
			}
			return nil, true
		case "tan":
			if arg0, ok := asFloat(arg0); ok {
				return math.Tan(arg0), true
			}
			return nil, true
		case "floor":
			switch arg0 := arg0.(type) {
			case float64:
				return math.Floor(arg0), true
			case int64, uint64:
				return arg0, true
			default:
				return nil, true
			}
		case "ceil":
			switch arg0 := arg0.(type) {
			case float64:
				return math.Ceil(arg0), true
			case int64, uint64:
				return arg0, true
			default:
				return nil, true
			}
		case "round":
			switch arg0 := arg0.(type) {
			case float64:
				return round(arg0), true
			case int64, uint64:
				return arg0, true
			default:
				return nil, true
			}
		case "asin":
			if arg0, ok := asFloat(arg0); ok {
				return math.Asin(arg0), true
			}
			return nil, true
		case "acos":
			if arg0, ok := asFloat(arg0); ok {
				return math.Acos(arg0), true
			}
			return nil, true
		case "atan":
			if arg0, ok := asFloat(arg0); ok {
				return math.Atan(arg0), true
			}
			return nil, true
		case "exp":
			if arg0, ok := asFloat(arg0); ok {
				return math.Exp(arg0), true
			}
			return nil, true
		case "ln":
			if arg0, ok := asFloat(arg0); ok {
				return math.Log(arg0), true
			}
			return nil, true
		case "log2":
			if arg0, ok := asFloat(arg0); ok {
				return math.Log2(arg0), true
			}
			return nil, true
		case "log10":
			if arg0, ok := asFloat(arg0); ok {
				return math.Log10(arg0), true
			}
			return nil, true
		case "sqrt":
			if arg0, ok := asFloat(arg0); ok {
				return math.Sqrt(arg0), true
			}
			return nil, true
		}
	} else if len(args) == 2 {
		arg0, arg1 := args[0], args[1]
		switch name {
		case "atan2":
			if arg0, arg1, ok := asFloats(arg0, arg1); ok {
				return math.Atan2(arg0, arg1), true
			}
			return nil, true
		case "log":
			if arg0, arg1, ok := asFloats(arg0, arg1); ok {
				return math.Log(arg0) / math.Log(arg1), true
			}
			return nil, true
		case "pow":
			if arg0, arg1, ok := asFloats(arg0, arg1); ok {
				return math.Pow(arg0, arg1), true
			}
			return nil, true
		}
	}
	return nil, false
}

func asFloat(x interface{}) (float64, bool) {
	switch arg0 := x.(type) {
	case float64:
		return arg0, true
	case int64:
		return float64(arg0), true
	case uint64:
		return float64(arg0), true
	default:
		return 0, false
	}
}

func asFloats(x, y interface{}) (float64, float64, bool) {
	arg0, ok := asFloat(x)
	if !ok {
		return 0, 0, false
	}
	arg1, ok := asFloat(y)
	if !ok {
		return 0, 0, false
	}
	return arg0, arg1, true
}

func round(x float64) float64 {
	t := math.Trunc(x)
	if math.Abs(x-t) >= 0.5 {
		return t + math.Copysign(1, x)
	}
	return t
}

// OpenAuthorizer is the Authorizer used when authorization is disabled.
// It allows all operations.
type openAuthorizer struct{}

// OpenAuthorizer can be shared by all goroutines.
var OpenAuthorizer = openAuthorizer{}

// AuthorizeDatabase returns true to allow any operation on a database.
func (a openAuthorizer) AuthorizeDatabase(influxql.Privilege, string) bool { return true }

// AuthorizeSeriesRead allows accesss to any series.
func (a openAuthorizer) AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool {
	return true
}

// AuthorizeSeriesWrite allows accesss to any series.
func (a openAuthorizer) AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool {
	return true
}

// AuthorizeSeriesRead allows any query to execute.
func (a openAuthorizer) AuthorizeQuery(_ string, _ *influxql.Query) error { return nil }
