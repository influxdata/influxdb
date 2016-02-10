package influxql

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrUnknownCall is returned when operating on an unknown function call.
var ErrUnknownCall = errors.New("unknown call")

const (
	// MinTime is used as the minimum time value when computing an unbounded range.
	MinTime = int64(0)

	// MaxTime is used as the maximum time value when computing an unbounded range.
	// This time is Jan 1, 2050 at midnight UTC.
	MaxTime = int64(2524608000000000000)
)

// Iterator represents a generic interface for all Iterators.
// Most iterator operations are done on the typed sub-interfaces.
type Iterator interface {
	Close() error
}

// Iterators represents a list of iterators.
type Iterators []Iterator

// Close closes all iterators.
func (a Iterators) Close() error {
	for _, itr := range a {
		itr.Close()
	}
	return nil
}

// filterNonNil returns a slice of iterators that removes all nil iterators.
func (a Iterators) filterNonNil() []Iterator {
	other := make([]Iterator, 0, len(a))
	for _, itr := range a {
		if itr == nil {
			continue
		}
		other = append(other, itr)
	}
	return other
}

// castType determines what type to cast the set of iterators to.
// An iterator type is chosen using this hierarchy:
//   float > integer > string > boolean
func (a Iterators) castType() DataType {
	if len(a) == 0 {
		return Unknown
	}

	typ := DataType(Boolean)
	for _, input := range a {
		switch input.(type) {
		case FloatIterator:
			// Once a float iterator is found, short circuit the end.
			return Float
		case IntegerIterator:
			if typ > Integer {
				typ = Integer
			}
		case StringIterator:
			if typ > String {
				typ = String
			}
		case BooleanIterator:
			// Boolean is the lowest type.
		}
	}
	return typ
}

// cast casts an array of iterators to a single type.
// Iterators that are not compatible or cannot be cast to the
// chosen iterator type are closed and dropped.
func (a Iterators) cast() interface{} {
	typ := a.castType()
	switch typ {
	case Float:
		return newFloatIterators(a)
	case Integer:
		return newIntegerIterators(a)
	case String:
		return newStringIterators(a)
	case Boolean:
		return newBooleanIterators(a)
	}
	return a
}

// NewMergeIterator returns an iterator to merge itrs into one.
// Inputs must either be merge iterators or only contain a single name/tag in
// sorted order. The iterator will output all points by window, name/tag, then
// time. This iterator is useful when you need all of the points for an
// interval.
func NewMergeIterator(inputs []Iterator, opt IteratorOptions) Iterator {
	inputs = Iterators(inputs).filterNonNil()
	if len(inputs) == 0 {
		return &nilFloatIterator{}
	}

	// Aggregate functions can use a more relaxed sorting so that points
	// within a window are grouped. This is much more efficient.
	switch inputs := Iterators(inputs).cast().(type) {
	case []FloatIterator:
		return newFloatMergeIterator(inputs, opt)
	case []IntegerIterator:
		return newIntegerMergeIterator(inputs, opt)
	case []StringIterator:
		return newStringMergeIterator(inputs, opt)
	case []BooleanIterator:
		return newBooleanMergeIterator(inputs, opt)
	default:
		panic(fmt.Sprintf("unsupported merge iterator type: %T", inputs))
	}
}

// NewSortedMergeIterator returns an iterator to merge itrs into one.
// Inputs must either be sorted merge iterators or only contain a single
// name/tag in sorted order. The iterator will output all points by name/tag,
// then time. This iterator is useful when you need all points for a name/tag
// to be in order.
func NewSortedMergeIterator(inputs []Iterator, opt IteratorOptions) Iterator {
	inputs = Iterators(inputs).filterNonNil()
	if len(inputs) == 0 {
		return &nilFloatIterator{}
	}

	switch inputs := Iterators(inputs).cast().(type) {
	case []FloatIterator:
		return newFloatSortedMergeIterator(inputs, opt)
	case []IntegerIterator:
		return newIntegerSortedMergeIterator(inputs, opt)
	case []StringIterator:
		return newStringSortedMergeIterator(inputs, opt)
	case []BooleanIterator:
		return newBooleanSortedMergeIterator(inputs, opt)
	default:
		panic(fmt.Sprintf("unsupported sorted merge iterator type: %T", inputs))
	}
}

// NewLimitIterator returns an iterator that limits the number of points per grouping.
func NewLimitIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatLimitIterator(input, opt)
	case IntegerIterator:
		return newIntegerLimitIterator(input, opt)
	case StringIterator:
		return newStringLimitIterator(input, opt)
	case BooleanIterator:
		return newBooleanLimitIterator(input, opt)
	default:
		panic(fmt.Sprintf("unsupported limit iterator type: %T", input))
	}
}

// NewDedupeIterator returns an iterator that only outputs unique points.
// This iterator maintains a serialized copy of each row so it is inefficient
// to use on large datasets. It is intended for small datasets such as meta queries.
func NewDedupeIterator(input Iterator) Iterator {
	if input == nil {
		return nil
	}

	switch input := input.(type) {
	case FloatIterator:
		return newFloatDedupeIterator(input)
	case IntegerIterator:
		return newIntegerDedupeIterator(input)
	case StringIterator:
		return newStringDedupeIterator(input)
	case BooleanIterator:
		return newBooleanDedupeIterator(input)
	default:
		panic(fmt.Sprintf("unsupported dedupe iterator type: %T", input))
	}
}

// NewFillIterator returns an iterator that fills in missing points in an aggregate.
func NewFillIterator(input Iterator, expr Expr, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatFillIterator(input, expr, opt)
	case IntegerIterator:
		return newIntegerFillIterator(input, expr, opt)
	case StringIterator:
		return newStringFillIterator(input, expr, opt)
	case BooleanIterator:
		return newBooleanFillIterator(input, expr, opt)
	default:
		panic(fmt.Sprintf("unsupported fill iterator type: %T", input))
	}
}

// AuxIterator represents an iterator that can split off separate auxilary iterators.
type AuxIterator interface {
	Iterator
	IteratorCreator

	// Auxilary iterator
	Iterator(name string) Iterator

	// Start starts writing to the created iterators.
	Start()
}

// NewAuxIterator returns a new instance of AuxIterator.
func NewAuxIterator(input Iterator, seriesKeys SeriesList, opt IteratorOptions) AuxIterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatAuxIterator(input, seriesKeys, opt)
	case IntegerIterator:
		return newIntegerAuxIterator(input, seriesKeys, opt)
	case StringIterator:
		return newStringAuxIterator(input, seriesKeys, opt)
	case BooleanIterator:
		return newBooleanAuxIterator(input, seriesKeys, opt)
	default:
		panic(fmt.Sprintf("unsupported aux iterator type: %T", input))
	}
}

// auxIteratorField represents an auxilary field within an AuxIterator.
type auxIteratorField struct {
	name string     // field name
	typ  DataType   // detected data type
	itrs []Iterator // auxillary iterators
	mu   sync.Mutex
	opt  IteratorOptions
}

func (f *auxIteratorField) append(itr Iterator) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.itrs = append(f.itrs, itr)
}

func (f *auxIteratorField) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, itr := range f.itrs {
		itr.Close()
	}
}

type auxIteratorFields []*auxIteratorField

// newAuxIteratorFields returns a new instance of auxIteratorFields from a list of field names.
func newAuxIteratorFields(seriesKeys SeriesList, opt IteratorOptions) auxIteratorFields {
	fields := make(auxIteratorFields, len(opt.Aux))
	for i, name := range opt.Aux {
		fields[i] = &auxIteratorField{name: name, opt: opt}
		for _, s := range seriesKeys {
			aux := s.Aux[i]
			if aux == Unknown {
				continue
			}

			if fields[i].typ == Unknown || aux < fields[i].typ {
				fields[i].typ = aux
			}
		}
	}
	return fields
}

func (a auxIteratorFields) close() {
	for _, f := range a {
		f.close()
	}
}

// iterator creates a new iterator for a named auxilary field.
func (a auxIteratorFields) iterator(name string) Iterator {
	for _, f := range a {
		// Skip field if it's name doesn't match.
		// Exit if no points were received by the iterator.
		if f.name != name {
			continue
		}

		// Create channel iterator by data type.
		switch f.typ {
		case Float:
			itr := &floatChanIterator{c: make(chan *FloatPoint, 1)}
			f.append(itr)
			return itr
		case Integer:
			itr := &integerChanIterator{c: make(chan *IntegerPoint, 1)}
			f.append(itr)
			return itr
		case String:
			itr := &stringChanIterator{c: make(chan *StringPoint, 1)}
			f.append(itr)
			return itr
		case Boolean:
			itr := &booleanChanIterator{c: make(chan *BooleanPoint, 1)}
			f.append(itr)
			return itr
		default:
			break
		}
	}

	return &nilFloatIterator{}
}

// send sends a point to all field iterators.
func (a auxIteratorFields) send(p Point) {
	values := p.aux()
	for i, f := range a {
		v := values[i]

		tags := p.tags()
		tags = tags.Subset(f.opt.Dimensions)

		// Send new point for each aux iterator.
		// Primitive pointers represent nil values.
		for _, itr := range f.itrs {
			switch itr := itr.(type) {
			case *floatChanIterator:
				switch v := v.(type) {
				case float64:
					itr.c <- &FloatPoint{Name: p.name(), Tags: tags, Time: p.time(), Value: v}
				case int64:
					itr.c <- &FloatPoint{Name: p.name(), Tags: tags, Time: p.time(), Value: float64(v)}
				default:
					itr.c <- &FloatPoint{Name: p.name(), Tags: tags, Time: p.time(), Nil: true}
				}
			case *integerChanIterator:
				switch v := v.(type) {
				case int64:
					itr.c <- &IntegerPoint{Name: p.name(), Tags: tags, Time: p.time(), Value: v}
				default:
					itr.c <- &IntegerPoint{Name: p.name(), Tags: tags, Time: p.time(), Nil: true}
				}
			case *stringChanIterator:
				switch v := v.(type) {
				case string:
					itr.c <- &StringPoint{Name: p.name(), Tags: tags, Time: p.time(), Value: v}
				default:
					itr.c <- &StringPoint{Name: p.name(), Tags: tags, Time: p.time(), Nil: true}
				}
			case *booleanChanIterator:
				switch v := v.(type) {
				case bool:
					itr.c <- &BooleanPoint{Name: p.name(), Tags: tags, Time: p.time(), Value: v}
				default:
					itr.c <- &BooleanPoint{Name: p.name(), Tags: tags, Time: p.time(), Nil: true}
				}
			default:
				panic(fmt.Sprintf("invalid aux itr type: %T", itr))
			}
		}
	}
}

// drainIterator reads all points from an iterator.
func drainIterator(itr Iterator) {
	for {
		switch itr := itr.(type) {
		case FloatIterator:
			if p := itr.Next(); p == nil {
				return
			}
		case IntegerIterator:
			if p := itr.Next(); p == nil {
				return
			}
		case StringIterator:
			if p := itr.Next(); p == nil {
				return
			}
		case BooleanIterator:
			if p := itr.Next(); p == nil {
				return
			}
		default:
			panic(fmt.Sprintf("unsupported iterator type for draining: %T", itr))
		}
	}
}

// IteratorCreator represents an interface for objects that can create Iterators.
type IteratorCreator interface {
	// Creates a simple iterator for use in an InfluxQL query.
	CreateIterator(opt IteratorOptions) (Iterator, error)

	// Returns the unique fields and dimensions across a list of sources.
	FieldDimensions(sources Sources) (fields, dimensions map[string]struct{}, err error)

	// Returns the series keys that will be returned by this iterator.
	SeriesKeys(opt IteratorOptions) (SeriesList, error)
}

// IteratorOptions is an object passed to CreateIterator to specify creation options.
type IteratorOptions struct {
	// Expression to iterate for.
	// This can be VarRef or a Call.
	Expr Expr

	// Auxilary tags or values to also retrieve for the point.
	Aux []string

	// Data sources from which to retrieve data.
	Sources []Source

	// Group by interval and tags.
	Interval   Interval
	Dimensions []string

	// Fill options.
	Fill      FillOption
	FillValue interface{}

	// Condition to filter by.
	Condition Expr

	// Time range for the iterator.
	StartTime int64
	EndTime   int64

	// Sorted in time ascending order if true.
	Ascending bool

	// Limits the number of points per series.
	Limit, Offset int

	// Limits the number of series.
	SLimit, SOffset int

	// Removes duplicate rows from raw queries.
	Dedupe bool
}

// newIteratorOptionsStmt creates the iterator options from stmt.
func newIteratorOptionsStmt(stmt *SelectStatement) (opt IteratorOptions, err error) {
	// Determine time range from the condition.
	startTime, endTime := TimeRange(stmt.Condition)
	if !startTime.IsZero() {
		opt.StartTime = startTime.UnixNano()
	} else {
		opt.StartTime = MinTime
	}
	if !endTime.IsZero() {
		opt.EndTime = endTime.UnixNano()
	} else {
		opt.EndTime = MaxTime
	}

	// Determine group by interval.
	interval, err := stmt.GroupByInterval()
	if err != nil {
		return opt, err
	}
	// Set duration to zero if a negative interval has been used.
	if interval < 0 {
		interval = 0
	}
	opt.Interval.Duration = interval

	// Determine dimensions.
	for _, d := range stmt.Dimensions {
		if d, ok := d.Expr.(*VarRef); ok {
			opt.Dimensions = append(opt.Dimensions, d.Val)
		}
	}

	opt.Sources = stmt.Sources
	opt.Condition = stmt.Condition
	opt.Ascending = stmt.TimeAscending()
	opt.Dedupe = stmt.Dedupe

	opt.Fill, opt.FillValue = stmt.Fill, stmt.FillValue
	opt.Limit, opt.Offset = stmt.Limit, stmt.Offset
	opt.SLimit, opt.SOffset = stmt.SLimit, stmt.SOffset

	return opt, nil
}

// MergeSorted returns true if the options require a sorted merge.
// This is only needed when the expression is a variable reference or there is no expr.
func (opt IteratorOptions) MergeSorted() bool {
	if opt.Expr == nil {
		return true
	}
	_, ok := opt.Expr.(*VarRef)
	return ok
}

// SeekTime returns the time the iterator should start from.
// For ascending iterators this is the start time, for descending iterators it's the end time.
func (opt IteratorOptions) SeekTime() int64 {
	if opt.Ascending {
		return opt.StartTime
	}
	return opt.EndTime
}

// Window returns the time window [start,end) that t falls within.
func (opt IteratorOptions) Window(t int64) (start, end int64) {
	if opt.Interval.IsZero() {
		return opt.StartTime, opt.EndTime
	}

	// Subtract the offset to the time so we calculate the correct base interval.
	t -= int64(opt.Interval.Offset)

	// Truncate time by duration.
	t -= t % int64(opt.Interval.Duration)

	// Apply the offset.
	start = t + int64(opt.Interval.Offset)
	end = start + int64(opt.Interval.Duration)
	return
}

// DerivativeInterval returns the time interval for the derivative function.
func (opt IteratorOptions) DerivativeInterval() Interval {
	// Use the interval on the derivative() call, if specified.
	if expr, ok := opt.Expr.(*Call); ok && len(expr.Args) == 2 {
		return Interval{Duration: expr.Args[1].(*DurationLiteral).Val}
	}

	// Otherwise use the group by interval, if specified.
	if opt.Interval.Duration > 0 {
		return Interval{Duration: opt.Interval.Duration}
	}

	return Interval{Duration: time.Second}
}

// selectInfo represents an object that stores info about select fields.
type selectInfo struct {
	calls map[*Call]struct{}
	refs  map[*VarRef]struct{}
}

// newSelectInfo creates a object with call and var ref info from stmt.
func newSelectInfo(stmt *SelectStatement) *selectInfo {
	info := &selectInfo{
		calls: make(map[*Call]struct{}),
		refs:  make(map[*VarRef]struct{}),
	}
	Walk(info, stmt.Fields)
	return info
}

func (v *selectInfo) Visit(n Node) Visitor {
	switch n := n.(type) {
	case *Call:
		v.calls[n] = struct{}{}
		return nil
	case *VarRef:
		v.refs[n] = struct{}{}
		return nil
	}
	return v
}

// Series represents a series that will be returned by the iterator.
type Series struct {
	Name string
	Tags Tags
	Aux  []DataType
}

// ID is a single string that combines the name and tags id for the series.
func (s *Series) ID() string {
	return s.Name + "\x00" + s.Tags.ID()
}

// Combine combines two series with the same name and tags.
// It will promote auxiliary iterator types to the highest type.
func (s *Series) Combine(other *Series) {
	for i, t := range s.Aux {
		if other.Aux[i] == Unknown {
			continue
		}

		if t == Unknown || other.Aux[i] < t {
			s.Aux[i] = other.Aux[i]
		}
	}
}

// SeriesList is a list of series that will be returned by an iterator.
type SeriesList []Series

func (a SeriesList) Len() int      { return len(a) }
func (a SeriesList) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a SeriesList) Less(i, j int) bool {
	if a[i].Name != a[j].Name {
		return a[i].Name < a[j].Name
	}
	return a[i].Tags.ID() < a[j].Tags.ID()
}

// Interval represents a repeating interval for a query.
type Interval struct {
	Duration time.Duration
	Offset   time.Duration
}

// IsZero returns true if the interval has no duration.
func (i Interval) IsZero() bool { return i.Duration == 0 }

// reduceOptions represents options for performing reductions on windows of points.
type reduceOptions struct {
	startTime int64
	endTime   int64
}

type nilFloatIterator struct{}

func (*nilFloatIterator) Close() error      { return nil }
func (*nilFloatIterator) Next() *FloatPoint { return nil }

type integerFloatCastIterator struct {
	input IntegerIterator
}

func (itr *integerFloatCastIterator) Close() error { return itr.input.Close() }
func (itr *integerFloatCastIterator) Next() *FloatPoint {
	p := itr.input.Next()
	if p == nil {
		return nil
	}

	return &FloatPoint{
		Name:  p.Name,
		Tags:  p.Tags,
		Time:  p.Time,
		Nil:   p.Nil,
		Value: float64(p.Value),
		Aux:   p.Aux,
	}
}
