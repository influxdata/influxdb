package influxql

import (
	"errors"
	"fmt"
	"math"
	"time"
)

//go:generate tmpl -data=[{"Name":"Float","name":"float","Type":"float64","Nil":"math.NaN()"},{"Name":"String","name":"string","Type":"string","Nil":"\"\""},{"Name":"Boolean","name":"boolean","Type":"bool","Nil":"false"}] iterator.gen.go.tmpl

// ErrUnknownCall is returned when operating on an unknown function call.
var ErrUnknownCall = errors.New("unknown call")

const (
	// MinTime is used as the minimum time value when computing an unbounded range.
	MinTime = int64(0)

	// MaxTime is used as the maximum time value when computing an unbounded range.
	MaxTime = int64(math.MaxInt64)
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

// NewMergeIterator returns an iterator to merge itrs into one.
func NewMergeIterator(inputs []Iterator, opt IteratorOptions) Iterator {
	inputs = Iterators(inputs).filterNonNil()
	if len(inputs) == 0 {
		return &nilFloatIterator{}
	}

	// If the expression is nil or a variable reference then do a full sort.
	if _, ok := opt.Expr.(*VarRef); opt.Expr == nil || ok {
		switch input := inputs[0].(type) {
		case FloatIterator:
			return newFloatSortedMergeIterator(newFloatIterators(inputs), opt)
		default:
			panic(fmt.Sprintf("unsupported sorted merge iterator type: %T", input))
		}
	}

	// Aggregate functions can use a more relaxed sorting so that points
	// within a window are grouped. This is much more efficient.
	switch input := inputs[0].(type) {
	case FloatIterator:
		return newFloatMergeIterator(newFloatIterators(inputs), opt)
	default:
		panic(fmt.Sprintf("unsupported merge iterator type: %T", input))
	}
}

// NewLimitIterator returns an iterator that limits the number of points per grouping.
func NewLimitIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatLimitIterator(input, opt)
	default:
		panic(fmt.Sprintf("unsupported limit iterator type: %T", input))
	}
}

// Join combines inputs based on timestamp and returns new iterators.
// The output iterators guarantee that one value will be output for every timestamp.
func Join(inputs []Iterator) (outputs []Iterator) {
	if len(inputs) == 0 {
		return inputs
	}

	itrs := make([]joinIterator, len(inputs))
	for i, input := range inputs {
		switch input := input.(type) {
		case FloatIterator:
			itrs[i] = newFloatJoinIterator(input)
		case StringIterator:
			itrs[i] = newStringJoinIterator(input)
		case BooleanIterator:
			itrs[i] = newBooleanJoinIterator(input)
		default:
			panic(fmt.Sprintf("unsupported join iterator type: %T", input))
		}
	}

	// Begin joining goroutine.
	go join(itrs)

	return joinIterators(itrs).iterators()
}

// join runs in a separate goroutine to join input values on timestamp.
func join(itrs []joinIterator) {
	for {
		// Find min timestamp and associated name & tags.
		var name string
		var tags Tags
		min := ZeroTime
		for _, itr := range itrs {
			bufTime, bufName, bufTags := itr.loadBuf()
			if bufTime != ZeroTime && (min == ZeroTime || bufTime < min) {
				min, name, tags = bufTime, bufName, bufTags
			}
		}

		// Exit when no more values are available.
		if min == ZeroTime {
			break
		}

		// Emit value on every output.
		for _, itr := range itrs {
			itr.emitAt(min, name, tags)
		}
	}

	// Close all iterators.
	for _, itr := range itrs {
		itr.Close()
	}
}

// joinIterator represents output iterator used by join().
type joinIterator interface {
	Iterator
	loadBuf() (t int64, name string, tags Tags)
	emitAt(t int64, name string, tags Tags)
}

type joinIterators []joinIterator

// iterators returns itrs as a list of generic iterators.
func (itrs joinIterators) iterators() []Iterator {
	a := make([]Iterator, len(itrs))
	for i, itr := range itrs {
		a[i] = itr
	}
	return a
}

// AuxIterator represents an iterator that can split off separate auxilary iterators.
type AuxIterator interface {
	Iterator

	// Auxilary iterator
	Iterator(name string) Iterator
}

// NewAuxIterator returns a new instance of AuxIterator.
func NewAuxIterator(input Iterator, opt IteratorOptions) AuxIterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatAuxIterator(input, opt)
	case StringIterator:
		return newStringAuxIterator(input, opt)
	case BooleanIterator:
		return newBooleanAuxIterator(input, opt)
	default:
		panic(fmt.Sprintf("unsupported aux iterator type: %T", input))
	}
}

// auxIteratorField represents an auxilary field within an AuxIterator.
type auxIteratorField struct {
	name    string     // field name
	typ     DataType   // detected data type
	initial Point      // first point
	itrs    []Iterator // auxillary iterators
	opt     IteratorOptions
}

type auxIteratorFields []*auxIteratorField

// newAuxIteratorFields returns a new instance of auxIteratorFields from a list of field names.
func newAuxIteratorFields(opt IteratorOptions) auxIteratorFields {
	fields := make(auxIteratorFields, len(opt.Aux))
	for i, name := range opt.Aux {
		fields[i] = &auxIteratorField{name: name, opt: opt}
	}
	return fields
}

func (a auxIteratorFields) close() {
	for _, f := range a {
		for _, itr := range f.itrs {
			itr.Close()
		}
	}
}

// init initializes all auxilary fields with initial points.
func (a auxIteratorFields) init(p Point) {
	values := p.aux()
	for i, f := range a {
		v := values[i]

		tags := p.tags()
		tags = tags.Subset(f.opt.Dimensions)

		switch v := v.(type) {
		case float64:
			f.typ = Float
			f.initial = &FloatPoint{
				Name:  p.name(),
				Tags:  tags,
				Time:  p.time(),
				Value: v,
			}
		case string:
			f.typ = String
			f.initial = &StringPoint{
				Name:  p.name(),
				Tags:  tags,
				Time:  p.time(),
				Value: v,
			}
		case bool:
			f.typ = Boolean
			f.initial = &BooleanPoint{
				Name:  p.name(),
				Tags:  tags,
				Time:  p.time(),
				Value: v,
			}
		default:
			panic(fmt.Sprintf("invalid aux value type: %T", v))
		}
	}
}

// iterator creates a new iterator for a named auxilary field.
func (a auxIteratorFields) iterator(name string) Iterator {
	for _, f := range a {
		// Skip field if it's name doesn't match.
		// Exit if no points were received by the iterator.
		if f.name != name {
			continue
		} else if f.initial == nil {
			break
		}

		// Create channel iterator by data type.
		switch f.typ {
		case Float:
			itr := &floatChanIterator{c: make(chan *FloatPoint, 1)}
			itr.c <- f.initial.(*FloatPoint)
			f.itrs = append(f.itrs, itr)
			return itr
		case String:
			itr := &stringChanIterator{c: make(chan *StringPoint, 1)}
			itr.c <- f.initial.(*StringPoint)
			f.itrs = append(f.itrs, itr)
			return itr
		case Boolean:
			itr := &booleanChanIterator{c: make(chan *BooleanPoint, 1)}
			itr.c <- f.initial.(*BooleanPoint)
			f.itrs = append(f.itrs, itr)
			return itr
		default:
			panic(fmt.Sprintf("unsupported chan iterator type: %s", f.typ))
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
		for _, itr := range f.itrs {
			switch itr := itr.(type) {
			case *floatChanIterator:
				v, _ := v.(float64)
				itr.c <- &FloatPoint{
					Name:  p.name(),
					Tags:  tags,
					Time:  p.time(),
					Value: v,
				}
			case *stringChanIterator:
				v, _ := v.(string)
				itr.c <- &StringPoint{
					Name:  p.name(),
					Tags:  tags,
					Time:  p.time(),
					Value: v,
				}
			case *booleanChanIterator:
				v, _ := v.(bool)
				itr.c <- &BooleanPoint{
					Name:  p.name(),
					Tags:  tags,
					Time:  p.time(),
					Value: v,
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
		case StringIterator:
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
}

// newIteratorOptionsStmt creates the iterator options from stmt.
func newIteratorOptionsStmt(stmt *SelectStatement) (opt IteratorOptions, err error) {
	// Determine group by interval.
	interval, err := stmt.GroupByInterval()
	if err != nil {
		return opt, err
	}
	opt.Interval.Duration = interval

	// Determine dimensions.
	for _, d := range stmt.Dimensions {
		if d, ok := d.Expr.(*VarRef); ok {
			opt.Dimensions = append(opt.Dimensions, d.Val)
		}
	}

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

	opt.Sources = stmt.Sources
	opt.Condition = stmt.Condition
	opt.Ascending = stmt.TimeAscending()

	opt.Limit, opt.Offset = stmt.Limit, stmt.Offset
	opt.SLimit, opt.SOffset = stmt.SLimit, stmt.SOffset

	return opt, nil
}

// Window returns the time window [start,end) that t falls within.
func (opt IteratorOptions) Window(t int64) (start, end int64) {
	if opt.Interval.IsZero() {
		return opt.StartTime, opt.EndTime
	}

	// Truncate time by duration.
	t -= t % int64(opt.Interval.Duration)

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
		return opt.Interval
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

// ErrNotImplemented is returned by IteratorCreator implementations when the
// requested expression cannot be implemented as an iterator.
var ErrNotImplemented = errors.New("not implemented")

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
