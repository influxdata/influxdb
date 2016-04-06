package influxql

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/influxql/internal"
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
	Stats() IteratorStats
	Close() error
}

// Iterators represents a list of iterators.
type Iterators []Iterator

// Stats returns the aggregation of all iterator stats.
func (a Iterators) Stats() IteratorStats {
	var stats IteratorStats
	for _, itr := range a {
		stats.Add(itr.Stats())
	}
	return stats
}

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

// NewIntervalIterator returns an iterator that sets the time on each point to the interval.
func NewIntervalIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatIntervalIterator(input, opt)
	case IntegerIterator:
		return newIntegerIntervalIterator(input, opt)
	case StringIterator:
		return newStringIntervalIterator(input, opt)
	case BooleanIterator:
		return newBooleanIntervalIterator(input, opt)
	default:
		panic(fmt.Sprintf("unsupported fill iterator type: %T", input))
	}
}

// NewInterruptIterator returns an iterator that will stop producing output when a channel
// has been closed on the passed in channel.
func NewInterruptIterator(input Iterator, closing <-chan struct{}) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatInterruptIterator(input, closing)
	case IntegerIterator:
		return newIntegerInterruptIterator(input, closing)
	case StringIterator:
		return newStringInterruptIterator(input, closing)
	case BooleanIterator:
		return newBooleanInterruptIterator(input, closing)
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

	// Backgrounds the iterator so that, when start is called, it will
	// continuously read from the iterator.
	Background()
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
			itr := &floatChanIterator{cond: sync.NewCond(&sync.Mutex{})}
			f.append(itr)
			return itr
		case Integer:
			itr := &integerChanIterator{cond: sync.NewCond(&sync.Mutex{})}
			f.append(itr)
			return itr
		case String:
			itr := &stringChanIterator{cond: sync.NewCond(&sync.Mutex{})}
			f.append(itr)
			return itr
		case Boolean:
			itr := &booleanChanIterator{cond: sync.NewCond(&sync.Mutex{})}
			f.append(itr)
			return itr
		default:
			break
		}
	}

	return &nilFloatIterator{}
}

// send sends a point to all field iterators.
func (a auxIteratorFields) send(p Point) (ok bool) {
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
				ok = itr.setBuf(p.name(), tags, p.time(), v) || ok
			case *integerChanIterator:
				ok = itr.setBuf(p.name(), tags, p.time(), v) || ok
			case *stringChanIterator:
				ok = itr.setBuf(p.name(), tags, p.time(), v) || ok
			case *booleanChanIterator:
				ok = itr.setBuf(p.name(), tags, p.time(), v) || ok
			default:
				panic(fmt.Sprintf("invalid aux itr type: %T", itr))
			}
		}
	}
	return ok
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

// NewReaderIterator returns an iterator that streams from a reader.
func NewReaderIterator(r io.Reader) (Iterator, error) {
	var p Point
	dec := NewPointDecoder(r)
	if err := dec.DecodePoint(&p); err == io.EOF {
		return &nilFloatIterator{}, nil
	} else if err != nil {
		return nil, err
	}

	switch p := p.(type) {
	case *FloatPoint:
		return newFloatReaderIterator(r, p, dec.Stats()), nil
	case *IntegerPoint:
		return newIntegerReaderIterator(r, p, dec.Stats()), nil
	case *StringPoint:
		return newStringReaderIterator(r, p, dec.Stats()), nil
	case *BooleanPoint:
		return newBooleanReaderIterator(r, p, dec.Stats()), nil
	default:
		panic(fmt.Sprintf("unsupported point for reader iterator: %T", p))
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

	// Expands regex sources to all matching sources.
	ExpandSources(sources Sources) (Sources, error)
}

// IteratorCreators represents a list of iterator creators.
type IteratorCreators []IteratorCreator

// Close closes all iterator creators that implement io.Closer.
func (a IteratorCreators) Close() error {
	for _, ic := range a {
		if ic, ok := ic.(io.Closer); ok {
			ic.Close()
		}
	}
	return nil
}

// CreateIterator returns a single combined iterator from multiple iterator creators.
func (a IteratorCreators) CreateIterator(opt IteratorOptions) (Iterator, error) {
	// Create iterators for each shard.
	// Ensure that they are closed if an error occurs.
	itrs := make([]Iterator, 0, len(a))
	if err := func() error {
		for _, ic := range a {
			itr, err := ic.CreateIterator(opt)
			if err != nil {
				return err
			}
			itrs = append(itrs, itr)
		}
		return nil
	}(); err != nil {
		Iterators(itrs).Close()
		return nil, err
	}

	// Merge into a single iterator.
	if opt.MergeSorted() {
		itr := NewSortedMergeIterator(itrs, opt)
		if opt.InterruptCh != nil {
			itr = NewInterruptIterator(itr, opt.InterruptCh)
		}
		return itr, nil
	}

	itr := NewMergeIterator(itrs, opt)
	if opt.Expr != nil {
		if expr, ok := opt.Expr.(*Call); ok && expr.Name == "count" {
			opt.Expr = &Call{
				Name: "sum",
				Args: expr.Args,
			}
		}
	}

	if opt.InterruptCh != nil {
		itr = NewInterruptIterator(itr, opt.InterruptCh)
	}
	return NewCallIterator(itr, opt)
}

// FieldDimensions returns unique fields and dimensions from multiple iterator creators.
func (a IteratorCreators) FieldDimensions(sources Sources) (fields, dimensions map[string]struct{}, err error) {
	fields = make(map[string]struct{})
	dimensions = make(map[string]struct{})

	for _, ic := range a {
		f, d, err := ic.FieldDimensions(sources)
		if err != nil {
			return nil, nil, err
		}
		for k := range f {
			fields[k] = struct{}{}
		}
		for k := range d {
			dimensions[k] = struct{}{}
		}
	}
	return
}

// SeriesKeys returns a list of series in all iterator creators in a.
// If a series exists in multiple creators in a, all instances will be combined
// into a single Series by calling Combine on it.
func (a IteratorCreators) SeriesKeys(opt IteratorOptions) (SeriesList, error) {
	seriesMap := make(map[string]Series)
	for _, ic := range a {
		series, err := ic.SeriesKeys(opt)
		if err != nil {
			return nil, err
		}

		for _, s := range series {
			cur, ok := seriesMap[s.ID()]
			if ok {
				cur.Combine(&s)
			} else {
				seriesMap[s.ID()] = s
			}
		}
	}

	seriesList := make([]Series, 0, len(seriesMap))
	for _, s := range seriesMap {
		seriesList = append(seriesList, s)
	}
	sort.Sort(SeriesList(seriesList))
	return SeriesList(seriesList), nil
}

// ExpandSources expands sources across all iterator creators and returns a unique result.
func (a IteratorCreators) ExpandSources(sources Sources) (Sources, error) {
	m := make(map[string]Source)

	for _, ic := range a {
		expanded, err := ic.ExpandSources(sources)
		if err != nil {
			return nil, err
		}

		for _, src := range expanded {
			switch src := src.(type) {
			case *Measurement:
				m[src.String()] = src
			default:
				return nil, fmt.Errorf("IteratorCreators.ExpandSources: unsupported source type: %T", src)
			}
		}
	}

	// Convert set to sorted slice.
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)

	// Convert set to a list of Sources.
	sorted := make(Sources, 0, len(m))
	for _, name := range names {
		sorted = append(sorted, m[name])
	}

	return sorted, nil
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

	// If this channel is set and is closed, the iterator should try to exit
	// and close as soon as possible.
	InterruptCh <-chan struct{}
}

// newIteratorOptionsStmt creates the iterator options from stmt.
func newIteratorOptionsStmt(stmt *SelectStatement, sopt *SelectOptions) (opt IteratorOptions, err error) {
	// Determine time range from the condition.
	startTime, endTime, err := TimeRange(stmt.Condition)
	if err != nil {
		return IteratorOptions{}, err
	}

	if !startTime.IsZero() {
		opt.StartTime = startTime.UnixNano()
	} else {
		if sopt != nil {
			opt.StartTime = sopt.MinTime.UnixNano()
		} else {
			opt.StartTime = MinTime
		}
	}
	if !endTime.IsZero() {
		opt.EndTime = endTime.UnixNano()
	} else {
		if sopt != nil {
			opt.EndTime = sopt.MaxTime.UnixNano()
		} else {
			opt.EndTime = MaxTime
		}
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
	if opt.Fill == NullFill && stmt.Target != nil {
		// Set the fill option to none if a target has been given.
		// Null values will get ignored when being written to the target
		// so fill(null) wouldn't write any null values to begin with.
		opt.Fill = NoFill
	}
	opt.Limit, opt.Offset = stmt.Limit, stmt.Offset
	opt.SLimit, opt.SOffset = stmt.SLimit, stmt.SOffset
	if sopt != nil {
		opt.InterruptCh = sopt.InterruptCh
	}

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
		return opt.StartTime, opt.EndTime + 1
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

// MarshalBinary encodes opt into a binary format.
func (opt *IteratorOptions) MarshalBinary() ([]byte, error) {
	return proto.Marshal(encodeIteratorOptions(opt))
}

// UnmarshalBinary decodes from a binary format in to opt.
func (opt *IteratorOptions) UnmarshalBinary(buf []byte) error {
	var pb internal.IteratorOptions
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}

	other, err := decodeIteratorOptions(&pb)
	if err != nil {
		return err
	}
	*opt = *other

	return nil
}

func encodeIteratorOptions(opt *IteratorOptions) *internal.IteratorOptions {
	pb := &internal.IteratorOptions{
		Aux:        opt.Aux,
		Interval:   encodeInterval(opt.Interval),
		Dimensions: opt.Dimensions,
		Fill:       proto.Int32(int32(opt.Fill)),
		StartTime:  proto.Int64(opt.StartTime),
		EndTime:    proto.Int64(opt.EndTime),
		Ascending:  proto.Bool(opt.Ascending),
		Limit:      proto.Int64(int64(opt.Limit)),
		Offset:     proto.Int64(int64(opt.Offset)),
		SLimit:     proto.Int64(int64(opt.SLimit)),
		SOffset:    proto.Int64(int64(opt.SOffset)),
		Dedupe:     proto.Bool(opt.Dedupe),
	}

	// Set expression, if set.
	if opt.Expr != nil {
		pb.Expr = proto.String(opt.Expr.String())
	}

	// Convert and encode sources to measurements.
	sources := make([]*internal.Measurement, len(opt.Sources))
	for i, source := range opt.Sources {
		mm := source.(*Measurement)
		sources[i] = encodeMeasurement(mm)
	}
	pb.Sources = sources

	// Fill value can only be a number. Set it if available.
	if v, ok := opt.FillValue.(float64); ok {
		pb.FillValue = proto.Float64(v)
	}

	// Set condition, if set.
	if opt.Condition != nil {
		pb.Condition = proto.String(opt.Condition.String())
	}

	return pb
}

func decodeIteratorOptions(pb *internal.IteratorOptions) (*IteratorOptions, error) {
	opt := &IteratorOptions{
		Aux:        pb.GetAux(),
		Interval:   decodeInterval(pb.GetInterval()),
		Dimensions: pb.GetDimensions(),
		Fill:       FillOption(pb.GetFill()),
		FillValue:  pb.GetFillValue(),
		StartTime:  pb.GetStartTime(),
		EndTime:    pb.GetEndTime(),
		Ascending:  pb.GetAscending(),
		Limit:      int(pb.GetLimit()),
		Offset:     int(pb.GetOffset()),
		SLimit:     int(pb.GetSLimit()),
		SOffset:    int(pb.GetSOffset()),
		Dedupe:     pb.GetDedupe(),
	}

	// Set expression, if set.
	if pb.Expr != nil {
		expr, err := ParseExpr(pb.GetExpr())
		if err != nil {
			return nil, err
		}
		opt.Expr = expr
	}

	// Convert and encode sources to measurements.
	sources := make([]Source, len(pb.GetSources()))
	for i, source := range pb.GetSources() {
		mm, err := decodeMeasurement(source)
		if err != nil {
			return nil, err
		}
		sources[i] = mm
	}
	opt.Sources = sources

	// Set condition, if set.
	if pb.Condition != nil {
		expr, err := ParseExpr(pb.GetCondition())
		if err != nil {
			return nil, err
		}
		opt.Condition = expr
	}

	return opt, nil
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

func encodeSeries(s Series) *internal.Series {
	aux := make([]uint32, len(s.Aux))
	for i := range s.Aux {
		aux[i] = uint32(s.Aux[i])
	}

	return &internal.Series{
		Name: proto.String(s.Name),
		Tags: encodeTags(s.Tags.KeyValues()),
		Aux:  aux,
	}
}

func decodeSeries(pb *internal.Series) Series {
	var aux []DataType
	if len(pb.GetAux()) > 0 {
		aux = make([]DataType, len(pb.GetAux()))
		for i := range pb.GetAux() {
			aux[i] = DataType(pb.GetAux()[i])
		}
	}

	return Series{
		Name: pb.GetName(),
		Tags: newTagsID(string(pb.GetTags())),
		Aux:  aux,
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

// MarshalBinary encodes list into a binary format.
func (a SeriesList) MarshalBinary() ([]byte, error) {
	return proto.Marshal(encodeSeriesList(a))
}

// UnmarshalBinary decodes from a binary format.
func (a *SeriesList) UnmarshalBinary(buf []byte) error {
	var pb internal.SeriesList
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}

	(*a) = decodeSeriesList(&pb)

	return nil
}

func encodeSeriesList(a SeriesList) *internal.SeriesList {
	pb := make([]*internal.Series, len(a))
	for i := range a {
		pb[i] = encodeSeries(a[i])
	}

	return &internal.SeriesList{
		Items: pb,
	}
}

func decodeSeriesList(pb *internal.SeriesList) SeriesList {
	a := make([]Series, len(pb.GetItems()))
	for i := range pb.GetItems() {
		a[i] = decodeSeries(pb.GetItems()[i])
	}
	return SeriesList(a)
}

// Interval represents a repeating interval for a query.
type Interval struct {
	Duration time.Duration
	Offset   time.Duration
}

// IsZero returns true if the interval has no duration.
func (i Interval) IsZero() bool { return i.Duration == 0 }

func encodeInterval(i Interval) *internal.Interval {
	return &internal.Interval{
		Duration: proto.Int64(i.Duration.Nanoseconds()),
		Offset:   proto.Int64(i.Offset.Nanoseconds()),
	}
}

func decodeInterval(pb *internal.Interval) Interval {
	return Interval{
		Duration: time.Duration(pb.GetDuration()),
		Offset:   time.Duration(pb.GetOffset()),
	}
}

type nilFloatIterator struct{}

func (*nilFloatIterator) Stats() IteratorStats { return IteratorStats{} }
func (*nilFloatIterator) Close() error         { return nil }
func (*nilFloatIterator) Next() *FloatPoint    { return nil }

// integerFloatTransformIterator executes a function to modify an existing point for every
// output of the input iterator.
type integerFloatTransformIterator struct {
	input IntegerIterator
	fn    integerFloatTransformFunc
}

// Stats returns stats from the input iterator.
func (itr *integerFloatTransformIterator) Stats() IteratorStats { return itr.input.Stats() }

// Close closes the iterator and all child iterators.
func (itr *integerFloatTransformIterator) Close() error { return itr.input.Close() }

// Next returns the minimum value for the next available interval.
func (itr *integerFloatTransformIterator) Next() *FloatPoint {
	p := itr.input.Next()
	if p != nil {
		return itr.fn(p)
	}
	return nil
}

// integerFloatTransformFunc creates or modifies a point.
// The point passed in may be modified and returned rather than allocating a
// new point if possible.
type integerFloatTransformFunc func(p *IntegerPoint) *FloatPoint

type integerFloatCastIterator struct {
	input IntegerIterator
}

func (itr *integerFloatCastIterator) Stats() IteratorStats { return itr.input.Stats() }
func (itr *integerFloatCastIterator) Close() error         { return itr.input.Close() }
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

// IteratorStats represents statistics about an iterator.
// Some statistics are available immediately upon iterator creation while
// some are derived as the iterator processes data.
type IteratorStats struct {
	SeriesN int // series represented
	PointN  int // points returned
}

// Add aggregates fields from s and other together. Overwrites s.
func (s *IteratorStats) Add(other IteratorStats) {
	s.SeriesN += other.SeriesN
	s.PointN += other.PointN
}

func encodeIteratorStats(stats *IteratorStats) *internal.IteratorStats {
	return &internal.IteratorStats{
		SeriesN: proto.Int64(int64(stats.SeriesN)),
		PointN:  proto.Int64(int64(stats.PointN)),
	}
}

func decodeIteratorStats(pb *internal.IteratorStats) IteratorStats {
	return IteratorStats{
		SeriesN: int(pb.GetSeriesN()),
		PointN:  int(pb.GetPointN()),
	}
}
