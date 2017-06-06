package query

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"sort"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query/internal/iterators"
)

// WriteEdge is the end of the edge that is written to by the Node.
type WriteEdge struct {
	// Node is the node that creates an Iterator and sends it to this edge.
	// This should always be set to a value.
	Node Node

	// Output is the output end of the edge. This should always be set.
	Output *ReadEdge

	itr   influxql.Iterator
	ready bool
	mu    sync.RWMutex
}

// SetIterator marks this Edge as ready and sets the Iterator as the returned
// iterator. If the Edge has already been set, this panics. The result can be
// retrieved from the output edge.
func (e *WriteEdge) SetIterator(itr influxql.Iterator) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.ready {
		panic("unable to call SetIterator on the same node twice")
	}
	e.itr = itr
	e.ready = true
}

// WrapIterator replaces the returned Iterator with the result of a function
// that wraps the original Iterator. If the Iterator is nil, the function is
// not called.
func (e *WriteEdge) WrapIterator(fn func(influxql.Iterator) influxql.Iterator) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.ready {
		panic(fmt.Sprintf("attempted to wrap an iterator from an edge before it was ready: %T", e.Node))
	}
	if e.itr != nil {
		e.itr = fn(e.itr)
	}
}

// Attach attaches this WriteEdge to a Node and returns the WriteEdge so it
// can be assigned to the Node's output.
func (e *WriteEdge) Attach(n Node) *WriteEdge {
	e.Node = n
	return e
}

// Chain attaches this Node to the WriteEdge and creates a new ReadEdge that
// is attached to the same node. It returns a new WriteEdge to replace
// the one.
func (e *WriteEdge) Chain(n Node) (*WriteEdge, *ReadEdge) {
	e.Node = n
	return AddEdge(nil, n)
}

// ReadEdge is the end of the edge that reads from the Iterator.
type ReadEdge struct {
	// Node is the node that will read the Iterator from this edge.
	// This may be nil if there is no Node that will read this edge.
	Node Node

	// Input is the input end of the edge. This should always be set.
	Input *WriteEdge
}

// Iterator returns the Iterator created for this Node by the WriteEdge.
// If the WriteEdge is not ready, this function will panic.
func (e *ReadEdge) Iterator() influxql.Iterator {
	e.Input.mu.RLock()
	if !e.Input.ready {
		e.Input.mu.RUnlock()
		panic(fmt.Sprintf("attempted to retrieve an iterator from an edge before it was ready: %T", e.Input.Node))
	}
	itr := e.Input.itr
	e.Input.mu.RUnlock()
	return itr
}

// Ready returns whether this ReadEdge is ready to be read from. This edge
// will be ready after the attached WriteEdge has called SetIterator().
func (e *ReadEdge) Ready() (ready bool) {
	e.Input.mu.RLock()
	ready = e.Input.ready
	e.Input.mu.RUnlock()
	return ready
}

// Insert splits the current edge and inserts a Node into the middle.
// It then returns the newly created ReadEdge that points to the inserted
// Node and the newly created WriteEdge that the Node should use to send its
// results.
func (e *ReadEdge) Insert(n Node) (*ReadEdge, *WriteEdge) {
	// Create a new ReadEdge. The input should be the current WriteEdge for
	// this node.
	out := &ReadEdge{Node: n, Input: e.Input}
	// Reset the Input so it points to the newly created output as its input.
	e.Input.Output = out
	// Redirect this ReadEdge's input to a new input edge.
	e.Input = &WriteEdge{Node: n, Output: e}
	// Return the newly created edges so they can be stored with the newly
	// inserted Node.
	return out, e.Input
}

// Append adds a node to the end of the chain.
func (e *ReadEdge) Append(n Node) (*WriteEdge, *ReadEdge) {
	e.Node = n
	return AddEdge(n, nil)
}

// NewEdge creates a new edge where both sides are unattached.
func NewEdge() (*WriteEdge, *ReadEdge) {
	input := &WriteEdge{}
	output := &ReadEdge{}
	input.Output, output.Input = output, input
	return input, output
}

// NewReadEdge creates a new edge with the ReadEdge attached to the Node
// and an unattached WriteEdge.
func NewReadEdge(n Node) (*WriteEdge, *ReadEdge) {
	input, output := NewEdge()
	output.Node = n
	return input, output
}

// AddEdge creates a new edge between two nodes.
func AddEdge(in, out Node) (*WriteEdge, *ReadEdge) {
	input := &WriteEdge{Node: in}
	output := &ReadEdge{Node: out}
	input.Output, output.Input = output, input
	return input, output
}

// RemoveNode removes the edges pointing to a node and connects the other sides
// of the edge to each other.
func RemoveNode(in *ReadEdge, out *WriteEdge) {
	// Retrieve the other side of the edge and assign their matching pair
	// to each other.
	write, read := in.Input, out.Output
	read.Input, write.Output = write, read
}

type Node interface {
	// Description returns a brief description about what this node does.  This
	// should include details that describe what the node will do based on the
	// current configuration of the node.
	Description() string

	// Inputs returns the Edges that produce Iterators that will be consumed by
	// this Node.
	Inputs() []*ReadEdge

	// Outputs returns the Edges that will receive an Iterator from this Node.
	Outputs() []*WriteEdge

	// Execute executes the Node and transmits the created Iterators to the
	// output edges.
	Execute() error

	// Type returns the type output for this node if it is known. Typically,
	// type information is available after linking, but it may be known
	// before that.
	Type() influxql.DataType
}

// RestrictedTypeInputNode is implemented by Nodes that accept a restricted
// set of type options.
type RestrictedTypeInputNode interface {
	Node

	// ValidateInputTypes validates the inputs for this Node and
	// returns an appropriate error if the input type is not valid.
	ValidateInputTypes() error
}

// AllInputsReady determines if all of the input edges for a node are ready.
func AllInputsReady(n Node) bool {
	inputs := n.Inputs()
	if len(inputs) == 0 {
		return true
	}

	for _, input := range inputs {
		if !input.Ready() {
			return false
		}
	}
	return true
}

var _ Node = &IteratorCreator{}

type IteratorCreator struct {
	Expr            *influxql.VarRef
	FunctionCall    string
	Condition       influxql.Expr
	AuxiliaryFields *AuxiliaryFields
	Database        Database
	Dimensions      []string
	Tags            map[string]struct{}
	Interval        influxql.Interval
	Location        *time.Location
	Ordered         bool
	TimeRange       TimeRange
	Ascending       bool
	SLimit          int
	SOffset         int
	MaxSeriesN      int
	Output          *WriteEdge
}

func (ic *IteratorCreator) Description() string {
	var buf bytes.Buffer
	buf.WriteString("create iterator")
	if ic.Expr != nil {
		var expr influxql.Expr = ic.Expr
		if ic.FunctionCall != "" {
			expr = &influxql.Call{
				Name: ic.FunctionCall,
				Args: []influxql.Expr{ic.Expr},
			}
		}
		fmt.Fprintf(&buf, " for %s", expr)
	}
	if ic.AuxiliaryFields != nil {
		names := make([]string, 0, len(ic.AuxiliaryFields.Aux))
		for _, name := range ic.AuxiliaryFields.Aux {
			names = append(names, name.String())
		}
		fmt.Fprintf(&buf, " [aux: %s]", strings.Join(names, ", "))
	}
	if len(ic.Tags) > 0 {
		tags := make([]string, 0, len(ic.Tags))
		for d := range ic.Tags {
			tags = append(tags, d)
		}
		sort.Strings(tags)
		fmt.Fprintf(&buf, " group by %s", strings.Join(tags, ", "))
	}
	if !ic.Interval.IsZero() {
		fmt.Fprintf(&buf, " interval(%s)", ic.Interval.Duration)
	}
	fmt.Fprintf(&buf, " from %s to %s", ic.TimeRange.Min, ic.TimeRange.Max)
	if ic.Condition != nil {
		fmt.Fprintf(&buf, " where %s", ic.Condition)
	}
	return buf.String()
}

func (ic *IteratorCreator) Inputs() []*ReadEdge { return nil }
func (ic *IteratorCreator) Outputs() []*WriteEdge {
	if ic.Output != nil {
		return []*WriteEdge{ic.Output}
	}
	return nil
}

func (ic *IteratorCreator) Execute() error {
	var auxFields []influxql.VarRef
	if ic.AuxiliaryFields != nil {
		auxFields = ic.AuxiliaryFields.Aux
	}
	opt := influxql.IteratorOptions{
		Expr:       ic.Expr,
		Condition:  ic.Condition,
		Dimensions: ic.Dimensions,
		GroupBy:    ic.Tags,
		Interval:   ic.Interval,
		Location:   ic.Location,
		Ordered:    ic.Ordered,
		Aux:        auxFields,
		StartTime:  ic.TimeRange.Min.UnixNano(),
		EndTime:    ic.TimeRange.Max.UnixNano(),
		Ascending:  ic.Ascending,
		SLimit:     ic.SLimit,
		SOffset:    ic.SOffset,
		MaxSeriesN: ic.MaxSeriesN,
	}
	if ic.FunctionCall != "" {
		opt.Expr = &influxql.Call{
			Name: ic.FunctionCall,
			Args: []influxql.Expr{opt.Expr},
		}
	}
	itr, err := ic.Database.CreateIterator(opt)
	if err != nil {
		return err
	}
	ic.Output.SetIterator(itr)
	return nil
}

func (ic *IteratorCreator) Type() influxql.DataType {
	if ic.Expr != nil {
		if ic.FunctionCall != "" {
			switch ic.FunctionCall {
			case "mean":
				return influxql.Float
			case "count":
				return influxql.Integer
			}
		}
		return ic.Database.MapType(ic.Expr.Val)
	}
	return influxql.Unknown
}

var _ Node = &Merge{}

type Merge struct {
	Ordered    bool
	Ascending  bool
	InputNodes []*ReadEdge
	Output     *WriteEdge
}

func (m *Merge) Description() string {
	if m.Ordered {
		return fmt.Sprintf("sorted merge of %d nodes", len(m.InputNodes))
	}
	return fmt.Sprintf("merge of %d nodes", len(m.InputNodes))
}

func (m *Merge) AddInput(n Node) *WriteEdge {
	in, out := AddEdge(n, m)
	m.InputNodes = append(m.InputNodes, out)
	return in
}

func (m *Merge) Inputs() []*ReadEdge   { return m.InputNodes }
func (m *Merge) Outputs() []*WriteEdge { return []*WriteEdge{m.Output} }

func (m *Merge) Execute() error {
	if len(m.InputNodes) == 0 {
		m.Output.SetIterator(nil)
		return nil
	} else if len(m.InputNodes) == 1 {
		m.Output.SetIterator(m.InputNodes[0].Iterator())
		return nil
	}

	inputs := make([]influxql.Iterator, len(m.InputNodes))
	for i, input := range m.InputNodes {
		inputs[i] = input.Iterator()
	}

	opt := influxql.IteratorOptions{Ascending: m.Ascending}
	if m.Ordered {
		itr := influxql.NewSortedMergeIterator(inputs, opt)
		m.Output.SetIterator(itr)
	} else {
		itr := influxql.NewMergeIterator(inputs, opt)
		m.Output.SetIterator(itr)
	}
	return nil
}

func (m *Merge) Type() influxql.DataType {
	var typ influxql.DataType
	for _, input := range m.Inputs() {
		if n := input.Input.Node; n != nil {
			if t := n.Type(); typ.LessThan(t) {
				typ = t
			}
		}
	}
	return typ
}

var _ Node = &FunctionCall{}

type FunctionCall struct {
	Name       string
	Dimensions *Dimensions
	GroupBy    *Dimensions
	Interval   influxql.Interval
	Location   *time.Location
	Ascending  bool
	Ordered    bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (c *FunctionCall) Description() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s()", c.Name)
	if c.GroupBy.Len() > 0 {
		tags := make([]string, 0, c.GroupBy.Len())
		for d := range c.GroupBy.Map() {
			tags = append(tags, d)
		}
		sort.Strings(tags)
		fmt.Fprintf(&buf, " group by %s", strings.Join(tags, ", "))
	}
	return buf.String()
}

func (c *FunctionCall) Inputs() []*ReadEdge   { return []*ReadEdge{c.Input} }
func (c *FunctionCall) Outputs() []*WriteEdge { return []*WriteEdge{c.Output} }

func (c *FunctionCall) Execute() error {
	input := c.Input.Iterator()
	if input == nil {
		c.Output.SetIterator(nil)
		return nil
	}

	call := &influxql.Call{Name: c.Name}
	opt := influxql.IteratorOptions{
		Expr:       call,
		Dimensions: c.Dimensions.Get(),
		GroupBy:    c.GroupBy.Map(),
		Interval:   c.Interval,
		Location:   c.Location,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  c.Ascending,
		Ordered:    c.Ordered,
	}
	itr, err := influxql.NewCallIterator(input, opt)
	if err != nil {
		return err
	}
	c.Output.SetIterator(itr)
	return nil
}

func (c *FunctionCall) Type() influxql.DataType {
	switch c.Name {
	case "mean":
		return influxql.Float
	case "count":
		return influxql.Integer
	default:
		if n := c.Input.Input.Node; n != nil {
			return n.Type()
		}
		return influxql.Unknown
	}
}

func (c *FunctionCall) ValidateInputTypes() error {
	n := c.Input.Input.Node
	if n == nil {
		return nil
	}

	typ := n.Type()
	if typ == influxql.Unknown {
		return nil
	}

	switch c.Name {
	case "min", "max":
		if typ != influxql.Float && typ != influxql.Integer && typ != influxql.Boolean {
			return fmt.Errorf("cannot use type %s in argument to %s", typ, c.Name)
		}
	case "sum", "mean":
		if typ != influxql.Float && typ != influxql.Integer {
			return fmt.Errorf("cannot use type %s in argument to %s", typ, c.Name)
		}
	}
	return nil
}

type Median struct {
	Dimensions *Dimensions
	GroupBy    *Dimensions
	Interval   influxql.Interval
	Location   *time.Location
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (m *Median) Description() string {
	return "median()"
}

func (m *Median) Inputs() []*ReadEdge   { return []*ReadEdge{m.Input} }
func (m *Median) Outputs() []*WriteEdge { return []*WriteEdge{m.Output} }

func (m *Median) Execute() error {
	input := m.Input.Iterator()
	if input == nil {
		m.Output.SetIterator(nil)
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: m.Dimensions.Get(),
		GroupBy:    m.GroupBy.Map(),
		Interval:   m.Interval,
		Location:   m.Location,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  m.Ascending,
		Ordered:    true,
	}
	itr, err := influxql.NewMedianIterator(input, opt)
	if err != nil {
		return err
	}
	m.Output.SetIterator(itr)
	return nil
}

func (m *Median) Type() influxql.DataType {
	return influxql.Float
}

func (m *Median) ValidateInputTypes() error {
	n := m.Input.Input.Node
	if n == nil {
		return nil
	}

	typ := n.Type()
	if typ == influxql.Unknown {
		return nil
	} else if typ != influxql.Float && typ != influxql.Integer {
		return fmt.Errorf("cannot use type %s in argument to median", typ)
	}
	return nil
}

type Mode struct {
	Dimensions *Dimensions
	GroupBy    *Dimensions
	Interval   influxql.Interval
	Location   *time.Location
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (m *Mode) Description() string {
	return "mode()"
}

func (m *Mode) Inputs() []*ReadEdge   { return []*ReadEdge{m.Input} }
func (m *Mode) Outputs() []*WriteEdge { return []*WriteEdge{m.Output} }

func (m *Mode) Execute() error {
	input := m.Input.Iterator()
	if input == nil {
		m.Output.SetIterator(nil)
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: m.Dimensions.Get(),
		GroupBy:    m.GroupBy.Map(),
		Interval:   m.Interval,
		Location:   m.Location,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  m.Ascending,
	}
	itr, err := influxql.NewModeIterator(input, opt)
	if err != nil {
		return err
	}
	m.Output.SetIterator(itr)
	return nil
}

func (m *Mode) Type() influxql.DataType {
	if n := m.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type Stddev struct {
	Dimensions *Dimensions
	GroupBy    *Dimensions
	Interval   influxql.Interval
	Location   *time.Location
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (s *Stddev) Description() string {
	return "stddev()"
}

func (s *Stddev) Inputs() []*ReadEdge   { return []*ReadEdge{s.Input} }
func (s *Stddev) Outputs() []*WriteEdge { return []*WriteEdge{s.Output} }

func (s *Stddev) Execute() error {
	input := s.Input.Iterator()
	if input == nil {
		s.Output.SetIterator(nil)
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: s.Dimensions.Get(),
		GroupBy:    s.GroupBy.Map(),
		Interval:   s.Interval,
		Location:   s.Location,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  s.Ascending,
	}
	itr, err := influxql.NewStddevIterator(input, opt)
	if err != nil {
		return err
	}
	s.Output.SetIterator(itr)
	return nil
}

func (s *Stddev) Type() influxql.DataType {
	if n := s.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type Spread struct {
	Dimensions *Dimensions
	GroupBy    *Dimensions
	Interval   influxql.Interval
	Location   *time.Location
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (s *Spread) Description() string {
	return "spread()"
}

func (s *Spread) Inputs() []*ReadEdge   { return []*ReadEdge{s.Input} }
func (s *Spread) Outputs() []*WriteEdge { return []*WriteEdge{s.Output} }

func (s *Spread) Execute() error {
	input := s.Input.Iterator()
	if input == nil {
		s.Output.SetIterator(nil)
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: s.Dimensions.Get(),
		GroupBy:    s.GroupBy.Map(),
		Interval:   s.Interval,
		Location:   s.Location,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  s.Ascending,
	}
	itr, err := influxql.NewSpreadIterator(input, opt)
	if err != nil {
		return err
	}
	s.Output.SetIterator(itr)
	return nil
}

func (s *Spread) Type() influxql.DataType {
	if n := s.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type Percentile struct {
	Number     float64
	Dimensions *Dimensions
	GroupBy    *Dimensions
	Interval   influxql.Interval
	Location   *time.Location
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (p *Percentile) Description() string {
	return fmt.Sprintf("percentile(%2.f)", p.Number)
}

func (p *Percentile) Inputs() []*ReadEdge   { return []*ReadEdge{p.Input} }
func (p *Percentile) Outputs() []*WriteEdge { return []*WriteEdge{p.Output} }

func (p *Percentile) Execute() error {
	input := p.Input.Iterator()
	if input == nil {
		p.Output.SetIterator(nil)
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: p.Dimensions.Get(),
		GroupBy:    p.GroupBy.Map(),
		Interval:   p.Interval,
		Location:   p.Location,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  p.Ascending,
		Ordered:    true,
	}
	itr, err := influxql.NewPercentileIterator(input, opt, p.Number)
	if err != nil {
		return err
	}
	p.Output.SetIterator(itr)
	return nil
}

func (p *Percentile) Type() influxql.DataType {
	if n := p.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type Sample struct {
	N          int
	Dimensions *Dimensions
	GroupBy    *Dimensions
	Interval   influxql.Interval
	Location   *time.Location
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (s *Sample) Description() string {
	return fmt.Sprintf("sample(%d)", s.N)
}

func (s *Sample) Inputs() []*ReadEdge   { return []*ReadEdge{s.Input} }
func (s *Sample) Outputs() []*WriteEdge { return []*WriteEdge{s.Output} }

func (s *Sample) Execute() error {
	input := s.Input.Iterator()
	if input == nil {
		s.Output.SetIterator(nil)
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: s.Dimensions.Get(),
		GroupBy:    s.GroupBy.Map(),
		Interval:   s.Interval,
		Location:   s.Location,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  s.Ascending,
	}
	itr, err := influxql.NewSampleIterator(input, opt, s.N)
	if err != nil {
		return err
	}
	s.Output.SetIterator(itr)
	return nil
}

func (s *Sample) Type() influxql.DataType {
	if n := s.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type Derivative struct {
	Duration      time.Duration
	IsNonNegative bool
	Dimensions    *Dimensions
	GroupBy       *Dimensions
	Ascending     bool
	Input         *ReadEdge
	Output        *WriteEdge
}

func (d *Derivative) Description() string {
	if d.IsNonNegative {
		return fmt.Sprintf("non_negative_derivative(%s)", influxql.FormatDuration(d.Duration))
	}
	return fmt.Sprintf("derivative(%s)", influxql.FormatDuration(d.Duration))
}

func (d *Derivative) Inputs() []*ReadEdge   { return []*ReadEdge{d.Input} }
func (d *Derivative) Outputs() []*WriteEdge { return []*WriteEdge{d.Output} }

func (d *Derivative) Execute() error {
	input := d.Input.Iterator()
	if input == nil {
		d.Output.SetIterator(nil)
		return nil
	}

	interval := influxql.Interval{Duration: d.Duration}
	opt := influxql.IteratorOptions{
		Dimensions: d.Dimensions.Get(),
		GroupBy:    d.GroupBy.Map(),
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  d.Ascending,
	}
	itr, err := influxql.NewDerivativeIterator(input, opt, interval, d.IsNonNegative)
	if err != nil {
		return err
	}
	d.Output.SetIterator(itr)
	return nil
}

func (d *Derivative) Type() influxql.DataType {
	return influxql.Float
}

type Elapsed struct {
	Duration   time.Duration
	Dimensions *Dimensions
	GroupBy    *Dimensions
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (e *Elapsed) Description() string {
	return fmt.Sprintf("elapsed(%s)", influxql.FormatDuration(e.Duration))
}

func (e *Elapsed) Inputs() []*ReadEdge   { return []*ReadEdge{e.Input} }
func (e *Elapsed) Outputs() []*WriteEdge { return []*WriteEdge{e.Output} }

func (e *Elapsed) Execute() error {
	input := e.Input.Iterator()
	if input == nil {
		e.Output.SetIterator(nil)
		return nil
	}

	interval := influxql.Interval{Duration: e.Duration}
	opt := influxql.IteratorOptions{
		Dimensions: e.Dimensions.Get(),
		GroupBy:    e.GroupBy.Map(),
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  e.Ascending,
	}
	itr, err := influxql.NewElapsedIterator(input, opt, interval)
	if err != nil {
		return err
	}
	e.Output.SetIterator(itr)
	return nil
}

func (e *Elapsed) Type() influxql.DataType {
	return influxql.Integer
}

type Difference struct {
	IsNonNegative bool
	Dimensions    *Dimensions
	GroupBy       *Dimensions
	Ascending     bool
	Input         *ReadEdge
	Output        *WriteEdge
}

func (d *Difference) Description() string {
	if d.IsNonNegative {
		return "non_negative_difference()"
	}
	return "difference()"
}

func (d *Difference) Inputs() []*ReadEdge   { return []*ReadEdge{d.Input} }
func (d *Difference) Outputs() []*WriteEdge { return []*WriteEdge{d.Output} }

func (d *Difference) Execute() error {
	input := d.Input.Iterator()
	if input == nil {
		d.Output.SetIterator(nil)
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: d.Dimensions.Get(),
		GroupBy:    d.GroupBy.Map(),
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  d.Ascending,
	}
	itr, err := influxql.NewDifferenceIterator(input, opt, d.IsNonNegative)
	if err != nil {
		return err
	}
	d.Output.SetIterator(itr)
	return nil
}

func (d *Difference) Type() influxql.DataType {
	if n := d.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type MovingAverage struct {
	WindowSize int
	Dimensions *Dimensions
	GroupBy    *Dimensions
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (m *MovingAverage) Description() string {
	return fmt.Sprintf("moving_average(%d)", m.WindowSize)
}

func (m *MovingAverage) Inputs() []*ReadEdge   { return []*ReadEdge{m.Input} }
func (m *MovingAverage) Outputs() []*WriteEdge { return []*WriteEdge{m.Output} }

func (m *MovingAverage) Execute() error {
	input := m.Input.Iterator()
	if input == nil {
		m.Output.SetIterator(nil)
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: m.Dimensions.Get(),
		GroupBy:    m.GroupBy.Map(),
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  m.Ascending,
	}
	itr, err := influxql.NewMovingAverageIterator(input, m.WindowSize, opt)
	if err != nil {
		return err
	}
	m.Output.SetIterator(itr)
	return nil
}

func (m *MovingAverage) Type() influxql.DataType {
	return influxql.Float
}

type CumulativeSum struct {
	Dimensions *Dimensions
	GroupBy    *Dimensions
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (c *CumulativeSum) Description() string {
	return "cumulative_sum()"
}

func (c *CumulativeSum) Inputs() []*ReadEdge   { return []*ReadEdge{c.Input} }
func (c *CumulativeSum) Outputs() []*WriteEdge { return []*WriteEdge{c.Output} }

func (c *CumulativeSum) Execute() error {
	input := c.Input.Iterator()
	if input == nil {
		c.Output.SetIterator(nil)
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: c.Dimensions.Get(),
		GroupBy:    c.GroupBy.Map(),
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  c.Ascending,
	}
	itr, err := influxql.NewCumulativeSumIterator(input, opt)
	if err != nil {
		return err
	}
	c.Output.SetIterator(itr)
	return nil
}

func (c *CumulativeSum) Type() influxql.DataType {
	if n := c.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type Integral struct {
	Duration   time.Duration
	Dimensions *Dimensions
	GroupBy    *Dimensions
	Interval   influxql.Interval
	Location   *time.Location
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (i *Integral) Description() string {
	return fmt.Sprintf("integral(%s)", influxql.FormatDuration(i.Duration))
}

func (i *Integral) Inputs() []*ReadEdge   { return []*ReadEdge{i.Input} }
func (i *Integral) Outputs() []*WriteEdge { return []*WriteEdge{i.Output} }

func (i *Integral) Execute() error {
	input := i.Input.Iterator()
	if input == nil {
		i.Output.SetIterator(nil)
		return nil
	}

	interval := influxql.Interval{Duration: i.Duration}
	opt := influxql.IteratorOptions{
		Dimensions: i.Dimensions.Get(),
		GroupBy:    i.GroupBy.Map(),
		Interval:   i.Interval,
		Location:   i.Location,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  i.Ascending,
	}
	itr, err := influxql.NewIntegralIterator(input, opt, interval)
	if err != nil {
		return err
	}
	i.Output.SetIterator(itr)
	return nil
}

func (i *Integral) Type() influxql.DataType {
	return influxql.Float
}

type HoltWinters struct {
	N, S       int
	WithFit    bool
	Dimensions *Dimensions
	GroupBy    *Dimensions
	Interval   time.Duration
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (hw *HoltWinters) Description() string {
	if hw.WithFit {
		return fmt.Sprintf("holt_winters_with_fit(%d, %d)", hw.N, hw.S)
	}
	return fmt.Sprintf("holt_winters(%d, %d)", hw.N, hw.S)
}

func (hw *HoltWinters) Inputs() []*ReadEdge   { return []*ReadEdge{hw.Input} }
func (hw *HoltWinters) Outputs() []*WriteEdge { return []*WriteEdge{hw.Output} }

func (hw *HoltWinters) Execute() error {
	input := hw.Input.Iterator()
	if input == nil {
		hw.Output.SetIterator(nil)
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: hw.Dimensions.Get(),
		GroupBy:    hw.GroupBy.Map(),
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ascending:  hw.Ascending,
	}
	itr, err := influxql.NewHoltWintersIterator(input, opt, hw.N, hw.S, hw.WithFit, hw.Interval)
	if err != nil {
		return err
	}
	hw.Output.SetIterator(itr)
	return nil
}

func (hw *HoltWinters) Type() influxql.DataType {
	return influxql.Float
}

type Distinct struct {
	Dimensions *Dimensions
	GroupBy    *Dimensions
	Interval   influxql.Interval
	Location   *time.Location
	Ascending  bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (d *Distinct) Description() string {
	return "find distinct values"
}

func (d *Distinct) Inputs() []*ReadEdge   { return []*ReadEdge{d.Input} }
func (d *Distinct) Outputs() []*WriteEdge { return []*WriteEdge{d.Output} }

func (d *Distinct) Execute() error {
	opt := influxql.IteratorOptions{
		Dimensions: d.Dimensions.Get(),
		GroupBy:    d.GroupBy.Map(),
		Interval:   d.Interval,
		Location:   d.Location,
		Ascending:  d.Ascending,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
	}
	itr, err := influxql.NewDistinctIterator(d.Input.Iterator(), opt)
	if err != nil {
		return err
	}
	d.Output.SetIterator(itr)
	return nil
}

func (d *Distinct) Type() influxql.DataType {
	if n := d.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type TopBottomSelector struct {
	Name       string
	Limit      int
	Dimensions *Dimensions
	Interval   influxql.Interval
	Location   *time.Location
	Ascending  bool
	Ordered    bool
	KeepTags   bool
	Input      *ReadEdge
	Output     *WriteEdge
}

func (s *TopBottomSelector) Description() string {
	return fmt.Sprintf("%s(%d)", s.Name, s.Limit)
}

func (s *TopBottomSelector) Inputs() []*ReadEdge   { return []*ReadEdge{s.Input} }
func (s *TopBottomSelector) Outputs() []*WriteEdge { return []*WriteEdge{s.Output} }

func (s *TopBottomSelector) Execute() error {
	input := s.Input.Iterator()
	if input == nil {
		s.Output.SetIterator(nil)
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: s.Dimensions.Get(),
		Interval:   s.Interval,
		Location:   s.Location,
		StartTime:  influxql.MinTime,
		EndTime:    influxql.MaxTime,
		Ordered:    s.Ordered,
		Ascending:  s.Ascending,
	}
	var itr influxql.Iterator
	var err error
	if s.Name == "top" {
		itr, err = influxql.NewTopIterator(input, opt, s.Limit, s.KeepTags)
	} else {
		itr, err = influxql.NewBottomIterator(input, opt, s.Limit, s.KeepTags)
	}
	if err != nil {
		return err
	}
	s.Output.SetIterator(itr)
	return nil
}

func (s *TopBottomSelector) Type() influxql.DataType {
	if n := s.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

type AuxiliaryFields struct {
	Aux        []influxql.VarRef
	Dimensions *Dimensions
	Input      *ReadEdge
	Output     *WriteEdge
	outputs    []*WriteEdge
	refs       []*influxql.VarRef
}

func (c *AuxiliaryFields) Description() string {
	return "access auxiliary fields"
}

func (c *AuxiliaryFields) Inputs() []*ReadEdge { return []*ReadEdge{c.Input} }
func (c *AuxiliaryFields) Outputs() []*WriteEdge {
	if c.Output != nil {
		outputs := make([]*WriteEdge, 0, len(c.outputs)+1)
		outputs = append(outputs, c.Output)
		outputs = append(outputs, c.outputs...)
		return outputs
	} else {
		return c.outputs
	}
}

func (c *AuxiliaryFields) Execute() error {
	input := c.Input.Iterator()
	if input == nil {
		if c.Output != nil {
			c.Output.SetIterator(nil)
		}
		for i := range c.refs {
			c.outputs[i].SetIterator(nil)
		}
		return nil
	}

	opt := influxql.IteratorOptions{
		Dimensions: c.Dimensions.Get(),
		Aux:        c.Aux,
	}
	aitr := influxql.NewAuxIterator(input, opt)
	for i, ref := range c.refs {
		itr := aitr.Iterator(ref.Val, ref.Type)
		c.outputs[i].SetIterator(itr)
	}
	if c.Output != nil {
		c.Output.SetIterator(aitr)
		aitr.Start()
	} else {
		aitr.Background()
	}
	return nil
}

func (c *AuxiliaryFields) Type() influxql.DataType {
	if n := c.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

// Iterator registers an auxiliary field to be sent to the passed in WriteEdge
// and configures that WriteEdge with the AuxiliaryFields as its Node.
func (c *AuxiliaryFields) Iterator(ref *influxql.VarRef, out *WriteEdge) {
	field := &AuxiliaryField{Ref: ref, Output: out}
	out.Node = field

	out, field.Input = AddEdge(c, field)
	c.outputs = append(c.outputs, out)

	// Attempt to find an existing variable that matches this one to avoid
	// duplicating the same variable reference in the auxiliary fields.
	for idx := range c.Aux {
		v := &c.Aux[idx]
		if *v == *ref {
			c.refs = append(c.refs, v)
			return
		}
	}

	// Register a new auxiliary field and take a reference to it.
	c.Aux = append(c.Aux, *ref)
	c.refs = append(c.refs, &c.Aux[len(c.Aux)-1])
}

var _ Node = &Zip{}

type Zip struct {
	InputNodes []*ReadEdge
	Output     *WriteEdge
	Ascending  bool
}

func (z *Zip) Description() string {
	return "combine iterators into a single point"
}

func (z *Zip) Cost() Cost {
	return Cost{}
}

func (z *Zip) Inputs() []*ReadEdge   { return z.InputNodes }
func (z *Zip) Outputs() []*WriteEdge { return []*WriteEdge{z.Output} }

func (z *Zip) Execute() error {
	inputs := make([]influxql.Iterator, len(z.InputNodes))
	for i, n := range z.InputNodes {
		input := n.Iterator()
		if input == nil {
			input = influxql.NilIterator
		}
		inputs[i] = input
	}
	z.Output.SetIterator(iterators.Zip(inputs, z.Ascending))
	return nil
}

func (z *Zip) Type() influxql.DataType {
	return influxql.Unknown
}

var _ Node = &Filter{}

type Filter struct {
	Fields    map[string]influxql.IteratorMap
	Condition influxql.Expr
	Input     *ReadEdge
	Output    *WriteEdge
}

func (f *Filter) Description() string {
	return f.Condition.String()
}

func (f *Filter) Cost() Cost {
	return Cost{}
}

func (f *Filter) Inputs() []*ReadEdge   { return []*ReadEdge{f.Input} }
func (f *Filter) Outputs() []*WriteEdge { return []*WriteEdge{f.Output} }

func (f *Filter) Execute() error {
	input := f.Input.Iterator()
	if input == nil {
		f.Output.SetIterator(nil)
		return nil
	}
	f.Output.SetIterator(iterators.Filter(input, f.Fields, f.Condition))
	return nil
}

func (f *Filter) Type() influxql.DataType {
	if n := f.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

var _ Node = &IteratorMapper{}

type IteratorMapper struct {
	AuxiliaryFields *AuxiliaryFields
	Columns         []influxql.VarRef
	Dimensions      *Dimensions
	Input           *ReadEdge
	outputs         []*WriteEdge
	drivers         []influxql.IteratorMap
}

func (m *IteratorMapper) Description() string {
	return "map the results of the subquery"
}

func (m *IteratorMapper) Inputs() []*ReadEdge   { return []*ReadEdge{m.Input} }
func (m *IteratorMapper) Outputs() []*WriteEdge { return m.outputs }

func (m *IteratorMapper) Execute() error {
	input := m.Input.Iterator()
	if input == nil {
		for _, output := range m.outputs {
			output.SetIterator(nil)
		}
		return nil
	}

	// Map the auxiliary fields to drivers.
	var drivers []influxql.IteratorMap
	if m.AuxiliaryFields != nil {
		drivers = make([]influxql.IteratorMap, len(m.AuxiliaryFields.Aux))
	AUXFIELDS:
		for i, ref := range m.AuxiliaryFields.Aux {
			if ref.Type == influxql.Tag {
				drivers[i] = influxql.TagMap(ref.Val)
				continue
			}
			for j, name := range m.Columns {
				if ref.Val == name.Val {
					drivers[i] = influxql.FieldMap(j)
					continue AUXFIELDS
				}
			}
			drivers[i] = influxql.NullMap{}
		}
	}

	itr := iterators.Map(input, drivers, m.Dimensions.Get())
	for i, output := range m.outputs {
		var typ influxql.DataType
		switch d := m.drivers[i].(type) {
		case influxql.FieldMap:
			typ = m.Columns[d].Type
		case influxql.TagMap:
			typ = influxql.Tag
		}
		output.SetIterator(itr.Iterator(m.drivers[i], typ))
	}
	itr.Start()
	return nil
}

func (m *IteratorMapper) Type() influxql.DataType {
	// This iterator does not produce any direct outputs, but instead
	// reads through them and duplexes the results to its outputs.
	// So it functionally has no type for the output since it has no output.
	return influxql.Unknown
}

// Field registers an iterator to read from the field at the index.
func (m *IteratorMapper) Field(index int, out *WriteEdge) {
	m.outputs = append(m.outputs, out)
	m.drivers = append(m.drivers, influxql.FieldMap(index))
	out.Node = m
}

// Tag registers an iterator to read from the tag with the given name.
func (m *IteratorMapper) Tag(name string, out *WriteEdge) {
	m.outputs = append(m.outputs, out)
	m.drivers = append(m.drivers, influxql.TagMap(name))
	out.Node = m
}

// Aux registers an iterator that has no driver and just reads the
// auxiliary fields.
func (m *IteratorMapper) Aux(out *WriteEdge) {
	m.outputs = append(m.outputs, out)
	m.drivers = append(m.drivers, influxql.NullMap{})
	out.Node = m
}

var _ Node = &AuxiliaryField{}

type AuxiliaryField struct {
	Ref    *influxql.VarRef
	Input  *ReadEdge
	Output *WriteEdge
}

func (f *AuxiliaryField) Description() string {
	return f.Ref.String()
}

func (f *AuxiliaryField) Inputs() []*ReadEdge {
	return []*ReadEdge{f.Input}
}

func (f *AuxiliaryField) Outputs() []*WriteEdge {
	return []*WriteEdge{f.Output}
}

func (f *AuxiliaryField) Execute() error {
	f.Output.SetIterator(f.Input.Iterator())
	return nil
}

func (f *AuxiliaryField) Type() influxql.DataType {
	return f.Ref.Type
}

var _ Node = &BinaryExpr{}

type BinaryExpr struct {
	LHS, RHS  *ReadEdge
	Output    *WriteEdge
	Op        influxql.Token
	Fill      influxql.FillOption
	FillValue interface{}
	Ascending bool
	Desc      string
}

func (c *BinaryExpr) Description() string {
	return c.Desc
}

func (c *BinaryExpr) Inputs() []*ReadEdge   { return []*ReadEdge{c.LHS, c.RHS} }
func (c *BinaryExpr) Outputs() []*WriteEdge { return []*WriteEdge{c.Output} }

func (c *BinaryExpr) Execute() error {
	opt := influxql.IteratorOptions{
		Fill:      c.Fill,
		FillValue: c.FillValue,
		Ascending: c.Ascending,
	}
	lhs, rhs := c.LHS.Iterator(), c.RHS.Iterator()
	itr, err := influxql.BuildTransformIterator(lhs, rhs, c.Op, opt)
	if err != nil {
		return err
	}
	c.Output.SetIterator(itr)
	return nil
}

func (c *BinaryExpr) Type() influxql.DataType {
	var lhs, rhs influxql.DataType
	if n := c.LHS.Input.Node; n != nil {
		lhs = n.Type()
	}
	if n := c.RHS.Input.Node; n != nil {
		lhs = n.Type()
	}

	if lhs.LessThan(rhs) {
		return rhs
	} else {
		return lhs
	}
}

var _ Node = &LHSBinaryExpr{}

type LHSBinaryExpr struct {
	LHS    *ReadEdge
	RHS    influxql.Literal
	Output *WriteEdge
	Op     influxql.Token
	Desc   string
}

func (c *LHSBinaryExpr) Description() string {
	return c.Desc
}

func (c *LHSBinaryExpr) Inputs() []*ReadEdge   { return []*ReadEdge{c.LHS} }
func (c *LHSBinaryExpr) Outputs() []*WriteEdge { return []*WriteEdge{c.Output} }

func (c *LHSBinaryExpr) Execute() error {
	lhs := c.LHS.Iterator()
	itr, err := influxql.BuildRHSTransformIterator(lhs, c.RHS, c.Op)
	if err != nil {
		return err
	}
	c.Output.SetIterator(itr)
	return nil
}

func (c *LHSBinaryExpr) Type() influxql.DataType {
	if n := c.LHS.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

var _ Node = &RHSBinaryExpr{}

type RHSBinaryExpr struct {
	LHS    influxql.Literal
	RHS    *ReadEdge
	Output *WriteEdge
	Op     influxql.Token
	Desc   string
}

func (c *RHSBinaryExpr) Description() string {
	return c.Desc
}

func (c *RHSBinaryExpr) Inputs() []*ReadEdge   { return []*ReadEdge{c.RHS} }
func (c *RHSBinaryExpr) Outputs() []*WriteEdge { return []*WriteEdge{c.Output} }

func (c *RHSBinaryExpr) Execute() error {
	rhs := c.RHS.Iterator()
	itr, err := influxql.BuildLHSTransformIterator(c.LHS, rhs, c.Op)
	if err != nil {
		return err
	}
	c.Output.SetIterator(itr)
	return nil
}

func (c *RHSBinaryExpr) Type() influxql.DataType {
	if n := c.RHS.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

var _ Node = &Interval{}

type Interval struct {
	TimeRange TimeRange
	Interval  influxql.Interval
	Location  *time.Location
	Input     *ReadEdge
	Output    *WriteEdge
}

func (i *Interval) Description() string {
	return fmt.Sprintf("normalize time values")
}

func (i *Interval) Inputs() []*ReadEdge   { return []*ReadEdge{i.Input} }
func (i *Interval) Outputs() []*WriteEdge { return []*WriteEdge{i.Output} }

func (i *Interval) Execute() error {
	opt := influxql.IteratorOptions{
		StartTime: i.TimeRange.Min.UnixNano(),
		EndTime:   i.TimeRange.Max.UnixNano(),
		Interval:  i.Interval,
		Location:  i.Location,
	}

	input := i.Input.Iterator()
	if input == nil {
		i.Output.SetIterator(nil)
		return nil
	}
	i.Output.SetIterator(influxql.NewIntervalIterator(input, opt))
	return nil
}

func (i *Interval) Type() influxql.DataType {
	if n := i.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

var _ Node = &Limit{}

type Limit struct {
	Input  *ReadEdge
	Output *WriteEdge
	Limit  int
	Offset int
}

func (c *Limit) Description() string {
	if c.Limit > 0 && c.Offset > 0 {
		return fmt.Sprintf("limit %d/offset %d", c.Limit, c.Offset)
	} else if c.Limit > 0 {
		return fmt.Sprintf("limit %d", c.Limit)
	} else if c.Offset > 0 {
		return fmt.Sprintf("offset %d", c.Offset)
	}
	return "limit 0/offset 0"
}

func (c *Limit) Inputs() []*ReadEdge   { return []*ReadEdge{c.Input} }
func (c *Limit) Outputs() []*WriteEdge { return []*WriteEdge{c.Output} }

func (c *Limit) Execute() error {
	input := c.Input.Iterator()
	if input == nil {
		c.Output.SetIterator(nil)
		return nil
	}

	opt := influxql.IteratorOptions{
		Limit:  c.Limit,
		Offset: c.Offset,
	}
	c.Output.SetIterator(influxql.NewLimitIterator(input, opt))
	return nil
}

func (c *Limit) Type() influxql.DataType {
	if n := c.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

var _ Node = &Fill{}

type Fill struct {
	Input  *ReadEdge
	Output *WriteEdge

	TimeRange TimeRange
	Interval  influxql.Interval
	Ascending bool
	Location  *time.Location
	AuxCount  int
	Option    influxql.FillOption
	Value     interface{}
}

func (f *Fill) Description() string {
	var buf bytes.Buffer
	switch f.Option {
	case influxql.NullFill:
		buf.WriteString("fill(null)")
	case influxql.NumberFill:
		fmt.Fprintf(&buf, "fill(%v)", f.Value)
	case influxql.LinearFill:
		buf.WriteString("fill(linear)")
	case influxql.PreviousFill:
		buf.WriteString("fill(previous)")
	}
	fmt.Fprintf(&buf, " from %s to %s every %s", f.TimeRange.Min, f.TimeRange.Max, influxql.FormatDuration(f.Interval.Duration))
	return buf.String()
}

func (f *Fill) Inputs() []*ReadEdge   { return []*ReadEdge{f.Input} }
func (f *Fill) Outputs() []*WriteEdge { return []*WriteEdge{f.Output} }

func (f *Fill) Execute() error {
	opt := influxql.IteratorOptions{
		Interval:  f.Interval,
		StartTime: f.TimeRange.Min.UnixNano(),
		EndTime:   f.TimeRange.Max.UnixNano(),
		Ascending: f.Ascending,
		Location:  f.Location,
		Fill:      f.Option,
		FillValue: f.Value,
	}
	if f.AuxCount > 0 {
		opt.Aux = make([]influxql.VarRef, f.AuxCount)
	}

	input := f.Input.Iterator()
	if input == nil {
		f.Output.SetIterator(nil)
		return nil
	}

	f.Output.SetIterator(influxql.NewFillIterator(input, nil, opt))
	return nil
}

func (f *Fill) Type() influxql.DataType {
	if n := f.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

var _ Node = &Dedupe{}

type Dedupe struct {
	Fast   bool
	Input  *ReadEdge
	Output *WriteEdge
}

func (d *Dedupe) Description() string {
	return "dedupe points"
}

func (d *Dedupe) Inputs() []*ReadEdge   { return []*ReadEdge{d.Input} }
func (d *Dedupe) Outputs() []*WriteEdge { return []*WriteEdge{d.Output} }

func (d *Dedupe) Execute() error {
	input := d.Input.Iterator()
	if input == nil {
		d.Output.SetIterator(nil)
		return nil
	}

	if d.Fast {
		if input, ok := input.(influxql.FloatIterator); ok {
			d.Output.SetIterator(influxql.NewFloatFastDedupeIterator(input))
		} else {
			d.Output.SetIterator(influxql.NewDedupeIterator(input))
		}
	} else {
		d.Output.SetIterator(influxql.NewDedupeIterator(input))
	}
	return nil
}

func (d *Dedupe) Type() influxql.DataType {
	if n := d.Input.Input.Node; n != nil {
		return n.Type()
	}
	return influxql.Unknown
}

var _ Node = &Nil{}

type Nil struct {
	Output *WriteEdge
}

func (n *Nil) Description() string {
	return "<nil>"
}

func (n *Nil) Inputs() []*ReadEdge   { return nil }
func (n *Nil) Outputs() []*WriteEdge { return []*WriteEdge{n.Output} }

func (n *Nil) Execute() error {
	n.Output.SetIterator(nil)
	return nil
}

func (n *Nil) Type() influxql.DataType {
	return influxql.Unknown
}

// Visitor visits every node in a graph.
type Visitor interface {
	// Visit is called for every node in the graph.
	// If false is returned from the function, the inputs/outputs
	// of the current node are not visited. If an error is returned,
	// processing also stops and the error is returned to Walk/Down.
	Visit(n Node) (ok bool, err error)
}

type VisitorFunc func(n Node) (ok bool, err error)

func (fn VisitorFunc) Visit(n Node) (ok bool, err error) {
	return fn(n)
}

// Walk iterates through all of the nodes going up the tree.
// If a node is referenced as the input from multiple edges, it may be
// visited multiple times.
func Walk(n Node, v Visitor) error {
	if n == nil {
		return nil
	} else if ok, err := v.Visit(n); err != nil {
		return err
	} else if !ok {
		return nil
	}

	for _, input := range n.Inputs() {
		if input.Input.Node != nil {
			if err := Walk(input.Input.Node, v); err != nil {
				return err
			}
		}
	}
	return nil
}
