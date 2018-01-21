package tracing

import (
	"sort"
	"sync"
	"time"
)

// The Trace type functions as a container for capturing Spans used to
// trace the execution of a request.
type Trace struct {
	mu    sync.Mutex
	spans map[uint64]RawSpan
}

// NewTrace starts a new trace and returns a root span identified by the provided name.
//
// Additional options may be specified to override the default behavior when creating the span.
func NewTrace(name string, opt ...StartSpanOption) (*Trace, *Span) {
	t := &Trace{spans: make(map[uint64]RawSpan)}
	s := &Span{tracer: t}
	s.raw.Name = name
	s.raw.Context.TraceID, s.raw.Context.SpanID = randomID2()
	setOptions(s, opt)

	return t, s
}

// NewTraceFromSpan starts a new trace and returns the associated span, which is a child of the
// parent span context.
func NewTraceFromSpan(name string, parent SpanContext, opt ...StartSpanOption) (*Trace, *Span) {
	t := &Trace{spans: make(map[uint64]RawSpan)}
	s := &Span{tracer: t}
	s.raw.Name = name
	s.raw.ParentSpanID = parent.SpanID
	s.raw.Context.TraceID = parent.TraceID
	s.raw.Context.SpanID = randomID()
	setOptions(s, opt)

	return t, s
}

func (t *Trace) startSpan(name string, sc SpanContext, opt []StartSpanOption) *Span {
	s := &Span{tracer: t}
	s.raw.Name = name
	s.raw.Context.SpanID = randomID()
	s.raw.Context.TraceID = sc.TraceID
	s.raw.ParentSpanID = sc.SpanID
	setOptions(s, opt)

	return s
}

func setOptions(s *Span, opt []StartSpanOption) {
	for _, o := range opt {
		o.applyStart(s)
	}

	if s.raw.Start.IsZero() {
		s.raw.Start = time.Now()
	}
}

func (t *Trace) addRawSpan(raw RawSpan) {
	t.mu.Lock()
	t.spans[raw.Context.SpanID] = raw
	t.mu.Unlock()
}

// Tree returns a graph of the current trace.
func (t *Trace) Tree() *TreeNode {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, s := range t.spans {
		if s.ParentSpanID == 0 {
			return t.treeFrom(s.Context.SpanID)
		}
	}
	return nil
}

// Merge combines other with the current trace. This is
// typically necessary when traces are transferred from a remote.
func (t *Trace) Merge(other *Trace) {
	for k, s := range other.spans {
		t.spans[k] = s
	}
}

func (t *Trace) TreeFrom(root uint64) *TreeNode {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.treeFrom(root)
}

func (t *Trace) treeFrom(root uint64) *TreeNode {
	c := map[uint64]*TreeNode{}

	for k, s := range t.spans {
		c[k] = &TreeNode{Raw: s}
	}

	if _, ok := c[root]; !ok {
		return nil
	}

	for _, n := range c {
		if n.Raw.ParentSpanID != 0 {
			if pn := c[n.Raw.ParentSpanID]; pn != nil {
				pn.Children = append(pn.Children, n)
			}
		}
	}

	// sort nodes
	var v treeSortVisitor
	Walk(&v, c[root])

	return c[root]
}

type treeSortVisitor struct{}

func (v *treeSortVisitor) Visit(node *TreeNode) Visitor {
	sort.Slice(node.Children, func(i, j int) bool {
		lt, rt := node.Children[i].Raw.Start.UnixNano(), node.Children[j].Raw.Start.UnixNano()
		if lt < rt {
			return true
		} else if lt > rt {
			return false
		}

		ln, rn := node.Children[i].Raw.Name, node.Children[j].Raw.Name
		return ln < rn
	})
	return v
}
