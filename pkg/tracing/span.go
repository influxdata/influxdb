package tracing

import (
	"sync"
	"time"

	"github.com/influxdata/influxdb/pkg/tracing/fields"
	"github.com/influxdata/influxdb/pkg/tracing/labels"
)

// The Span type denotes a specific operation for a Trace.
// A Span may have one or more children, identifying additional
// details about a trace.
type Span struct {
	tracer *Trace
	mu     sync.Mutex
	raw    RawSpan
}

type StartSpanOption interface {
	applyStart(*Span)
}

// The StartTime start span option specifies the start time of
// the new span rather than using now.
type StartTime time.Time

func (t StartTime) applyStart(s *Span) {
	s.raw.Start = time.Time(t)
}

// StartSpan creates a new child span using time.Now as the start time.
func (s *Span) StartSpan(name string, opt ...StartSpanOption) *Span {
	return s.tracer.startSpan(name, s.raw.Context, opt)
}

// Context returns a SpanContext that can be serialized and passed to a remote node to continue a trace.
func (s *Span) Context() SpanContext {
	return s.raw.Context
}

// SetLabels replaces any existing labels for the Span with args.
func (s *Span) SetLabels(args ...string) {
	s.mu.Lock()
	s.raw.Labels = labels.New(args...)
	s.mu.Unlock()
}

// MergeLabels merges args with any existing labels defined
// for the Span.
func (s *Span) MergeLabels(args ...string) {
	ls := labels.New(args...)
	s.mu.Lock()
	s.raw.Labels.Merge(ls)
	s.mu.Unlock()
}

// SetFields replaces any existing fields for the Span with args.
func (s *Span) SetFields(set fields.Fields) {
	s.mu.Lock()
	s.raw.Fields = set
	s.mu.Unlock()
}

// MergeFields merges the provides args with any existing fields defined
// for the Span.
func (s *Span) MergeFields(args ...fields.Field) {
	set := fields.New(args...)
	s.mu.Lock()
	s.raw.Fields.Merge(set)
	s.mu.Unlock()
}

// Finish marks the end of the span and records it to the associated Trace.
// If Finish is not called, the span will not appear in the trace.
func (s *Span) Finish() {
	s.mu.Lock()
	s.tracer.addRawSpan(s.raw)
	s.mu.Unlock()
}

func (s *Span) Tree() *TreeNode {
	return s.tracer.TreeFrom(s.raw.Context.SpanID)
}
