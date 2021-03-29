package zap

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	traceHTTPHeader = "Zap-Trace-Span"

	logTraceIDKey     = "ot_trace_id"
	logSpanIDKey      = "ot_span_id"
	logStartKey       = "ot_start"
	logStopKey        = "ot_stop"
	logDurationKey    = "ot_duration"
	logChildOfKey     = "ot_child_of"
	logFollowsFromKey = "ot_follows_from"
)

// Tracer implements opentracing.Tracer and logs each span as its own log.
type Tracer struct {
	log         *zap.Logger
	idGenerator platform2.IDGenerator
}

func NewTracer(log *zap.Logger, idGenerator platform2.IDGenerator) *Tracer {
	return &Tracer{
		log:         log,
		idGenerator: idGenerator,
	}
}

func (t *Tracer) StartSpan(operationName string, opts ...opentracing.StartSpanOption) opentracing.Span {
	startOpts := &opentracing.StartSpanOptions{
		StartTime: time.Now(),
	}
	for _, opt := range opts {
		opt.Apply(startOpts)
	}
	ctx := newSpanContext()
	ctx.spanID = t.idGenerator.ID()
	for _, ref := range startOpts.References {
		refCtx, ok := ref.ReferencedContext.(SpanContext)
		if ok {
			ctx.traceID = refCtx.traceID
			break
		}
	}
	if !ctx.traceID.Valid() {
		ctx.traceID = t.idGenerator.ID()
	}
	return &Span{
		tracer: t,
		opts:   *startOpts,
		opName: operationName,
		tags:   make(map[string]interface{}),
		ctx:    ctx,
	}
}

func (t *Tracer) Inject(sm opentracing.SpanContext, format interface{}, carrier interface{}) error {
	ctx, ok := sm.(SpanContext)
	if !ok {
		return fmt.Errorf("unsupported span context %T", sm)
	}
	switch format {
	case opentracing.Binary:
		w, ok := carrier.(io.Writer)
		if !ok {
			return fmt.Errorf("carrier must be an io.Writer for binary format, got %T", carrier)
		}
		return json.NewEncoder(w).Encode(sm)
	case opentracing.TextMap:
		w, ok := carrier.(opentracing.TextMapWriter)
		if !ok {
			return fmt.Errorf("carrier must be an opentracing.TextMapWriter for text map format, got %T", carrier)
		}
		return injectTextMapWriter(ctx, w)
	case opentracing.HTTPHeaders:
		w, ok := carrier.(opentracing.TextMapWriter)
		if !ok {
			return fmt.Errorf("carrier must be an opentracing.TextMapWriter for http header format, got %T", carrier)
		}
		return injectTextMapWriter(ctx, w)
	default:
		return fmt.Errorf("unsupported format %v", format)
	}
}

func (t *Tracer) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	var err error
	ctx := newSpanContext()
	switch format {
	case opentracing.Binary:
		r, ok := carrier.(io.Reader)
		if !ok {
			return nil, fmt.Errorf("carrier must be an io.Reader for binary format, got %T", carrier)
		}
		err = json.NewDecoder(r).Decode(&ctx)
	case opentracing.TextMap:
		r, ok := carrier.(opentracing.TextMapReader)
		if !ok {
			return nil, fmt.Errorf("carrier must be an opentracing.TextMapReader for text map format, got %T", carrier)
		}
		err = extractTextMapReader(&ctx, r)
	case opentracing.HTTPHeaders:
		r, ok := carrier.(opentracing.TextMapReader)
		if !ok {
			return nil, fmt.Errorf("carrier must be an opentracing.TextMapReader for http header format, got %T", carrier)
		}
		err = extractTextMapReader(&ctx, r)
	default:
		return nil, fmt.Errorf("unsupported format %v", format)
	}
	if !ctx.traceID.Valid() {
		ctx.traceID = t.idGenerator.ID()
	}
	if !ctx.spanID.Valid() {
		return nil, errors.New("no span ID found in carrier")
	}
	return ctx, err
}

func injectTextMapWriter(ctx SpanContext, w opentracing.TextMapWriter) error {
	data, err := json.Marshal(ctx)
	if err != nil {
		return err
	}
	w.Set(traceHTTPHeader, string(data))
	return nil
}

func extractTextMapReader(ctx *SpanContext, r opentracing.TextMapReader) error {
	var data []byte
	r.ForeachKey(func(k, v string) error {
		if http.CanonicalHeaderKey(k) == traceHTTPHeader {
			data = []byte(v)
		}
		return nil
	})
	return json.Unmarshal(data, ctx)
}

// Span implements opentracing.Span, all Spans must be created using  the Tracer.
type Span struct {
	tracer *Tracer
	opts   opentracing.StartSpanOptions
	opName string
	tags   map[string]interface{}
	fields []zapcore.Field
	ctx    SpanContext
}

func (s *Span) Finish() {
	s.FinishWithOptions(opentracing.FinishOptions{})
}

func (s *Span) FinishWithOptions(opts opentracing.FinishOptions) {
	if opts.FinishTime.IsZero() {
		opts.FinishTime = time.Now()
	}
	duration := opts.FinishTime.Sub(s.opts.StartTime)
	fields := append(s.fields,
		zap.String(logTraceIDKey, s.ctx.traceID.String()),
		zap.String(logSpanIDKey, s.ctx.spanID.String()),
		zap.Time(logStartKey, s.opts.StartTime),
		zap.Time(logStopKey, opts.FinishTime),
		zap.Duration(logDurationKey, duration),
	)
	for _, ref := range s.opts.References {
		ctx, ok := ref.ReferencedContext.(SpanContext)
		if !ok {
			continue
		}
		switch ref.Type {
		case opentracing.ChildOfRef:
			fields = append(fields, zap.String(logChildOfKey, ctx.spanID.String()))
		case opentracing.FollowsFromRef:
			fields = append(fields, zap.String(logFollowsFromKey, ctx.spanID.String()))
		}
	}
	for k, v := range s.tags {
		fields = append(fields, zap.Any(k, v))
	}
	for k, v := range s.ctx.baggage {
		fields = append(fields, zap.String(k, v))
	}
	s.tracer.log.Info(s.opName, fields...)
}

func (s *Span) Context() opentracing.SpanContext {
	return s.ctx
}

func (s *Span) SetOperationName(operationName string) opentracing.Span {
	s.opName = operationName
	return s
}

func (s *Span) SetTag(key string, value interface{}) opentracing.Span {
	s.tags[key] = value
	return s
}

func (s *Span) LogFields(fields ...log.Field) {
	for _, field := range fields {
		s.fields = append(s.fields, convertField(field))
	}
}

func convertField(field log.Field) zapcore.Field {
	return zap.Any(field.Key(), field.Value())
}

func (s *Span) LogKV(keyValues ...interface{}) {
	if len(keyValues)%2 != 0 {
		s.LogFields(log.Error(fmt.Errorf("non-even keyValues len: %v", len(keyValues))))
		return
	}
	fields, err := log.InterleavedKVToFields(keyValues...)
	if err != nil {
		s.LogFields(log.Error(err), log.String("function", "LogKV"))
		return
	}
	s.LogFields(fields...)
}

func (s *Span) SetBaggageItem(restrictedKey string, value string) opentracing.Span {
	s.ctx.baggage[restrictedKey] = value
	return s
}

func (s *Span) BaggageItem(restrictedKey string) string {
	return s.ctx.baggage[restrictedKey]
}

func (s *Span) Tracer() opentracing.Tracer {
	return s.tracer
}

// LogEvent is deprecated, as such it is not implemented.
func (s *Span) LogEvent(event string) {
	panic("use of deprecated LogEvent: not implemented")
}

// LogEventWithPayload is deprecated, as such it is not implemented.
func (s *Span) LogEventWithPayload(event string, payload interface{}) {
	panic("use of deprecated LogEventWithPayload: not implemented")
}

// Log is deprecated, as such it is not implemented.
func (s *Span) Log(data opentracing.LogData) {
	panic("use of deprecated Log: not implemented")
}

// SpanContext implements opentracing.SpanContext, all span contexts must be created using the Tracer.
type SpanContext struct {
	traceID platform2.ID
	spanID  platform2.ID
	baggage map[string]string
}

func newSpanContext() SpanContext {
	return SpanContext{
		baggage: make(map[string]string),
	}
}

func (c SpanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	for k, v := range c.baggage {
		if !handler(k, v) {
			return
		}
	}
}

func (c SpanContext) MarshalJSON() ([]byte, error) {
	raw := struct {
		TraceID platform2.ID      `json:"trace_id"`
		SpanID  platform2.ID      `json:"span_id"`
		Baggage map[string]string `json:"baggage"`
	}{
		TraceID: c.traceID,
		SpanID:  c.spanID,
		Baggage: c.baggage,
	}
	return json.Marshal(raw)
}

func (c *SpanContext) UnmarshalJSON(data []byte) error {
	raw := struct {
		TraceID platform2.ID      `json:"trace_id"`
		SpanID  platform2.ID      `json:"span_id"`
		Baggage map[string]string `json:"baggage"`
	}{
		TraceID: c.traceID,
		SpanID:  c.spanID,
		Baggage: c.baggage,
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	c.traceID = raw.TraceID
	c.spanID = raw.SpanID
	c.baggage = raw.Baggage

	return nil
}
