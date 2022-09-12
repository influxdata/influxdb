package logger

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/pkg/snowflake"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// OperationNameKey is the logging context key used for identifying name of an operation.
	OperationNameKey = "op_name"

	// OperationEventKey is the logging context key used for identifying a notable
	// event during the course of an operation.
	OperationEventKey = "op_event"

	// OperationElapsedKey is the logging context key used for identifying time elapsed to finish an operation.
	OperationElapsedKey = "op_elapsed"

	// DBInstanceKey is the logging context key used for identifying name of the relevant database.
	DBInstanceKey = "db_instance"

	// DBRetentionKey is the logging context key used for identifying name of the relevant retention policy.
	DBRetentionKey = "db_rp"

	// DBShardGroupKey is the logging context key used for identifying relevant shard group.
	DBShardGroupKey = "db_shard_group"

	// DBShardIDKey is the logging context key used for identifying name of the relevant shard number.
	DBShardIDKey = "db_shard_id"

	// TraceIDKey is the logging context key used for identifying the current trace.
	TraceIDKey = "ot_trace_id"

	// TraceSampledKey is the logging context key used for determining whether the current trace will be sampled.
	TraceSampledKey = "ot_trace_sampled"
)
const (
	eventStart = "start"
	eventEnd   = "end"
)

var (
	gen = snowflake.New(0)
)

func nextID() string {
	return gen.NextString()
}

// OperationName returns a field for tracking the name of an operation.
func OperationName(name string) zapcore.Field {
	return zap.String(OperationNameKey, name)
}

// OperationElapsed returns a field for tracking the duration of an operation.
func OperationElapsed(d time.Duration) zapcore.Field {
	return zap.Duration(OperationElapsedKey, d)
}

// OperationEventStart returns a field for tracking the start of an operation.
func OperationEventStart() zapcore.Field {
	return zap.String(OperationEventKey, eventStart)
}

// OperationEventEnd returns a field for tracking the end of an operation.
func OperationEventEnd() zapcore.Field {
	return zap.String(OperationEventKey, eventEnd)
}

// Database returns a field for tracking the name of a database.
func Database(name string) zapcore.Field {
	return zap.String(DBInstanceKey, name)
}

// Database returns a field for tracking the name of a database.
func RetentionPolicy(name string) zapcore.Field {
	return zap.String(DBRetentionKey, name)
}

// ShardGroup returns a field for tracking the shard group identifier.
func ShardGroup(id uint64) zapcore.Field {
	return zap.Uint64(DBShardGroupKey, id)
}

// Shard returns a field for tracking the shard identifier.
func Shard(id uint64) zapcore.Field {
	return zap.Uint64(DBShardIDKey, id)
}

// TraceFields returns a fields "ot_trace_id" and "ot_trace_sampled", values pulled from the (Jaeger) trace ID
// found in the given context. Returns nil if the context doesn't have a trace ID.
func TraceFields(ctx context.Context) []zap.Field {
	id, sampled, found := tracing.InfoFromContext(ctx)
	if !found {
		return nil
	}
	return []zap.Field{zap.String(TraceIDKey, id), zap.Bool(TraceSampledKey, sampled)}
}

// TraceID returns a field "trace_id", value pulled from the (Jaeger) trace ID found in the given context.
// Returns zap.Skip() if the context doesn't have a trace ID.
func TraceID(ctx context.Context) zap.Field {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		if spanContext, ok := span.Context().(jaeger.SpanContext); ok {
			return zap.String("trace_id", spanContext.TraceID().String())
		}
	}
	return zap.Skip()
}

// NewOperation uses the exiting log to create a new logger with context
// containing a trace id and the operation. Prior to returning, a standardized message
// is logged indicating the operation has started. The returned function should be
// called when the operation concludes in order to log a corresponding message which
// includes an elapsed time and that the operation has ended.
func NewOperation(ctx context.Context, log *zap.Logger, msg, name string, fields ...zapcore.Field) (*zap.Logger, func()) {
	f := []zapcore.Field{OperationName(name), TraceID(ctx)}
	if len(fields) > 0 {
		f = append(f, fields...)
	}

	now := time.Now()
	log = log.With(f...)
	log.Info(msg+" (start)", OperationEventStart())

	return log, func() { log.Info(msg+" (end)", OperationEventEnd(), OperationElapsed(time.Since(now))) }
}
