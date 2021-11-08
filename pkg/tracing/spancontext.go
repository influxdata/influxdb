package tracing

import (
	"github.com/influxdata/influxdb/v2/pkg/tracing/wire"
	"google.golang.org/protobuf/proto"
)

// A SpanContext represents the minimal information to identify a span in a trace.
// This is typically serialized to continue a trace on a remote node.
type SpanContext struct {
	TraceID uint64 // TraceID is assigned a random number to this trace.
	SpanID  uint64 // SpanID is assigned a random number to identify this span.
}

func (s SpanContext) MarshalBinary() ([]byte, error) {
	return proto.Marshal(&wire.SpanContext{
		TraceID: s.TraceID,
		SpanID:  s.SpanID,
	})
}

func (s *SpanContext) UnmarshalBinary(data []byte) error {
	var ws wire.SpanContext
	err := proto.Unmarshal(data, &ws)
	if err == nil {
		*s = SpanContext{
			TraceID: ws.TraceID,
			SpanID:  ws.SpanID,
		}
	}
	return err
}
