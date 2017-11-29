package tracing

import (
	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/pkg/tracing/wire"
)

// A SpanContext represents the minimal information to identify a span in a trace.
// This is typically serialized to continue a trace on a remote node.
type SpanContext struct {
	TraceID uint64 // TraceID is assigned a random number to this trace.
	SpanID  uint64 // SpanID is assigned a random number to identify this span.
}

func (s SpanContext) MarshalBinary() ([]byte, error) {
	ws := wire.SpanContext(s)
	return proto.Marshal(&ws)
}

func (s *SpanContext) UnmarshalBinary(data []byte) error {
	var ws wire.SpanContext
	err := proto.Unmarshal(data, &ws)
	if err == nil {
		*s = SpanContext(ws)
	}
	return err
}
