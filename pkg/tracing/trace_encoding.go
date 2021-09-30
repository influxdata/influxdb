package tracing

import (
	"math"
	"time"

	"github.com/influxdata/influxdb/pkg/tracing/fields"
	"github.com/influxdata/influxdb/pkg/tracing/labels"
	"github.com/influxdata/influxdb/pkg/tracing/wire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func fieldsToWire(set fields.Fields) []*wire.Field {
	var r []*wire.Field
	for _, f := range set {
		wf := wire.Field{Key: f.Key()}
		switch val := f.Value().(type) {
		case string:
			wf.FieldType = wire.FieldType_FieldTypeString
			wf.Value = &wire.Field_StringVal{StringVal: val}

		case bool:
			var numericVal int64
			if val {
				numericVal = 1
			}
			wf.FieldType = wire.FieldType_FieldTypeBool
			wf.Value = &wire.Field_NumericVal{NumericVal: numericVal}

		case int64:
			wf.FieldType = wire.FieldType_FieldTypeInt64
			wf.Value = &wire.Field_NumericVal{NumericVal: val}

		case uint64:
			wf.FieldType = wire.FieldType_FieldTypeUint64
			wf.Value = &wire.Field_NumericVal{NumericVal: int64(val)}

		case time.Duration:
			wf.FieldType = wire.FieldType_FieldTypeDuration
			wf.Value = &wire.Field_NumericVal{NumericVal: int64(val)}

		case float64:
			wf.FieldType = wire.FieldType_FieldTypeFloat64
			wf.Value = &wire.Field_NumericVal{NumericVal: int64(math.Float64bits(val))}

		default:
			continue
		}

		r = append(r, &wf)
	}
	return r
}

func labelsToWire(set labels.Labels) []string {
	var r []string
	for i := range set {
		r = append(r, set[i].Key, set[i].Value)
	}
	return r
}

func (t *Trace) MarshalBinary() ([]byte, error) {
	wt := wire.Trace{}
	for _, sp := range t.spans {
		wt.Spans = append(wt.Spans, &wire.Span{
			Context: &wire.SpanContext{
				TraceID: sp.Context.TraceID,
				SpanID:  sp.Context.SpanID,
			},
			ParentSpanID: sp.ParentSpanID,
			Name:         sp.Name,
			Start:        timestamppb.New(sp.Start),
			Labels:       labelsToWire(sp.Labels),
			Fields:       fieldsToWire(sp.Fields),
		})
	}

	return proto.Marshal(&wt)
}

func wireToFields(wfs []*wire.Field) fields.Fields {
	var fs []fields.Field
	for _, wf := range wfs {
		switch wf.FieldType {
		case wire.FieldType_FieldTypeString:
			fs = append(fs, fields.String(wf.Key, wf.GetStringVal()))

		case wire.FieldType_FieldTypeBool:
			var boolVal bool
			if wf.GetNumericVal() != 0 {
				boolVal = true
			}
			fs = append(fs, fields.Bool(wf.Key, boolVal))

		case wire.FieldType_FieldTypeInt64:
			fs = append(fs, fields.Int64(wf.Key, wf.GetNumericVal()))

		case wire.FieldType_FieldTypeUint64:
			fs = append(fs, fields.Uint64(wf.Key, uint64(wf.GetNumericVal())))

		case wire.FieldType_FieldTypeDuration:
			fs = append(fs, fields.Duration(wf.Key, time.Duration(wf.GetNumericVal())))

		case wire.FieldType_FieldTypeFloat64:
			fs = append(fs, fields.Float64(wf.Key, math.Float64frombits(uint64(wf.GetNumericVal()))))
		}
	}

	return fields.New(fs...)
}

func (t *Trace) UnmarshalBinary(data []byte) error {
	var wt wire.Trace
	if err := proto.Unmarshal(data, &wt); err != nil {
		return err
	}

	t.spans = make(map[uint64]RawSpan)

	for _, sp := range wt.Spans {
		t.spans[sp.Context.SpanID] = RawSpan{
			Context: SpanContext{
				TraceID: sp.Context.TraceID,
				SpanID:  sp.Context.SpanID,
			},
			ParentSpanID: sp.ParentSpanID,
			Name:         sp.Name,
			Start:        sp.Start.AsTime(),
			Labels:       labels.New(sp.Labels...),
			Fields:       wireToFields(sp.Fields),
		}
	}

	return nil
}
