package tsdb

import (
	"encoding/binary"
	"strings"

	"github.com/influxdata/influxdb/cmd/influx_tsm/tsdb/internal"
	"github.com/influxdata/influxdb/influxql"

	"github.com/gogo/protobuf/proto"
)

// Cursor represents an iterator over a series.
type Cursor interface {
	SeekTo(seek int64) (key int64, value interface{})
	Next() (key int64, value interface{})
}

// Field represents an encoded field.
type Field struct {
	ID   uint8             `json:"id,omitempty"`
	Name string            `json:"name,omitempty"`
	Type influxql.DataType `json:"type,omitempty"`
}

// MeasurementFields is a mapping from measurements to its fields.
type MeasurementFields struct {
	Fields map[string]*Field `json:"fields"`
	Codec  *FieldCodec
}

// MarshalBinary encodes the object to a binary format.
func (m *MeasurementFields) MarshalBinary() ([]byte, error) {
	var pb internal.MeasurementFields
	for _, f := range m.Fields {
		id := int32(f.ID)
		name := f.Name
		t := int32(f.Type)
		pb.Fields = append(pb.Fields, &internal.Field{ID: &id, Name: &name, Type: &t})
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes the object from a binary format.
func (m *MeasurementFields) UnmarshalBinary(buf []byte) error {
	var pb internal.MeasurementFields
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	m.Fields = make(map[string]*Field)
	for _, f := range pb.Fields {
		m.Fields[f.GetName()] = &Field{ID: uint8(f.GetID()), Name: f.GetName(), Type: influxql.DataType(f.GetType())}
	}
	return nil
}

// Series represents a series in the shard.
type Series struct {
	Key  string
	Tags map[string]string
}

// MarshalBinary encodes the object to a binary format.
func (s *Series) MarshalBinary() ([]byte, error) {
	var pb internal.Series
	pb.Key = &s.Key
	for k, v := range s.Tags {
		key := k
		value := v
		pb.Tags = append(pb.Tags, &internal.Tag{Key: &key, Value: &value})
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes the object from a binary format.
func (s *Series) UnmarshalBinary(buf []byte) error {
	var pb internal.Series
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	s.Key = pb.GetKey()
	s.Tags = make(map[string]string)
	for _, t := range pb.Tags {
		s.Tags[t.GetKey()] = t.GetValue()
	}
	return nil
}

// MeasurementFromSeriesKey returns the Measurement name for a given series.
func MeasurementFromSeriesKey(key string) string {
	idx := strings.Index(key, ",")
	if idx == -1 {
		return key
	}
	return key[:strings.Index(key, ",")]
}

// DecodeKeyValue decodes the key and value from bytes.
func DecodeKeyValue(field string, dec *FieldCodec, k, v []byte) (int64, interface{}) {
	// Convert key to a timestamp.
	key := int64(btou64(k[0:8]))

	decValue, err := dec.DecodeByName(field, v)
	if err != nil {
		return key, nil
	}
	return key, decValue
}

// btou64 converts an 8-byte slice into an uint64.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }
