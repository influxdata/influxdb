package mock

import (
	"regexp"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
)

type TSDBStore struct {
	ShardGroupFn func(ids []uint64) query.ShardGroup
}

func (t *TSDBStore) ShardGroup(ids []uint64) query.ShardGroup {
	return t.ShardGroupFn(ids)
}

type ShardMeta struct {
	Fields     map[string]influxql.DataType
	Dimensions []string
}

type ShardGroup struct {
	Measurements     map[string]ShardMeta
	CreateIteratorFn func(measurement string, opt influxql.IteratorOptions) (influxql.Iterator, error)
}

func (s *ShardGroup) MeasurementsByRegex(re *regexp.Regexp) []string {
	measurements := make([]string, 0, len(s.Measurements))
	for name := range s.Measurements {
		if re.MatchString(name) {
			measurements = append(measurements, name)
		}
	}
	return measurements
}

func (s *ShardGroup) FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	for _, name := range measurements {
		meta := s.Measurements[name]
		for k, typ := range meta.Fields {
			fields[k] = typ
		}
		for _, d := range meta.Dimensions {
			dimensions[d] = struct{}{}
		}
	}
	return fields, dimensions, nil
}

func (s *ShardGroup) MapType(measurement, field string) influxql.DataType {
	meta := s.Measurements[measurement]
	if typ, ok := meta.Fields[field]; ok {
		return typ
	}
	for _, d := range meta.Dimensions {
		if d == field {
			return influxql.Tag
		}
	}
	return influxql.Unknown
}

func (s *ShardGroup) CreateIterator(measurement string, opt influxql.IteratorOptions) (influxql.Iterator, error) {
	if s.CreateIteratorFn != nil {
		return s.CreateIteratorFn(measurement, opt)
	}
	return nil, nil
}
