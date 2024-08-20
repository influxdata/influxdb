package parquet

import (
	"fmt"
	"strings"

	"github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter/parquet/index"
	tsdb "github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter/parquet/tsm1"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxql"
)

type table struct {
	tagKeys reads.KeyMerger
	fields  reads.KeyMerger
	types   map[string]influxql.DataType
}

func (t *table) addTags(tags models.Tags) {
	t.tagKeys.MergeTagKeys(tags)
}

func (t *table) addField(name []byte, typ influxql.DataType) {
	t.fields.MergeKeys([][]byte{name})
	t.types[string(name)] = typ
}

func (t *table) exporterSchema() (*index.MeasurementSchema, error) {
	tagKeys := t.tagKeys.Get()

	tagSet := make(map[string]struct{}, len(tagKeys))
	for _, key := range tagKeys {
		tagSet[string(key)] = struct{}{}
	}

	fieldKeys := t.fields.Get()

	fieldSet := make(map[index.MeasurementField]struct{}, len(fieldKeys))
	for _, field := range fieldKeys {
		blockType, err := tsdb.BlockTypeForType(t.types[string(field)].Zero())
		if err != nil {
			return nil, err
		}
		measurementField := index.MeasurementField{
			Name: string(field),
			Type: blockType,
		}
		fieldSet[measurementField] = struct{}{}
	}

	return &index.MeasurementSchema{
		TagSet:   tagSet,
		FieldSet: fieldSet,
	}, nil
}

func newTable() *table {
	return &table{
		types: make(map[string]influxql.DataType),
	}
}

type measurements map[string]*table

func newMeasurements() measurements {
	return make(measurements)
}

func (s measurements) getTable(measurement string) *table {
	table, ok := s[measurement]
	if !ok {
		table = newTable()
		s[measurement] = table
	}
	return table
}

func (s measurements) String() string {
	var sb strings.Builder
	for m, t := range s {
		sb.WriteString(fmt.Sprintf("table %s:\n", m))
		schema, _ := t.exporterSchema()
		sb.WriteString(fmt.Sprintf("%v", schema))
	}
	return sb.String()
}
