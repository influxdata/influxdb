package parquet

import (
	"fmt"
	"slices"
	"strings"

	"github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter/parquet/index"
	models2 "github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter/parquet/models"
	tsdb "github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter/parquet/tsm1"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

// table represents measurement in v1 db
type table struct {
	fields []models2.EscapedString
	tagKeys/*map[models2.EscapedString]*/ []models2.EscapedString
	types map[models2.EscapedString]influxql.DataType
}

func (t *table) addTags(name models2.EscapedString, tags models.Tags) {
	keys := t.tagKeys /*[name]*/
	for _, tag := range tags {
		key := models2.MakeEscaped(tag.Key).S()
		if !slices.Contains(keys, key) {
			keys = append(keys, key)
		}
	}
	t.tagKeys /*[name]*/ = keys
}

func (t *table) addField(name models2.EscapedString, typ influxql.DataType) {
	if !slices.Contains(t.fields, name) {
		t.fields = append(t.fields, name)
		t.types[name] = typ
	}
}

func newTable() *table {
	return &table{
		//tagKeys: make(map[models2.EscapedString][]models2.EscapedString),
		types: make(map[models2.EscapedString]influxql.DataType),
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

func (s measurements) exporterSchema(measurement string) (*index.MeasurementSchema, error) {
	table := s[measurement]

	tagSet := make(map[string]struct{}, len(table.tagKeys))
	for _, key := range table.tagKeys {
		tagSet[string(key)] = struct{}{}
	}

	fieldSet := make(map[index.MeasurementField]struct{}, len(table.fields))
	for _, field := range table.fields {
		blockType, err := tsdb.BlockTypeForType(table.types[field].Zero())
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

func (s measurements) String() string {
	var sb strings.Builder
	for measurement, table := range s {
		sb.WriteString(fmt.Sprintf("[%s]:\n", measurement))
		sb.WriteString(fmt.Sprintf("  tags: %v\n", table.tagKeys /*[fn]*/))
		sb.WriteString("  fields:\n")
		for _, fn := range table.fields {
			sb.WriteString(fmt.Sprintf("    %s [%s]\n", fn, table.types[fn]))
		}
	}
	return sb.String()
}
