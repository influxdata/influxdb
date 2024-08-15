package parquet

import (
	"fmt"
	"slices"
	"strings"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

type table struct {
	fields  []string
	tagKeys map[string][]string
	types   map[string]influxql.DataType
}

func (t *table) addTags(name string, tags models.Tags) {
	keys := t.tagKeys[name]
	for _, tag := range tags {
		key := string(tag.Key)
		if !slices.Contains(keys, key) {
			keys = append(keys, key)
		}
	}
	t.tagKeys[name] = keys
}

func (t *table) addField(name string, typ influxql.DataType) {
	if !slices.Contains(t.fields, name) {
		t.fields = append(t.fields, name)
		t.types[name] = typ
	}
}

func newTable() *table {
	return &table{
		tagKeys: make(map[string][]string),
		types:   make(map[string]influxql.DataType),
	}
}

type schema map[string]*table

func newSchema() schema {
	return make(schema)
}

func (s schema) getTable(measurement string) *table {
	table, ok := s[measurement]
	if !ok {
		table = newTable()
		s[measurement] = table
	}
	return table
}

func (s schema) String() string {
	var sb strings.Builder
	sb.WriteString("=====")
	for measurement, table := range s {
		sb.WriteString(fmt.Sprintf("table %s:\n", measurement))
		for _, fn := range table.fields {
			sb.WriteString(fmt.Sprintf("field: %s\n", fn))
			sb.WriteString(fmt.Sprintf("  type: %s\n", table.types[fn]))
			sb.WriteString(fmt.Sprintf("  tag keys: %v\n", table.tagKeys[fn]))
		}
	}
	return sb.String()
}
