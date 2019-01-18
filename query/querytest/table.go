package querytest

import (
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
)

type Table struct {
	Measurement string
	Field       string
	ValueType   flux.ColType
	TagLabels   []string
	TagValues   []string
	Rows        []*Row
}

func (t *Table) Check() error {
	if len(t.TagValues) != len(t.TagLabels) {
		return fmt.Errorf("tag values and labels must have same length")
	}
	return nil
}

type Row struct {
	time  execute.Time
	value interface{}
}

func R(time execute.Time, value interface{}) *Row {
	return &Row{time: time, value: value}
}
