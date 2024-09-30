package index

import (
	tsdb "github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/tsm1"
)

type (
	MeasurementField struct {
		Name string         `json:"name"`
		Type tsdb.BlockType `json:"type"`
	}

	MeasurementSchema struct {
		TagSet   map[string]struct{}
		FieldSet map[MeasurementField]struct{}
	}

	MeasurementKey = string

	MeasurementSchemas map[MeasurementKey]*MeasurementSchema
)
