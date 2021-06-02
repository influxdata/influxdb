package pkger

import (
	"fmt"
	"sort"
	"strings"

	"github.com/influxdata/influxdb/v2"
)

const (
	fieldBucketSchemaType   = "schemaType"
	fieldMeasurementSchemas = "measurementSchemas"

	// measurementSchema fields
	fieldMeasurementSchemaName    = "name"
	fieldMeasurementSchemaColumns = "columns"

	// measurementColumn fields
	fieldMeasurementColumnName     = "name"
	fieldMeasurementColumnType     = "type"
	fieldMeasurementColumnDataType = "dataType"
)

type measurementSchemas []measurementSchema

func (s measurementSchemas) valid() []validationErr {
	var errs []validationErr

	for idx, ms := range s {
		if nestedErrs := ms.valid(); len(nestedErrs) > 0 {
			errs = append(errs, validationErr{
				Field:  fieldMeasurementSchemas,
				Index:  intPtr(idx),
				Nested: nestedErrs,
			})
		}
	}

	return errs
}

func (s measurementSchema) valid() []validationErr {
	var errs []validationErr

	if err := influxdb.ValidateMeasurementSchemaName(s.Name); err != nil {
		errs = append(errs, validationErr{
			Field: fieldMeasurementSchemaName,
			Msg:   err.Error(),
		})
	}

	// validate columns
	timeCount := 0
	fieldCount := 0
	names := make([]string, 0, len(s.Columns))

	columnErrors := make([]validationErr, len(s.Columns))

	for idx, col := range s.Columns {
		colErr := &columnErrors[idx]
		*colErr = validationErr{
			Field: fieldMeasurementSchemaColumns,
			Index: intPtr(idx),
		}

		names = append(names, col.Name)

		if err := influxdb.ValidateMeasurementSchemaName(col.Name); err != nil {
			colErr.Nested = append(colErr.Nested, validationErr{
				Field: fieldMeasurementColumnName,
				Msg:   err.Error(),
			})
		}

		colType := influxdb.SemanticColumnTypeFromString(col.Type)
		if colType == nil {
			colErr.Nested = append(colErr.Nested, validationErr{
				Field: fieldMeasurementColumnType,
				Msg:   "missing type",
			})
			continue
		}

		colDataType := influxdb.SchemaColumnDataTypeFromString(col.DataType)

		// all columns require a type field
		if col.Name == "time" {
			timeCount++
			if *colType != influxdb.SemanticColumnTypeTimestamp {
				colErr.Nested = append(colErr.Nested, validationErr{
					Field: fieldMeasurementColumnType,
					Msg:   "\"time\" column type must be timestamp",
				})
			}

			if colDataType != nil {
				colErr.Nested = append(colErr.Nested, validationErr{
					Field: fieldMeasurementColumnDataType,
					Msg:   "unexpected dataType for time column",
				})
			}
		}

		// ensure no other columns have a timestamp semantic
		switch *colType {
		case influxdb.SemanticColumnTypeTimestamp:
			if col.Name != "time" {
				colErr.Nested = append(colErr.Nested, validationErr{
					Field: fieldMeasurementColumnName,
					Msg:   "timestamp column must be named \"time\"",
				})
			}

		case influxdb.SemanticColumnTypeTag:
			// ensure tag columns don't include a data type value
			if colDataType != nil {
				colErr.Nested = append(colErr.Nested, validationErr{
					Field: fieldMeasurementColumnDataType,
					Msg:   "unexpected dataType for tag column",
				})
			}

		case influxdb.SemanticColumnTypeField:
			if colDataType == nil {
				colErr.Nested = append(colErr.Nested, validationErr{
					Field: fieldMeasurementColumnDataType,
					Msg:   "missing or invalid data type for field column",
				})
			}
			fieldCount++
		}
	}

	// collect only those column errors with nested errors
	for _, colErr := range columnErrors {
		if len(colErr.Nested) > 0 {
			errs = append(errs, colErr)
		}
	}

	if timeCount == 0 {
		errs = append(errs, validationErr{
			Field: fieldMeasurementSchemaColumns,
			Msg:   "missing \"time\" column",
		})
	}

	// ensure there is at least one field defined
	if fieldCount == 0 {
		errs = append(errs, validationErr{
			Field: fieldMeasurementSchemaColumns,
			Msg:   "at least one field column is required",
		})
	}

	// check for duplicate columns using general UTF-8 case insensitive comparison
	sort.Strings(names)
	for i := 0; i < len(names)-1; i++ {
		if strings.EqualFold(names[i], names[i+1]) {
			errs = append(errs, validationErr{
				Field: fieldMeasurementSchemaColumns,
				Msg:   fmt.Sprintf("duplicate columns with name %q", names[i]),
			})
		}
	}

	return errs
}

func (s measurementSchemas) summarize() []SummaryMeasurementSchema {
	if len(s) == 0 {
		return nil
	}

	schemas := make([]SummaryMeasurementSchema, 0, len(s))
	for _, schema := range s {
		schemas = append(schemas, schema.summarize())
	}

	// Measurements are in Name order for consistent output in summaries
	sort.Slice(schemas, func(i, j int) bool {
		return schemas[i].Name < schemas[j].Name
	})

	return schemas
}

type measurementSchema struct {
	Name    string              `json:"name" yaml:"name"`
	Columns []measurementColumn `json:"columns" yaml:"columns"`
}

func (s measurementSchema) summarize() SummaryMeasurementSchema {
	var cols []SummaryMeasurementSchemaColumn
	if len(s.Columns) > 0 {
		cols = make([]SummaryMeasurementSchemaColumn, 0, len(s.Columns))
		for i := range s.Columns {
			cols = append(cols, s.Columns[i].summarize())
		}

		// Columns are in Name order for consistent output in summaries
		sort.Slice(cols, func(i, j int) bool {
			return cols[i].Name < cols[j].Name
		})
	}

	return SummaryMeasurementSchema{Name: s.Name, Columns: cols}
}

type measurementColumn struct {
	Name     string `json:"name" yaml:"name"`
	Type     string `json:"type" yaml:"type"`
	DataType string `json:"dataType,omitempty" yaml:"dataType,omitempty"`
}

func (c measurementColumn) summarize() SummaryMeasurementSchemaColumn {
	return SummaryMeasurementSchemaColumn(c)
}
