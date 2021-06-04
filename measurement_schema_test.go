package influxdb_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2"
	influxerror "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/multierr"
)

func TestMeasurementSchema_Validate(t *testing.T) {
	col1 := func(name string, st influxdb.SemanticColumnType) influxdb.MeasurementSchemaColumn {
		return influxdb.MeasurementSchemaColumn{Name: name, Type: st}
	}
	col2 := func(name string, st influxdb.SemanticColumnType, dt influxdb.SchemaColumnDataType) influxdb.MeasurementSchemaColumn {
		return influxdb.MeasurementSchemaColumn{Name: name, Type: st, DataType: &dt}
	}

	// errp composes a new error from err with prefix p and quoted name
	errp := func(p, name string, err error) error {
		return fmt.Errorf("%s %q: %w", p, name, err)
	}

	type fields struct {
		Name    string
		Columns []influxdb.MeasurementSchemaColumn
	}

	okCols := []influxdb.MeasurementSchemaColumn{
		col1("host", influxdb.SemanticColumnTypeTag),
		col2("usage_user", influxdb.SemanticColumnTypeField, influxdb.SchemaColumnDataTypeFloat),
		col1("time", influxdb.SemanticColumnTypeTimestamp),
	}

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
		errs    []error
	}{
		{
			name: "is valid",
			fields: fields{
				Name:    "cpu",
				Columns: okCols,
			},
		},
		{
			name: "name too short",
			fields: fields{
				Name:    "",
				Columns: okCols,
			},
			errs: []error{errp("name", "", influxdb.ErrMeasurementSchemaNameTooShort)},
		},
		{
			name: "name too long",
			fields: fields{
				Name:    strings.Repeat("f", 129),
				Columns: okCols,
			},
			errs: []error{errp("name", strings.Repeat("f", 129), influxdb.ErrMeasurementSchemaNameTooLong)},
		},
		{
			name: "name starts with underscore",
			fields: fields{
				Name:    "_cpu",
				Columns: okCols,
			},
			errs: []error{errp("name", "_cpu", influxdb.ErrMeasurementSchemaNameUnderscore)},
		},
		{
			name: "name contains non-printable chars",
			fields: fields{
				Name:    "cp\x03u",
				Columns: okCols,
			},
			errs: []error{errp("name", "cp\x03u", &influxerror.Error{
				Code: influxerror.EInvalid,
				Err:  fmt.Errorf("non-printable character"),
			})},
		},
		{
			name: "name contains quotes",
			fields: fields{
				Name:    `"cpu"`,
				Columns: okCols,
			},
			errs: []error{errp("name", `"cpu"`, influxdb.ErrMeasurementSchemaNameQuotes)},
		},

		// Columns validation
		{
			name: "missing columns",
			fields: fields{
				Name:    "cpu",
				Columns: nil,
			},
			errs: []error{influxdb.ErrMeasurementSchemaColumnsMissing},
		},
		{
			name: "time column wrong semantic",
			fields: fields{
				Name: "cpu",
				Columns: []influxdb.MeasurementSchemaColumn{
					col1("host", influxdb.SemanticColumnTypeTag),
					col2("usage_user", influxdb.SemanticColumnTypeField, influxdb.SchemaColumnDataTypeFloat),
					col1("time", influxdb.SemanticColumnTypeField),
				},
			},
			errs: []error{influxdb.ErrMeasurementSchemaColumnsTimeInvalidSemantic},
		},
		{
			name: "time column with data type",
			fields: fields{
				Name: "cpu",
				Columns: []influxdb.MeasurementSchemaColumn{
					col1("host", influxdb.SemanticColumnTypeTag),
					col2("usage_user", influxdb.SemanticColumnTypeField, influxdb.SchemaColumnDataTypeFloat),
					col2("time", influxdb.SemanticColumnTypeTimestamp, influxdb.SchemaColumnDataTypeBoolean),
				},
			},
			errs: []error{influxdb.ErrMeasurementSchemaColumnsTimestampSemanticDataType},
		},
		{
			name: "missing time column",
			fields: fields{
				Name: "cpu",
				Columns: []influxdb.MeasurementSchemaColumn{
					col1("host", influxdb.SemanticColumnTypeTag),
					col2("usage_user", influxdb.SemanticColumnTypeField, influxdb.SchemaColumnDataTypeFloat),
				},
			},
			errs: []error{influxdb.ErrMeasurementSchemaColumnsMissingTime},
		},
		{
			name: "timestamp column that is not named time",
			fields: fields{
				Name: "cpu",
				Columns: []influxdb.MeasurementSchemaColumn{
					col1("host", influxdb.SemanticColumnTypeTag),
					col2("usage_user", influxdb.SemanticColumnTypeField, influxdb.SchemaColumnDataTypeFloat),
					col1("foo", influxdb.SemanticColumnTypeTimestamp),
					col1("time", influxdb.SemanticColumnTypeTimestamp),
				},
			},
			errs: []error{influxdb.ErrMeasurementSchemaColumnsTimestampSemanticInvalidName},
		},
		{
			name: "tag contains data type",
			fields: fields{
				Name: "cpu",
				Columns: []influxdb.MeasurementSchemaColumn{
					col2("host", influxdb.SemanticColumnTypeTag, influxdb.SchemaColumnDataTypeString),
					col2("usage_user", influxdb.SemanticColumnTypeField, influxdb.SchemaColumnDataTypeFloat),
					col1("time", influxdb.SemanticColumnTypeTimestamp),
				},
			},
			errs: []error{influxdb.ErrMeasurementSchemaColumnsTagSemanticDataType},
		},
		{
			name: "field missing data type",
			fields: fields{
				Name: "cpu",
				Columns: []influxdb.MeasurementSchemaColumn{
					col1("host", influxdb.SemanticColumnTypeTag),
					col1("usage_user", influxdb.SemanticColumnTypeField),
					col1("time", influxdb.SemanticColumnTypeTimestamp),
				},
			},
			errs: []error{influxdb.ErrMeasurementSchemaColumnsFieldSemanticMissingDataType},
		},
		{
			name: "missing fields",
			fields: fields{
				Name: "cpu",
				Columns: []influxdb.MeasurementSchemaColumn{
					col1("host", influxdb.SemanticColumnTypeTag),
					col1("region", influxdb.SemanticColumnTypeTag),
					col1("time", influxdb.SemanticColumnTypeTimestamp),
				},
			},
			errs: []error{influxdb.ErrMeasurementSchemaColumnsMissingFields},
		},
		{
			name: "duplicate column names",
			fields: fields{
				Name: "cpu",
				Columns: []influxdb.MeasurementSchemaColumn{
					col1("host", influxdb.SemanticColumnTypeTag),
					col2("host", influxdb.SemanticColumnTypeField, influxdb.SchemaColumnDataTypeFloat),
					col1("time", influxdb.SemanticColumnTypeTimestamp),
				},
			},
			errs: []error{influxdb.ErrMeasurementSchemaColumnsDuplicateNames},
		},
		{
			name: "duplicate column case insensitive names",
			fields: fields{
				Name: "cpu",
				Columns: []influxdb.MeasurementSchemaColumn{
					col1("host", influxdb.SemanticColumnTypeTag),
					col2("HOST", influxdb.SemanticColumnTypeField, influxdb.SchemaColumnDataTypeFloat),
					col1("time", influxdb.SemanticColumnTypeTimestamp),
				},
			},
			errs: []error{influxdb.ErrMeasurementSchemaColumnsDuplicateNames},
		},

		// column name validation
		{
			name: "column name too short",
			fields: fields{
				Name: "cpu",
				Columns: []influxdb.MeasurementSchemaColumn{
					col1("", influxdb.SemanticColumnTypeTag),
					col2("usage_user", influxdb.SemanticColumnTypeField, influxdb.SchemaColumnDataTypeFloat),
					col1("time", influxdb.SemanticColumnTypeTimestamp),
				},
			},
			errs: []error{errp("column name", "", influxdb.ErrMeasurementSchemaNameTooShort)},
		},
		{
			name: "column name too long",
			fields: fields{
				Name: "cpu",
				Columns: []influxdb.MeasurementSchemaColumn{
					col1(strings.Repeat("f", 129), influxdb.SemanticColumnTypeTag),
					col2("usage_user", influxdb.SemanticColumnTypeField, influxdb.SchemaColumnDataTypeFloat),
					col1("time", influxdb.SemanticColumnTypeTimestamp),
				},
			},
			errs: []error{errp("column name", strings.Repeat("f", 129), influxdb.ErrMeasurementSchemaNameTooLong)},
		},
		{
			name: "column name starts with underscore",
			fields: fields{
				Name: "cpu",
				Columns: []influxdb.MeasurementSchemaColumn{
					col1("_host", influxdb.SemanticColumnTypeTag),
					col2("usage_user", influxdb.SemanticColumnTypeField, influxdb.SchemaColumnDataTypeFloat),
					col1("time", influxdb.SemanticColumnTypeTimestamp),
				},
			},
			errs: []error{errp("column name", "_host", influxdb.ErrMeasurementSchemaNameUnderscore)},
		},
		{
			name: "column name contains quotes",
			fields: fields{
				Name: "cpu",
				Columns: []influxdb.MeasurementSchemaColumn{
					col1(`"host"`, influxdb.SemanticColumnTypeTag),
					col2("usage_user", influxdb.SemanticColumnTypeField, influxdb.SchemaColumnDataTypeFloat),
					col1("time", influxdb.SemanticColumnTypeTimestamp),
				},
			},
			errs: []error{errp("column name", `"host"`, influxdb.ErrMeasurementSchemaNameQuotes)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &influxdb.MeasurementSchema{
				Name:    tt.fields.Name,
				Columns: tt.fields.Columns,
			}

			if gotErr := m.Validate(); len(tt.errs) > 0 {
				gotErrs := multierr.Errors(gotErr)
				assert.ElementsMatch(t, gotErrs, tt.errs)
			} else {
				assert.NoError(t, gotErr)
			}

		})
	}
}
