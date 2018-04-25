package tsdb

import (
	"bytes"
	"fmt"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

// FieldValidator should return a PartialWriteError if the point should not be written.
type FieldValidator interface {
	Validate(mf *MeasurementFields, point models.Point) error
}

// defaultFieldValidator ensures that points do not use different types for fields that already exist.
type defaultFieldValidator struct{}

// Validate will return a PartialWriteError if the point has inconsistent fields.
func (defaultFieldValidator) Validate(mf *MeasurementFields, point models.Point) error {
	iter := point.FieldIterator()
	for iter.Next() {
		// Skip fields name "time", they are illegal.
		if bytes.Equal(iter.FieldKey(), timeBytes) {
			continue
		}

		// If the fields is not present, there cannot be a conflict.
		f := mf.FieldBytes(iter.FieldKey())
		if f == nil {
			continue
		}

		dataType := dataTypeFromModelsFieldType(iter.Type())
		if dataType == influxql.Unknown {
			continue
		}

		// If the types are not the same, there is a conflict.
		if f.Type != dataType {
			return PartialWriteError{
				Reason: fmt.Sprintf(
					"%s: input field \"%s\" on measurement \"%s\" is type %s, already exists as type %s",
					ErrFieldTypeConflict, iter.FieldKey(), point.Name(), dataType, f.Type),
				Dropped: 1,
			}
		}
	}

	return nil
}

// dataTypeFromModelsFieldType returns the influxql.DataType that corresponds to the
// passed in field type. If there is no good match, it returns Unknown.
func dataTypeFromModelsFieldType(fieldType models.FieldType) influxql.DataType {
	switch fieldType {
	case models.Float:
		return influxql.Float
	case models.Integer:
		return influxql.Integer
	case models.Unsigned:
		return influxql.Unsigned
	case models.Boolean:
		return influxql.Boolean
	case models.String:
		return influxql.String
	default:
		return influxql.Unknown
	}
}
