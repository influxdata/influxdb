package tsdb

import (
	"bytes"
	"fmt"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxql"
)

const MaxFieldValueLength = 1048576

// ValidateFields will return a PartialWriteError if:
//   - the point has inconsistent fields, or
//   - the point has fields that are too long
func ValidateFields(mf *MeasurementFields, point models.Point, skipSizeValidation bool) error {
	pointSize := point.StringSize()
	iter := point.FieldIterator()
	for iter.Next() {
		if !skipSizeValidation {
			// Check for size of field too large. Note it is much cheaper to check the whole point size
			// than checking the StringValue size (StringValue potentially takes an allocation if it must
			// unescape the string, and must at least parse the string)
			if pointSize > MaxFieldValueLength && iter.Type() == models.String {
				if sz := len(iter.StringValue()); sz > MaxFieldValueLength {
					return PartialWriteError{
						Reason: fmt.Sprintf(
							"input field \"%s\" on measurement \"%s\" is too long, %d > %d",
							iter.FieldKey(), point.Name(), sz, MaxFieldValueLength),
						Dropped: 1,
					}
				}
			}
		}

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
