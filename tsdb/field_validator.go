package tsdb

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

const MaxFieldValueLength = 1048576

// ValidateAndCreateFields will return a PartialWriteError if:
//   - the point has inconsistent fields, or
//   - the point has fields that are too long
func ValidateAndCreateFields(mf *MeasurementFields, point models.Point, skipSizeValidation bool) ([]*FieldCreate, *PartialWriteError) {
	pointSize := point.StringSize()
	iter := point.FieldIterator()
	var fieldsToCreate []*FieldCreate

	// We return fieldsToCreate even on error, because other writes
	// in parallel may depend on these previous fields having been
	// created in memory
	for iter.Next() {
		if !skipSizeValidation {
			// Check for size of field too large. Note it is much cheaper to check the whole point size
			// than checking the StringValue size (StringValue potentially takes an allocation if it must
			// unescape the string, and must at least parse the string)
			if pointSize > MaxFieldValueLength && iter.Type() == models.String {
				if sz := len(iter.StringValue()); sz > MaxFieldValueLength {
					return fieldsToCreate, &PartialWriteError{
						Reason: fmt.Sprintf(
							"input field %q on measurement %q is too long, %d > %d",
							iter.FieldKey(), point.Name(), sz, MaxFieldValueLength),
						Dropped: 1,
					}
				}
			}
		}

		fieldKey := iter.FieldKey()
		// Skip fields name "time", they are illegal.
		if bytes.Equal(fieldKey, timeBytes) {
			continue
		}

		dataType := dataTypeFromModelsFieldType(iter.Type())
		if dataType == influxql.Unknown {
			continue
		}

		fieldName := string(fieldKey)
		f, created, err := mf.CreateFieldIfNotExists(fieldName, dataType)
		if errors.Is(err, ErrFieldTypeConflict) {
			return fieldsToCreate, &PartialWriteError{
				Reason: fmt.Sprintf(
					"%s: input field %q on measurement %q is type %s, already exists as type %s",
					err, fieldName, point.Name(), dataType, f.Type),
				Dropped: 1,
			}
		} else if err != nil {
			return fieldsToCreate, &PartialWriteError{
				Reason: fmt.Sprintf(
					"error adding field %q to measurement %q: %s",
					fieldName, point.Name(), err),
				Dropped: 1,
			}
		} else if created {
			fieldsToCreate = append(fieldsToCreate, &FieldCreate{point.Name(), f})
		}
	}
	return fieldsToCreate, nil
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
