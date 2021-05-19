package influxdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	influxid "github.com/influxdata/influxdb/v2/kit/platform"
	influxerror "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/models"
	"go.uber.org/multierr"
)

// SchemaType differentiates the supported schema for a bucket.
type SchemaType int

const (
	SchemaTypeImplicit SchemaType = iota // SchemaTypeImplicit specifies the bucket has an implicit schema.
	SchemaTypeExplicit                   // SchemaTypeExplicit specifies the bucket has an explicit schema.
)

// SchemaTypeFromString returns the SchemaType for s
// or nil if none exists.
func SchemaTypeFromString(s string) *SchemaType {
	switch s {
	case "implicit":
		return SchemaTypeImplicit.Ptr()
	case "explicit":
		return SchemaTypeExplicit.Ptr()
	default:
		return nil
	}
}

func (s *SchemaType) String() string {
	if s == nil {
		return ""
	}

	switch s := *s; s {
	case SchemaTypeImplicit:
		return "implicit"
	case SchemaTypeExplicit:
		return "explicit"
	default:
		return "SchemaType(" + strconv.FormatInt(int64(s), 10) + ")"
	}
}

func (s *SchemaType) UnmarshalJSON(d []byte) error {
	var val string
	if err := json.Unmarshal(d, &val); err != nil {
		return err
	}

	switch val {
	case "implicit":
		*s = SchemaTypeImplicit
	case "explicit":
		*s = SchemaTypeExplicit
	default:
		return errors.New("unexpected value")
	}

	return nil
}

func (s *SchemaType) Equals(other *SchemaType) bool {
	if s == nil && other == nil {
		return true
	} else if s == nil || other == nil {
		return false
	}

	return *s == *other
}

func (s SchemaType) MarshalJSON() ([]byte, error) {
	switch s {
	case SchemaTypeImplicit:
		return []byte(`"implicit"`), nil
	case SchemaTypeExplicit:
		return []byte(`"explicit"`), nil
	default:
		return nil, errors.New("unexpected value")
	}
}

// Ptr returns a pointer to s.
func (s SchemaType) Ptr() *SchemaType { return &s }

// SemanticColumnType specifies the semantics of a measurement column.
type SemanticColumnType int

const (
	SemanticColumnTypeTimestamp SemanticColumnType = iota // SemanticColumnTypeTimestamp identifies the column is used as the timestamp
	SemanticColumnTypeTag                                 // SemanticColumnTypeTag identifies the column is used as a tag
	SemanticColumnTypeField                               // SemanticColumnTypeField identifies the column is used as a field
)

func SemanticColumnTypeFromString(s string) *SemanticColumnType {
	switch s {
	case "timestamp":
		return SemanticColumnTypeTimestamp.Ptr()
	case "tag":
		return SemanticColumnTypeTag.Ptr()
	case "field":
		return SemanticColumnTypeField.Ptr()
	default:
		return nil
	}
}

func (s SemanticColumnType) String() string {
	switch s {
	case SemanticColumnTypeTimestamp:
		return "timestamp"
	case SemanticColumnTypeTag:
		return "tag"
	case SemanticColumnTypeField:
		return "field"
	default:
		return "SemanticColumnType(" + strconv.FormatInt(int64(s), 10) + ")"
	}
}

func (s SemanticColumnType) Ptr() *SemanticColumnType {
	return &s
}

func (s *SemanticColumnType) UnmarshalJSON(d []byte) error {
	var val string
	if err := json.Unmarshal(d, &val); err != nil {
		return err
	}

	switch val {
	case "timestamp":
		*s = SemanticColumnTypeTimestamp
	case "tag":
		*s = SemanticColumnTypeTag
	case "field":
		*s = SemanticColumnTypeField
	default:
		return errors.New("unexpected value")
	}

	return nil
}

func (s SemanticColumnType) MarshalJSON() ([]byte, error) {
	switch s {
	case SemanticColumnTypeTimestamp:
		return []byte(`"timestamp"`), nil
	case SemanticColumnTypeTag:
		return []byte(`"tag"`), nil
	case SemanticColumnTypeField:
		return []byte(`"field"`), nil
	default:
		return nil, errors.New("unexpected value")
	}
}

type SchemaColumnDataType uint

const (
	SchemaColumnDataTypeFloat SchemaColumnDataType = iota
	SchemaColumnDataTypeInteger
	SchemaColumnDataTypeUnsigned
	SchemaColumnDataTypeString
	SchemaColumnDataTypeBoolean
)

func SchemaColumnDataTypeFromString(s string) *SchemaColumnDataType {
	switch s {
	case "float":
		return SchemaColumnDataTypeFloat.Ptr()
	case "integer":
		return SchemaColumnDataTypeInteger.Ptr()
	case "unsigned":
		return SchemaColumnDataTypeUnsigned.Ptr()
	case "string":
		return SchemaColumnDataTypeString.Ptr()
	case "boolean":
		return SchemaColumnDataTypeBoolean.Ptr()
	default:
		return nil
	}
}

// Ptr returns a pointer to s.
func (s SchemaColumnDataType) Ptr() *SchemaColumnDataType { return &s }

func (s *SchemaColumnDataType) String() string {
	if s == nil {
		return ""
	}

	switch *s {
	case SchemaColumnDataTypeFloat:
		return "float"
	case SchemaColumnDataTypeInteger:
		return "integer"
	case SchemaColumnDataTypeUnsigned:
		return "unsigned"
	case SchemaColumnDataTypeString:
		return "string"
	case SchemaColumnDataTypeBoolean:
		return "boolean"
	default:
		return "SchemaColumnDataType(" + strconv.FormatInt(int64(*s), 10) + ")"
	}
}

func (s *SchemaColumnDataType) UnmarshalJSON(d []byte) error {
	var val string
	if err := json.Unmarshal(d, &val); err != nil {
		return err
	}

	switch val {
	case "float":
		*s = SchemaColumnDataTypeFloat
	case "integer":
		*s = SchemaColumnDataTypeInteger
	case "unsigned":
		*s = SchemaColumnDataTypeUnsigned
	case "string":
		*s = SchemaColumnDataTypeString
	case "boolean":
		*s = SchemaColumnDataTypeBoolean
	default:
		return errors.New("unexpected value")
	}

	return nil
}

func (s SchemaColumnDataType) MarshalJSON() ([]byte, error) {
	switch s {
	case SchemaColumnDataTypeFloat:
		return []byte(`"float"`), nil
	case SchemaColumnDataTypeInteger:
		return []byte(`"integer"`), nil
	case SchemaColumnDataTypeUnsigned:
		return []byte(`"unsigned"`), nil
	case SchemaColumnDataTypeString:
		return []byte(`"string"`), nil
	case SchemaColumnDataTypeBoolean:
		return []byte(`"boolean"`), nil
	default:
		return nil, errors.New("unexpected value")
	}
}

var (
	schemaTypeToFieldTypeMap = [...]models.FieldType{
		SchemaColumnDataTypeFloat:    models.Float,
		SchemaColumnDataTypeInteger:  models.Integer,
		SchemaColumnDataTypeUnsigned: models.Unsigned,
		SchemaColumnDataTypeString:   models.String,
		SchemaColumnDataTypeBoolean:  models.Boolean,
	}
)

// ToFieldType maps SchemaColumnDataType to the equivalent models.FieldType or
// models.Empty if no such mapping exists.
func (s SchemaColumnDataType) ToFieldType() models.FieldType {
	if int(s) > len(schemaTypeToFieldTypeMap) {
		return models.Empty
	}
	return schemaTypeToFieldTypeMap[s]
}

type MeasurementSchema struct {
	ID       influxid.ID               `json:"id,omitempty"`
	OrgID    influxid.ID               `json:"orgID"`
	BucketID influxid.ID               `json:"bucketID"`
	Name     string                    `json:"name"`
	Columns  []MeasurementSchemaColumn `json:"columns"`
	CRUDLog
}

func (m *MeasurementSchema) Validate() error {
	var err error

	err = multierr.Append(err, m.validateName("name", m.Name))
	err = multierr.Append(err, m.validateColumns())

	return err
}

// ValidateMeasurementSchemaName determines if name is a valid identifier for
// a measurement schema or column name and if not, returns an error.
func ValidateMeasurementSchemaName(name string) error {
	if len(name) == 0 {
		return ErrMeasurementSchemaNameTooShort
	}

	if len(name) > 128 {
		return ErrMeasurementSchemaNameTooLong
	}

	if err := models.CheckToken([]byte(name)); err != nil {
		return &influxerror.Error{
			Code: influxerror.EInvalid,
			Err:  err,
		}
	}

	if strings.HasPrefix(name, "_") {
		return ErrMeasurementSchemaNameUnderscore
	}

	if strings.Contains(name, `"`) || strings.Contains(name, `'`) {
		return ErrMeasurementSchemaNameQuotes
	}

	return nil
}

func (m *MeasurementSchema) validateName(prefix, name string) error {
	if err := ValidateMeasurementSchemaName(name); err != nil {
		return fmt.Errorf("%s %q: %w", prefix, name, err)
	}

	return nil
}

// columns implements sort.Interface to efficiently sort a MeasurementSchemaColumn slice
// by using indices to store the sorted element indices.
type columns struct {
	indices []int // indices is a list of indices representing a sorted columns
	columns []MeasurementSchemaColumn
}

// newColumns returns an instance of columns which contains a sorted version of c.
func newColumns(c []MeasurementSchemaColumn) columns {
	colIndices := make([]int, len(c))
	for i := range c {
		colIndices[i] = i
	}
	res := columns{
		indices: colIndices,
		columns: c,
	}
	sort.Sort(res)
	return res
}

func (c columns) Len() int {
	return len(c.columns)
}

func (c columns) Less(i, j int) bool {
	return c.columns[c.indices[i]].Name < c.columns[c.indices[j]].Name
}

func (c columns) Swap(i, j int) {
	c.indices[i], c.indices[j] = c.indices[j], c.indices[i]
}

// Index returns the sorted
func (c columns) Index(i int) *MeasurementSchemaColumn {
	return &c.columns[c.indices[i]]
}

func (m *MeasurementSchema) validateColumns() (err error) {
	if len(m.Columns) == 0 {
		return ErrMeasurementSchemaColumnsMissing
	}

	cols := newColumns(m.Columns)

	timeCount := 0
	fieldCount := 0
	for i := range cols.columns {
		col := &cols.columns[i]

		err = multierr.Append(err, m.validateName("column name", col.Name))

		// special handling for time column
		if col.Name == "time" {
			timeCount++
			if col.Type != SemanticColumnTypeTimestamp {
				err = multierr.Append(err, ErrMeasurementSchemaColumnsTimeInvalidSemantic)
			} else if col.DataType != nil {
				err = multierr.Append(err, ErrMeasurementSchemaColumnsTimestampSemanticDataType)
			}
			continue
		}

		// ensure no other columns have a timestamp semantic
		switch col.Type {
		case SemanticColumnTypeTimestamp:
			if col.Name != "time" {
				err = multierr.Append(err, ErrMeasurementSchemaColumnsTimestampSemanticInvalidName)
			} else {
				if col.DataType != nil {
					err = multierr.Append(err, ErrMeasurementSchemaColumnsTimestampSemanticDataType)
				}
			}

		case SemanticColumnTypeTag:
			// ensure tag columns don't include a data type value
			if col.DataType != nil {
				err = multierr.Append(err, ErrMeasurementSchemaColumnsTagSemanticDataType)
			}

		case SemanticColumnTypeField:
			if col.DataType == nil {
				err = multierr.Append(err, ErrMeasurementSchemaColumnsFieldSemanticMissingDataType)
			}
			fieldCount++
		}
	}

	if timeCount == 0 {
		err = multierr.Append(err, ErrMeasurementSchemaColumnsMissingTime)
	}

	// ensure there is at least one field defined
	if fieldCount == 0 {
		err = multierr.Append(err, ErrMeasurementSchemaColumnsMissingFields)
	}

	// check for duplicate columns using general UTF-8 case insensitive comparison
	for i := range cols.columns[1:] {
		if strings.EqualFold(cols.Index(i).Name, cols.Index(i+1).Name) {
			err = multierr.Append(err, ErrMeasurementSchemaColumnsDuplicateNames)
			break
		}
	}

	return err
}

type MeasurementSchemaColumn struct {
	Name     string                `json:"name"`
	Type     SemanticColumnType    `json:"type"`
	DataType *SchemaColumnDataType `json:"dataType,omitempty"`
}
