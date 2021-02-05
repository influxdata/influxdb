package csv2lp

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
	"unsafe"
)

// column labels used in flux CSV result
const (
	labelFieldName   = "_field"
	labelFieldValue  = "_value"
	labelTime        = "_time"
	labelStart       = "_start"
	labelStop        = "_stop"
	labelMeasurement = "_measurement"
)

// types of columns with respect to line protocol
const (
	linePartIgnored = iota + 1 // ignored in line protocol
	linePartMeasurement
	linePartTag
	linePartField
	linePartTime
)

// CsvTableColumn represents processing metadata about a csv column
type CsvTableColumn struct {
	// Label is a column label from the header row, such as "_start", "_stop", "_time"
	Label string
	// DataType such as "string", "long", "dateTime" ...
	DataType string
	// DataFormat is a format of DataType, such as "RFC3339", "2006-01-02"
	DataFormat string
	// LinePart is a line part of the column (0 means not determined yet), see linePart constants
	LinePart int
	// DefaultValue is used when column's value is an empty string.
	DefaultValue string
	// Index of this column when reading rows, -1 indicates a virtual column with DefaultValue data
	Index int
	// TimeZone of dateTime column, applied when parsing dateTime DataType
	TimeZone *time.Location
	// ParseF is an optional function used to convert column's string value to interface{}
	ParseF func(value string) (interface{}, error)
	// ComputeValue is an optional function used to compute column value out of row data
	ComputeValue func(row []string) string

	// escapedLabel contains escaped label that can be directly used in line protocol
	escapedLabel string
}

// LineLabel returns escaped name of the column so it can be then used as a tag name or field name in line protocol
func (c *CsvTableColumn) LineLabel() string {
	if len(c.escapedLabel) > 0 {
		return c.escapedLabel
	}
	return c.Label
}

// Value returns the value of the column for the supplied row
func (c *CsvTableColumn) Value(row []string) string {
	if c.Index < 0 || c.Index >= len(row) {
		if c.ComputeValue != nil {
			return c.ComputeValue(row)
		}
		return c.DefaultValue
	}
	val := row[c.Index]
	if len(val) > 0 {
		return val
	}
	return c.DefaultValue
}

// setupDataType setups data type from the value supplied
//
// columnValue contains typeName and possibly additional column metadata,
// it can be
//  1. typeName
//  2. typeName:format
//  3. typeName|defaultValue
//  4. typeName:format|defaultValue
//  5. #anycomment (all options above)
func (c *CsvTableColumn) setupDataType(columnValue string) {
	// ignoreLeadingComment is required to specify datatype together with CSV annotation
	// in annotations (such as #constant)
	columnValue = ignoreLeadingComment(columnValue)

	// | adds a default value to column
	pipeIndex := strings.Index(columnValue, "|")
	if pipeIndex > 1 {
		if c.DefaultValue == "" {
			c.DefaultValue = columnValue[pipeIndex+1:]
			columnValue = columnValue[:pipeIndex]
		}
	}
	// setup column format
	colonIndex := strings.Index(columnValue, ":")
	if colonIndex > 1 {
		c.DataFormat = columnValue[colonIndex+1:]
		columnValue = columnValue[:colonIndex]
	}

	// setup column linePart depending dataType
	switch {
	case columnValue == "tag":
		c.LinePart = linePartTag
	case strings.HasPrefix(columnValue, "ignore"):
		// ignore or ignored
		c.LinePart = linePartIgnored
	case columnValue == "dateTime":
		// dateTime field is used at most once in a protocol line
		c.LinePart = linePartTime
	case columnValue == "measurement":
		c.LinePart = linePartMeasurement
	case columnValue == "field":
		c.LinePart = linePartField
		columnValue = "" // this a generic field without a data type specified
	case columnValue == "time": // time is an alias for dateTime
		c.LinePart = linePartTime
		columnValue = dateTimeDatatype
	default:
		// nothing to do since we don't know the linePart yet
		// the line part is decided in recomputeLineProtocolColumns
	}
	// setup column data type
	c.DataType = columnValue

	// setup custom parsing
	if c.DataType == boolDatatype && c.DataFormat != "" {
		c.ParseF = createBoolParseFn(c.DataFormat)
		return
	}
	if c.DataType == longDatatype && strings.HasPrefix(c.DataFormat, "strict") {
		c.ParseF = createStrictLongParseFn(c.DataFormat[6:])
		return
	}
	if c.DataType == uLongDatatype && strings.HasPrefix(c.DataFormat, "strict") {
		c.ParseF = createStrictUnsignedLongParseFn(c.DataFormat[6:])
		return
	}
}

// CsvColumnError indicates conversion error in a specific column
type CsvColumnError struct {
	Column string
	Err    error
}

// Error interface implementation
func (e CsvColumnError) Error() string {
	return fmt.Sprintf("column '%s': %v", e.Column, e.Err)
}

// CsvTable contains metadata about columns and a state of the CSV processing
type CsvTable struct {
	// columns contains columns that extract values from data rows
	columns []*CsvTableColumn
	// partBits is a bitmap that is used to remember that a particular column annotation
	// (#group, #datatype and #default) was already processed for the table;
	// it is used to detect start of a new table in CSV flux results, a repeated annotation
	// is detected and a new CsvTable can be then created
	partBits uint8
	// readTableData indicates that the table is ready to read table data, which
	// is after reading annotation and header rows
	readTableData bool
	// lpColumnsValid indicates whether line protocol columns are valid or must be re-calculated from columns
	lpColumnsValid bool
	// extraColumns are added by table-wide annotations, such as #constant
	extraColumns []*CsvTableColumn
	// ignoreDataTypeInColumnName is true to skip parsing of data type as a part a column name
	ignoreDataTypeInColumnName bool
	// timeZone of dateTime column(s), applied when parsing dateTime value without a time zone specified
	timeZone *time.Location
	// validators validate table structure right before processing data rows
	validators []func(*CsvTable) error

	/* cached columns are initialized before reading the data rows using the computeLineProtocolColumns fn */
	// cachedMeasurement is a required column that read (line protocol) measurement
	cachedMeasurement *CsvTableColumn
	// cachedTime is an optional column that reads timestamp of lp row
	cachedTime *CsvTableColumn
	// cachedFieldName is an optional column that reads a field name to add to the protocol line
	cachedFieldName *CsvTableColumn
	// cachedFieldValue is an optional column that reads a field value to add to the protocol line
	cachedFieldValue *CsvTableColumn
	// cachedFields are columns that read field values, a field name is taken from a column label
	cachedFields []*CsvTableColumn
	// cachedTags are columns that read tag values, a tag name is taken from a column label
	cachedTags []*CsvTableColumn
}

// IgnoreDataTypeInColumnName sets a flag that can ignore dataType parsing in column names.
// When true, column names can then contain '|'. By default, column name can also contain datatype
// and a default value when named `name|datatype` or `name|datatype|default`,
// for example `ready|boolean|true`
func (t *CsvTable) IgnoreDataTypeInColumnName(val bool) {
	t.ignoreDataTypeInColumnName = val
}

// DataColumnsInfo returns a string representation of columns that are used to process CSV data
func (t *CsvTable) DataColumnsInfo() string {
	if t == nil {
		return "<nil>"
	}
	var builder = strings.Builder{}
	t.computeLineProtocolColumns() // censure that ached columns are initialized
	builder.WriteString(fmt.Sprintf("CsvTable{ dataColumns: %d constantColumns: %d\n", len(t.columns), len(t.extraColumns)))
	builder.WriteString(fmt.Sprintf(" measurement: %+v\n", t.cachedMeasurement))
	for _, col := range t.cachedTags {
		builder.WriteString(fmt.Sprintf(" tag:         %+v\n", col))
	}
	for _, col := range t.cachedFields {
		builder.WriteString(fmt.Sprintf(" field:       %+v\n", col))
	}
	builder.WriteString(fmt.Sprintf(" time:        %+v\n", t.cachedTime))
	builder.WriteString("}")

	return builder.String()
}

// NextTable resets the table to a state in which it expects annotations and header rows
func (t *CsvTable) NextTable() {
	t.partBits = 0 // no column annotations parsed yet
	t.readTableData = false
	t.columns = []*CsvTableColumn{}
	t.extraColumns = []*CsvTableColumn{}
}

// createColumns create a slice of CsvTableColumn for the supplied rowSize
func createColumns(rowSize int) []*CsvTableColumn {
	retVal := make([]*CsvTableColumn, rowSize)
	for i := 0; i < rowSize; i++ {
		retVal[i] = &CsvTableColumn{
			Index: i,
		}
	}
	return retVal
}

// AddRow updates the state of the CSV table with a new header, annotation or data row.
// Returns true if the row is a data row.
func (t *CsvTable) AddRow(row []string) bool {
	// detect data row or table header row
	if len(row[0]) == 0 || row[0][0] != '#' {
		if !t.readTableData {
			// expect a header row
			t.lpColumnsValid = false // line protocol columns change
			if t.partBits == 0 {
				// create columns since no column annotations were processed
				t.columns = createColumns(len(row))
			}
			// assign column labels for the header row
			for i := 0; i < len(t.columns); i++ {
				col := t.columns[i]
				if len(col.Label) == 0 && col.Index < len(row) {
					col.Label = row[col.Index]
					// assign column data type if possible
					if len(col.DataType) == 0 && !t.ignoreDataTypeInColumnName {
						if idx := strings.IndexByte(col.Label, '|'); idx != -1 {
							col.setupDataType(col.Label[idx+1:])
							col.Label = col.Label[:idx]
						}
					}
				}
			}
			// header row is read, now expect data rows
			t.readTableData = true
			return false
		}
		return true
	}

	// process all supported annotations
	for i := 0; i < len(supportedAnnotations); i++ {
		supportedAnnotation := supportedAnnotations[i]
		if supportedAnnotation.matches(row[0]) {
			if len(row[0]) > len(supportedAnnotation.prefix) && row[0][len(supportedAnnotation.prefix)] != ' ' {
				continue // ignoring, not a supported annotation
			}
			t.lpColumnsValid = false // line protocol columns change
			if supportedAnnotation.isTableAnnotation() {
				// process table-level annotation
				if err := supportedAnnotation.setupTable(t, row); err != nil {
					log.Println("WARNING: ", err)
				}
				return false
			}
			// invariant: !supportedAnnotation.isTableAnnotation()
			if t.readTableData {
				// any column annotation stops reading of data rows
				t.NextTable()
			}
			// create new columns upon new or repeated column annotation
			if t.partBits == 0 || t.partBits&supportedAnnotation.flag == 1 {
				t.partBits = supportedAnnotation.flag
				t.columns = createColumns(len(row))
			} else {
				t.partBits = t.partBits | supportedAnnotation.flag
			}
			// setup columns according to column annotation
			for j := 0; j < len(t.columns); j++ {
				col := t.columns[j]
				if col.Index >= len(row) {
					continue // missing value
				} else {
					supportedAnnotation.setupColumn(col, row[col.Index])
				}
			}
			return false
		}
	}
	// warn about unsupported annotation unless a comment row
	if !strings.HasPrefix(row[0], "# ") {
		log.Println("WARNING: unsupported annotation: ", row[0])
	}
	return false
}

// computeLineProtocolColumns computes columns that are
// used to create line protocol rows when required to do so
//
// returns true if new columns were initialized or false if there
// was no change in line protocol columns
func (t *CsvTable) computeLineProtocolColumns() bool {
	if !t.lpColumnsValid {
		t.recomputeLineProtocolColumns()
		return true
	}
	return false
}

// recomputeLineProtocolColumns always computes the columns that are
// used to create line protocol rows
func (t *CsvTable) recomputeLineProtocolColumns() {
	// reset results
	t.cachedMeasurement = nil
	t.cachedTime = nil
	t.cachedFieldName = nil
	t.cachedFieldValue = nil
	t.cachedTags = nil
	t.cachedFields = nil
	// collect unique tag names (#19453)
	var tags = make(map[string]*CsvTableColumn)

	// having a _field column indicates fields without a line type are ignored
	defaultIsField := t.Column(labelFieldName) == nil

	// go over columns + extra columns
	columns := make([]*CsvTableColumn, len(t.columns)+len(t.extraColumns))
	copy(columns, t.columns)
	copy(columns[len(t.columns):], t.extraColumns)
	for i := 0; i < len(columns); i++ {
		col := columns[i]
		switch {
		case col.Label == labelMeasurement || col.LinePart == linePartMeasurement:
			t.cachedMeasurement = col
		case col.Label == labelTime || col.LinePart == linePartTime:
			if t.cachedTime != nil && t.cachedTime.Label != labelStart && t.cachedTime.Label != labelStop {
				log.Printf("WARNING: at most one dateTime column is expected, '%s' column is ignored\n", t.cachedTime.Label)
			}
			t.cachedTime = col
		case len(strings.TrimSpace(col.Label)) == 0 || col.LinePart == linePartIgnored:
			// ignored columns that are marked to be ignored or without a label
		case col.Label == labelFieldName:
			t.cachedFieldName = col
		case col.Label == labelFieldValue:
			t.cachedFieldValue = col
		case col.LinePart == linePartTag:
			if val, found := tags[col.Label]; found {
				log.Printf("WARNING: ignoring duplicate tag '%s' at column index %d, using column at index %d\n", col.Label, val.Index, col.Index)
			}
			col.escapedLabel = escapeTag(col.Label)
			tags[col.Label] = col
		case col.LinePart == linePartField:
			col.escapedLabel = escapeTag(col.Label)
			t.cachedFields = append(t.cachedFields, col)
		default:
			if defaultIsField {
				col.escapedLabel = escapeTag(col.Label)
				t.cachedFields = append(t.cachedFields, col)
			}
		}
	}
	// line protocol requires sorted unique tags
	if len(tags) > 0 {
		t.cachedTags = make([]*CsvTableColumn, 0, len(tags))
		for _, v := range tags {
			t.cachedTags = append(t.cachedTags, v)
		}
		sort.Slice(t.cachedTags, func(i, j int) bool {
			return t.cachedTags[i].Label < t.cachedTags[j].Label
		})
	}
	// setup timezone for timestamp column
	if t.cachedTime != nil && t.cachedTime.TimeZone == nil {
		t.cachedTime.TimeZone = t.timeZone
	}

	t.lpColumnsValid = true // line protocol columns are now fresh
}

// CreateLine produces a protocol line out of the supplied row or returns error
func (t *CsvTable) CreateLine(row []string) (line string, err error) {
	buffer := make([]byte, 100)[:0]
	buffer, err = t.AppendLine(buffer, row, -1)
	if err != nil {
		return "", err
	}
	return *(*string)(unsafe.Pointer(&buffer)), nil
}

// AppendLine appends a protocol line to the supplied buffer using a CSV row and returns appended buffer or an error if any
func (t *CsvTable) AppendLine(buffer []byte, row []string, lineNumber int) ([]byte, error) {
	if t.computeLineProtocolColumns() {
		// validate column data types
		if t.cachedFieldValue != nil && !IsTypeSupported(t.cachedFieldValue.DataType) {
			return buffer, CsvColumnError{
				t.cachedFieldValue.Label,
				fmt.Errorf("data type '%s' is not supported", t.cachedFieldValue.DataType),
			}
		}
		for _, c := range t.cachedFields {
			if !IsTypeSupported(c.DataType) {
				return buffer, CsvColumnError{
					c.Label,
					fmt.Errorf("data type '%s' is not supported", c.DataType),
				}
			}
		}
		for _, v := range t.validators {
			if err := v(t); err != nil {
				return buffer, err
			}
		}
	}

	if t.cachedMeasurement == nil {
		return buffer, errors.New("no measurement column found")
	}
	measurement := t.cachedMeasurement.Value(row)
	if measurement == "" {
		return buffer, CsvColumnError{
			t.cachedMeasurement.Label,
			errors.New("no measurement supplied"),
		}
	}
	buffer = append(buffer, escapeMeasurement(measurement)...)
	for _, tag := range t.cachedTags {
		value := tag.Value(row)
		if tag.Index < len(row) && len(value) > 0 {
			buffer = append(buffer, ',')
			buffer = append(buffer, tag.LineLabel()...)
			buffer = append(buffer, '=')
			buffer = append(buffer, escapeTag(value)...)
		}
	}
	buffer = append(buffer, ' ')
	fieldAdded := false
	if t.cachedFieldName != nil && t.cachedFieldValue != nil {
		field := t.cachedFieldName.Value(row)
		value := t.cachedFieldValue.Value(row)
		if len(value) > 0 && len(field) > 0 {
			buffer = append(buffer, escapeTag(field)...)
			buffer = append(buffer, '=')
			var err error
			buffer, err = appendConverted(buffer, value, t.cachedFieldValue, lineNumber)
			if err != nil {
				return buffer, CsvColumnError{
					t.cachedFieldName.Label,
					err,
				}
			}
			fieldAdded = true
		}
	}
	for _, field := range t.cachedFields {
		value := field.Value(row)
		if len(value) > 0 {
			if !fieldAdded {
				fieldAdded = true
			} else {
				buffer = append(buffer, ',')
			}
			buffer = append(buffer, field.LineLabel()...)
			buffer = append(buffer, '=')
			var err error
			buffer, err = appendConverted(buffer, value, field, lineNumber)
			if err != nil {
				return buffer, CsvColumnError{
					field.Label,
					err,
				}
			}
		}
	}
	if !fieldAdded {
		return buffer, errors.New("no field data found")
	}

	if t.cachedTime != nil && t.cachedTime.Index < len(row) {
		timeVal := t.cachedTime.Value(row)
		if len(timeVal) > 0 {
			if len(t.cachedTime.DataType) == 0 {
				// assume dateTime data type (number or RFC3339)
				t.cachedTime.DataType = dateTimeDatatype
				t.cachedTime.DataFormat = ""
			}
			buffer = append(buffer, ' ')
			var err error
			buffer, err = appendConverted(buffer, timeVal, t.cachedTime, lineNumber)
			if err != nil {
				return buffer, CsvColumnError{
					t.cachedTime.Label,
					err,
				}
			}
		}
	}
	return buffer, nil
}

// Column returns the first column of the supplied label or nil
func (t *CsvTable) Column(label string) *CsvTableColumn {
	for i := 0; i < len(t.columns); i++ {
		if t.columns[i].Label == label {
			return t.columns[i]
		}
	}
	return nil
}

// Columns returns available columns
func (t *CsvTable) Columns() []*CsvTableColumn {
	return t.columns
}

// ColumnLabels returns available columns labels
func (t *CsvTable) ColumnLabels() []string {
	labels := make([]string, len(t.columns))
	for i, col := range t.columns {
		labels[i] = col.Label
	}
	return labels
}

// Measurement returns measurement column or nil
func (t *CsvTable) Measurement() *CsvTableColumn {
	t.computeLineProtocolColumns()
	return t.cachedMeasurement
}

// Time returns time column or nil
func (t *CsvTable) Time() *CsvTableColumn {
	t.computeLineProtocolColumns()
	return t.cachedTime
}

// FieldName returns field name column or nil
func (t *CsvTable) FieldName() *CsvTableColumn {
	t.computeLineProtocolColumns()
	return t.cachedFieldName
}

// FieldValue returns field value column or nil
func (t *CsvTable) FieldValue() *CsvTableColumn {
	t.computeLineProtocolColumns()
	return t.cachedFieldValue
}

// Tags returns tags
func (t *CsvTable) Tags() []*CsvTableColumn {
	t.computeLineProtocolColumns()
	return t.cachedTags
}

// Fields returns fields
func (t *CsvTable) Fields() []*CsvTableColumn {
	t.computeLineProtocolColumns()
	return t.cachedFields
}
