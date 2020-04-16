package csv2lp

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// annotationComment represents parsed CSV annotation
type annotationComment struct {
	// prefix in a CSV row that recognizes this annotation
	prefix string
	// flag is 0 to represent an annotation that is used for all data rows
	// or a unique bit (>0) that that is unique to the annotation
	flag uint8
	// setupColumn setups metadata that drives the way of how column data
	// are parsed, mandatory when flag > 0
	setupColumn func(column *CsvTableColumn, columnValue string)
	// setupColumn setups metadata that drives the way of how the table data
	// are parsed, mandatory when flag == 0
	setupTable func(table *CsvTable, row []string) error
}

func (a *annotationComment) isTableAnnotation() bool {
	return a.setupTable != nil
}

func (a *annotationComment) matches(comment string) bool {
	return strings.HasPrefix(strings.ToLower(comment), a.prefix)
}

var supportedAnnotations = []annotationComment{
	{"#group", 1, func(column *CsvTableColumn, value string) {
		// standard flux query result annotation
		if strings.HasSuffix(value, "true") {
			column.LinePart = linePartTag
		}
	}, nil},
	{"#datatype", 2, func(column *CsvTableColumn, value string) {
		// standard flux query result annotation
		setupDataType(column, value)
	}, nil},
	{"#default", 4, func(column *CsvTableColumn, value string) {
		// standard flux query result annotation
		column.DefaultValue = ignoreLeadingComment(value)
	}, nil},
	{"#constant", 0, nil, func(table *CsvTable, row []string) error {
		// adds a virtual column with contsant value to all data rows
		// supported types of constant annotation rows are:
		//  1. "#constant,datatype,label,defaultValue"
		//  2. "#constant,measurement,value"
		//  3. "#constant,dateTime,value"
		//  4. "#constant datatype,label,defaultValue"
		//  5. "#constant measurement,value"
		//  6. "#constant dateTime,value"
		// defaultValue is optional, additional columns are ignored
		col := CsvTableColumn{}
		col.Index = -1 // this is a virtual column that never extracts data from data rows
		// setup column data type
		setupDataType(&col, row[0])
		var dataTypeIndex int
		if len(col.DataType) == 0 && col.LinePart == 0 {
			// type 1,2,3
			dataTypeIndex = 1
			if len(row) > 1 {
				setupDataType(&col, row[1])
			}
		} else {
			// type 4,5,6
			dataTypeIndex = 0
		}
		// setup label if available
		if len(row) > dataTypeIndex+1 {
			col.Label = row[dataTypeIndex+1]
		}
		// setup defaultValue if available
		if len(row) > dataTypeIndex+2 {
			col.DefaultValue = row[dataTypeIndex+2]
		}
		// support type 2,3,5,6 syntax for measurement and timestamp
		if col.LinePart == linePartMeasurement || col.LinePart == linePartTime {
			if col.DefaultValue == "" && col.Label != "" {
				// type 2,3,5,6
				col.DefaultValue = col.Label
				col.Label = "#constant " + col.DataType
			} else if col.Label == "" {
				// setup a label if no label is supplied fo focused error messages
				col.Label = "#constant " + col.DataType
			}
		}
		// add a virtual column to the table
		table.extraColumns = append(table.extraColumns, col)
		return nil
	}},
	{"#timezone", 0, nil, func(table *CsvTable, row []string) error {
		// setup timezone for parsing timestamps, UTC by default
		val := ignoreLeadingComment(row[0])
		if val == "" && len(row) > 1 {
			val = row[1] // #timezone,Local
		}
		tz, err := parseTimeZone(val)
		if err != nil {
			return fmt.Errorf("#timezone annotation: %v", err)
		}
		table.timeZone = tz
		return nil
	}},
}

// setupDataType setups data type from a column value
func setupDataType(column *CsvTableColumn, columnValue string) {
	// columnValue contains typeName and possibly additional column metadata,
	// it can be
	//  1. typeName
	//  2. typeName:format
	//  3. typeName|defaultValue
	//  4. typeName:format|defaultValue
	//  5. #anycomment (all options above)

	// ignoreLeadingComment is also required to specify datatype together with CSV annotation
	// in #constant annotation
	columnValue = ignoreLeadingComment(columnValue)

	// | adds a default value to column
	pipeIndex := strings.Index(columnValue, "|")
	if pipeIndex > 1 {
		if column.DefaultValue == "" {
			column.DefaultValue = columnValue[pipeIndex+1:]
			columnValue = columnValue[:pipeIndex]
		}
	}
	// setup column format
	colonIndex := strings.Index(columnValue, ":")
	if colonIndex > 1 {
		column.DataFormat = columnValue[colonIndex+1:]
		columnValue = columnValue[:colonIndex]
	}

	// setup column linePart depending dataType
	switch {
	case columnValue == "tag":
		column.LinePart = linePartTag
	case strings.HasPrefix(columnValue, "ignore"):
		// ignore or ignored
		column.LinePart = linePartIgnored
	case columnValue == "dateTime":
		// dateTime field is used at most once in a protocol line
		column.LinePart = linePartTime
	case columnValue == "measurement":
		column.LinePart = linePartMeasurement
	case columnValue == "field":
		column.LinePart = linePartField
		columnValue = "" // this a generic field without a data type specified
	case columnValue == "time": // time is an alias for dateTime
		column.LinePart = linePartTime
		columnValue = dateTimeDatatype
	}
	// setup column data type
	column.DataType = columnValue

	// setup custom parsing of bool data type
	if column.DataType == boolDatatype && column.DataFormat != "" {
		column.ParseF = createBoolParseFn(column.DataFormat)
	}
}

// ignoreLeadingComment returns a value without '#anyComment ' prefix
func ignoreLeadingComment(value string) string {
	if len(value) > 0 && value[0] == '#' {
		pos := strings.Index(value, " ")
		if pos > 0 {
			return strings.TrimLeft(value[pos+1:], " ")
		}
		return ""
	}
	return value
}

// parseTimeZone tries to parse the supplied timezone indicator as a Location or returns an error
func parseTimeZone(val string) (*time.Location, error) {
	switch {
	case val == "":
		return time.UTC, nil
	case strings.ToLower(val) == "local":
		return time.Local, nil
	case val[0] == '-' || val[0] == '+':
		if matched, _ := regexp.MatchString("[+-][0-9][0-9][0-9][0-9]", val); !matched {
			return nil, fmt.Errorf("timezone '%s' is not  +hhmm or -hhmm", val)
		}
		intVal, _ := strconv.Atoi(val)
		offset := (intVal/100)*3600 + (intVal%100)*60
		return time.FixedZone(val, offset), nil
	default:
		return time.LoadLocation(val)
	}
}
