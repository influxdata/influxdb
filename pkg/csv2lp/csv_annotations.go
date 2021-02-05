package csv2lp

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// annotationComment describes CSV annotation
type annotationComment struct {
	// prefix in a CSV row that recognizes this annotation
	prefix string
	// flag is 0 to represent an annotation that is used for all data rows
	// or a unique bit (>0) between supported annotation prefixes
	flag uint8
	// setupColumn setups metadata that drives the way of how column data
	// are parsed, mandatory when flag > 0
	setupColumn func(column *CsvTableColumn, columnValue string)
	// setupTable setups metadata that drives the way of how the table data
	// are parsed, mandatory when flag == 0
	setupTable func(table *CsvTable, row []string) error
}

// isTableAnnotation returns true for a table-wide annotation, false for column-based annotations
func (a annotationComment) isTableAnnotation() bool {
	return a.setupTable != nil
}

// matches tests whether an annotationComment can process the CSV comment row
func (a annotationComment) matches(comment string) bool {
	return strings.HasPrefix(strings.ToLower(comment), a.prefix)
}

func createConstantOrConcatColumn(table *CsvTable, row []string, annotationName string) CsvTableColumn {
	// adds a virtual column with constant value to all data rows
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
	col.setupDataType(row[0])
	var dataTypeIndex int
	if len(col.DataType) == 0 && col.LinePart == 0 {
		// type 1,2,3
		dataTypeIndex = 1
		if len(row) > 1 {
			col.setupDataType(row[1])
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
			col.Label = annotationName + " " + col.DataType
		} else if col.Label == "" {
			// setup a label if no label is supplied for focused error messages
			col.Label = annotationName + " " + col.DataType
		}
	}
	// add a virtual column to the table
	return col
}

// constantSetupTable setups the supplied CSV table from #constant annotation
func constantSetupTable(table *CsvTable, row []string) error {
	col := createConstantOrConcatColumn(table, row, "#constant")
	// add a virtual column to the table
	table.extraColumns = append(table.extraColumns, &col)
	return nil
}

// computedReplacer is used to replace value in computed columns
var computedReplacer *regexp.Regexp = regexp.MustCompile(`\$\{[^}]+\}`)

// concatSetupTable setups the supplied CSV table from #concat annotation
func concatSetupTable(table *CsvTable, row []string) error {
	col := createConstantOrConcatColumn(table, row, "#concat")
	template := col.DefaultValue
	col.ComputeValue = func(row []string) string {
		return computedReplacer.ReplaceAllStringFunc(template, func(text string) string {
			columnLabel := text[2 : len(text)-1] // ${columnLabel}
			if placeholderColumn := table.Column(columnLabel); placeholderColumn != nil {
				return placeholderColumn.Value(row)
			}
			log.Printf("WARNING: column %s: column '%s' cannot be replaced, no such column available", col.Label, columnLabel)
			return ""
		})
	}
	// add a virtual column to the table
	table.extraColumns = append(table.extraColumns, &col)
	// add validator to report error when no placeholder column is available
	table.validators = append(table.validators, func(table *CsvTable) error {
		placeholders := computedReplacer.FindAllString(template, len(template))
		for _, placeholder := range placeholders {
			columnLabel := placeholder[2 : len(placeholder)-1] // ${columnLabel}
			if placeholderColumn := table.Column(columnLabel); placeholderColumn == nil {
				return CsvColumnError{
					Column: col.Label,
					Err: fmt.Errorf("'%s' references an unknown column '%s', available columns are: %v",
						template, columnLabel, strings.Join(table.ColumnLabels(), ",")),
				}
			}
		}
		return nil
	})
	return nil
}

// supportedAnnotations contains all supported CSV annotations comments
var supportedAnnotations = []annotationComment{
	{
		prefix: "#group",
		flag:   1,
		setupColumn: func(column *CsvTableColumn, value string) {
			// standard flux query result annotation
			if strings.HasSuffix(value, "true") {
				// setup column's line part unless it is already set (#19452)
				if column.LinePart == 0 {
					column.LinePart = linePartTag
				}
			}
		},
	},
	{
		prefix: "#datatype",
		flag:   2,
		setupColumn: func(column *CsvTableColumn, value string) {
			// standard flux query result annotation
			column.setupDataType(value)
		},
	},
	{
		prefix: "#default",
		flag:   4,
		setupColumn: func(column *CsvTableColumn, value string) {
			// standard flux query result annotation
			column.DefaultValue = ignoreLeadingComment(value)
		},
	},
	{
		prefix:     "#constant",
		setupTable: constantSetupTable,
	},
	{
		prefix: "#timezone",
		setupTable: func(table *CsvTable, row []string) error {
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
		},
	},
	{
		prefix:     "#concat",
		setupTable: concatSetupTable,
	},
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

// parseTimeZone parses the supplied timezone from a string into a time.Location
//
//  parseTimeZone("")      // time.UTC
//  parseTimeZone("local") // time.Local
//  parseTimeZone("-0500") // time.FixedZone(-5*3600 + 0*60)
//  parseTimeZone("+0200") // time.FixedZone(2*3600 + 0*60)
//  parseTimeZone("EST")   // time.LoadLocation("EST")
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
