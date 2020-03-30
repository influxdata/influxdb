package write

import "strings"

type annotationComment struct {
	label       string
	flag        uint8                                      // bit to mark the presence of a column-based annotation
	setupColumn func(column *CsvTableColumn, value string) // mandatory when flag > 0
	setupTable  func(table *CsvTable, row []string)        // mandatory when flag == 0
}

func (a *annotationComment) isTableAnnotation() bool {
	return a.flag == uint8(0)
}

func setupDataType(column *CsvTableColumn, val string) {
	val = ignoreLeadingComment(val)
	switch {
	case val == "tag":
		column.LinePart = linePartTag
	case strings.HasPrefix(val, "ignore"):
		column.LinePart = linePartIgnored
	case strings.HasPrefix(val, "dateTime"):
		// dateTime field shall be used only for time line part
		column.LinePart = linePartTime
	case val == "measurement":
		column.LinePart = linePartMeasurement
	case val == "field":
		column.LinePart = linePartField
		val = ""
	case val == "time": // time is an alias for dateTime
		column.LinePart = linePartTime
		val = dateTimeDatatype
	}
	colonIndex := strings.Index(val, ":")
	if colonIndex > 1 {
		column.DataType = val[:colonIndex]
		column.DataFormat = val[colonIndex+1:]
	} else {
		column.DataType = val
		column.DataFormat = ""
	}
}

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

var supportedAnnotations = []annotationComment{
	{"#group", 1, func(column *CsvTableColumn, value string) {
		if strings.HasSuffix(value, "true") {
			column.LinePart = linePartTag
		}
	}, nil},
	{"#datatype", 2, func(column *CsvTableColumn, value string) {
		// use extra data type values to identify line parts
		setupDataType(column, value)
	}, nil},
	{"#default", 4, func(column *CsvTableColumn, value string) {
		column.DefaultValue = ignoreLeadingComment(value)
	}, nil},
	{"#constant", 0, nil, func(table *CsvTable, row []string) {
		// row format is: "#constant,datatype,label,defaultValue"
		col := CsvTableColumn{}
		col.Index = -1 // the will never extract data from row
		setupDataType(&col, row[0])
		var dataTypeIndex int
		if len(col.DataType) > 0 || col.LinePart != 0 {
			dataTypeIndex = 0
		} else {
			dataTypeIndex = 1
			if len(row) > 1 {
				setupDataType(&col, row[1])
			}
		}
		if len(row) > dataTypeIndex+1 {
			col.Label = row[dataTypeIndex+1]
		}
		if len(row) > dataTypeIndex+2 {
			col.DefaultValue = row[dataTypeIndex+2]
		}
		if col.LinePart == linePartMeasurement || col.LinePart == linePartTime {
			if col.DefaultValue == "" && col.Label != "" {
				// label is used as a default value
				col.DefaultValue = col.Label
				col.Label = "#constant " + col.DataType
			}
			if col.Label == "" {
				col.Label = "#constant " + col.DataType
			}
		}
		table.extraColumns = append(table.extraColumns, col)
	}},
}
