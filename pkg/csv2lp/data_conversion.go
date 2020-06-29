package csv2lp

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/encoding/ianaindex"
)

// see https://v2.docs.influxdata.com/v2.0/reference/syntax/annotated-csv/#valid-data-types
const (
	stringDatatype       = "string"
	doubleDatatype       = "double"
	boolDatatype         = "boolean"
	longDatatype         = "long"
	uLongDatatype        = "unsignedLong"
	durationDatatype     = "duration"
	base64BinaryDataType = "base64Binary"
	dateTimeDatatype     = "dateTime"
)

// predefined dateTime formats
const (
	RFC3339          = "RFC3339"
	RFC3339Nano      = "RFC3339Nano"
	dataFormatNumber = "number" //the same as long, but serialized without i suffix, used for timestamps
)

var supportedDataTypes map[string]struct{}

func init() {
	supportedDataTypes = make(map[string]struct{}, 9)
	supportedDataTypes[stringDatatype] = struct{}{}
	supportedDataTypes[doubleDatatype] = struct{}{}
	supportedDataTypes[boolDatatype] = struct{}{}
	supportedDataTypes[longDatatype] = struct{}{}
	supportedDataTypes[uLongDatatype] = struct{}{}
	supportedDataTypes[durationDatatype] = struct{}{}
	supportedDataTypes[base64BinaryDataType] = struct{}{}
	supportedDataTypes[dateTimeDatatype] = struct{}{}
	supportedDataTypes[""] = struct{}{}
}

// IsTypeSupported returns true if the data type is supported
func IsTypeSupported(dataType string) bool {
	_, supported := supportedDataTypes[dataType]
	return supported
}

var replaceMeasurement *strings.Replacer = strings.NewReplacer(",", "\\,", " ", "\\ ")
var replaceTag *strings.Replacer = strings.NewReplacer(",", "\\,", " ", "\\ ", "=", "\\=")
var replaceQuoted *strings.Replacer = strings.NewReplacer("\"", "\\\"", "\\", "\\\\")

func escapeMeasurement(val string) string {
	for i := 0; i < len(val); i++ {
		if val[i] == ',' || val[i] == ' ' {
			return replaceMeasurement.Replace(val)
		}
	}
	return val
}
func escapeTag(val string) string {
	for i := 0; i < len(val); i++ {
		if val[i] == ',' || val[i] == ' ' || val[i] == '=' {
			return replaceTag.Replace(val)
		}
	}
	return val
}
func escapeString(val string) string {
	for i := 0; i < len(val); i++ {
		if val[i] == '"' || val[i] == '\\' {
			return replaceQuoted.Replace(val)
		}
	}
	return val
}

// normalizeNumberString normalizes the supplied value according to the supplied format.
// This normalization is intended to convert number strings of different locales to a strconv-parseable value.
//
// The format's first character is a fraction delimiter character.  Next characters in the format
// are simply removed, they are typically used to visually separate groups in large numbers.
// The removeFraction parameter controls whether the returned value can contain also the fraction part.
// An empty format means ". \n\t\r_"
//
// For example, to get a strconv-parseable float from a Spanish value '3.494.826.157,123', use format ",." .
func normalizeNumberString(value string, format string, removeFraction bool) (normalized string, truncated bool) {
	if format == "" {
		format = ". \n\t\r_"
	}
	if strings.ContainsAny(value, format) {
		formatRunes := []rune(format)
		fractionRune := formatRunes[0]
		ignored := formatRunes[1:]
		retVal := strings.Builder{}
		retVal.Grow(len(value))
	ForAllCharacters:
		for _, c := range value {
			// skip ignored characters
			for i := 0; i < len(ignored); i++ {
				if c == ignored[i] {
					continue ForAllCharacters
				}
			}
			if c == fractionRune {
				if removeFraction {
					return retVal.String(), true
				}
				retVal.WriteByte('.')
				continue
			}
			retVal.WriteRune(c)
		}

		return retVal.String(), false
	}
	return value, false
}

func toTypedValue(val string, column *CsvTableColumn, lineNumber int) (interface{}, error) {
	dataType := column.DataType
	dataFormat := column.DataFormat
	if column.ParseF != nil {
		return column.ParseF(val)
	}
	switch dataType {
	case stringDatatype:
		return val, nil
	case dateTimeDatatype:
		switch dataFormat {
		case "": // number or time.RFC3339
			t, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return time.Parse(time.RFC3339, val)
			}
			return time.Unix(0, t).UTC(), nil
		case RFC3339:
			return time.Parse(time.RFC3339, val)
		case RFC3339Nano:
			return time.Parse(time.RFC3339Nano, val)
		case dataFormatNumber:
			t, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, err
			}
			return time.Unix(0, t).UTC(), nil
		default:
			if column.TimeZone != nil {
				return time.ParseInLocation(dataFormat, val, column.TimeZone)
			}
			return time.Parse(dataFormat, val)
		}
	case durationDatatype:
		return time.ParseDuration(val)
	case doubleDatatype:
		normalized, _ := normalizeNumberString(val, dataFormat, false)
		return strconv.ParseFloat(normalized, 64)
	case boolDatatype:
		switch {
		case len(val) == 0:
			return nil, errors.New("Unsupported boolean value '" + val + "' , first character is expected to be 't','f','0','1','y','n'")
		case val[0] == 't' || val[0] == 'T' || val[0] == 'y' || val[0] == 'Y' || val[0] == '1':
			return true, nil
		case val[0] == 'f' || val[0] == 'F' || val[0] == 'n' || val[0] == 'N' || val[0] == '0':
			return false, nil
		default:
			return nil, errors.New("Unsupported boolean value '" + val + "' , first character is expected to be 't','f','0','1','y','n'")
		}
	case longDatatype:
		normalized, truncated := normalizeNumberString(val, dataFormat, true)
		if truncated {
			error := CreateRowColumnError(lineNumber, column.Label,
				fmt.Errorf("'%s' truncated to '%s' to fit into long data type", val, normalized))
			log.Printf("WARNING: %v\n", error)
		}
		return strconv.ParseInt(normalized, 10, 64)
	case uLongDatatype:
		normalized, truncated := normalizeNumberString(val, dataFormat, true)
		if truncated {
			error := CreateRowColumnError(lineNumber, column.Label,
				fmt.Errorf("'%s' truncated to '%s' to fit into unsignedLong data type", val, normalized))
			log.Printf("WARNING: %v\n", error)
		}
		return strconv.ParseUint(normalized, 10, 64)
	case base64BinaryDataType:
		return base64.StdEncoding.DecodeString(val)
	default:
		return nil, fmt.Errorf("unsupported data type '%s'", dataType)
	}
}

func appendProtocolValue(buffer []byte, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case uint64:
		return append(strconv.AppendUint(buffer, v, 10), 'u'), nil
	case int64:
		return append(strconv.AppendInt(buffer, v, 10), 'i'), nil
	case int:
		return append(strconv.AppendInt(buffer, int64(v), 10), 'i'), nil
	case float64:
		if math.IsNaN(v) {
			return buffer, errors.New("value is NaN")
		}
		if math.IsInf(v, 0) {
			return buffer, errors.New("value is Infinite")
		}
		return strconv.AppendFloat(buffer, v, 'f', -1, 64), nil
	case float32:
		v32 := float64(v)
		if math.IsNaN(v32) {
			return buffer, errors.New("value is NaN")
		}
		if math.IsInf(v32, 0) {
			return buffer, errors.New("value is Infinite")
		}
		return strconv.AppendFloat(buffer, v32, 'f', -1, 64), nil
	case string:
		buffer = append(buffer, '"')
		buffer = append(buffer, escapeString(v)...)
		buffer = append(buffer, '"')
		return buffer, nil
	case []byte:
		buf := make([]byte, base64.StdEncoding.EncodedLen(len(v)))
		base64.StdEncoding.Encode(buf, v)
		return append(buffer, buf...), nil
	case bool:
		if v {
			return append(buffer, "true"...), nil
		}
		return append(buffer, "false"...), nil
	case time.Time:
		return strconv.AppendInt(buffer, v.UnixNano(), 10), nil
	case time.Duration:
		return append(strconv.AppendInt(buffer, v.Nanoseconds(), 10), 'i'), nil
	default:
		return buffer, fmt.Errorf("unsupported value type: %T", v)
	}
}

func appendConverted(buffer []byte, val string, column *CsvTableColumn, lineNumber int) ([]byte, error) {
	if len(column.DataType) == 0 { // keep the value as it is
		return append(buffer, val...), nil
	}
	typedVal, err := toTypedValue(val, column, lineNumber)
	if err != nil {
		return buffer, err
	}
	return appendProtocolValue(buffer, typedVal)
}

func decodeNop(reader io.Reader) io.Reader {
	return reader
}

// CreateDecoder creates a decoding reader from the supplied encoding to UTF-8, or returns an error
func CreateDecoder(encoding string) (func(io.Reader) io.Reader, error) {
	if len(encoding) > 0 && encoding != "UTF-8" {
		enc, err := ianaindex.IANA.Encoding(encoding)
		if err != nil {
			return nil, fmt.Errorf("%v, see https://www.iana.org/assignments/character-sets/character-sets.xhtml", err)
		}
		if enc == nil {
			return nil, fmt.Errorf("unsupported encoding: %s", encoding)
		}
		return enc.NewDecoder().Reader, nil
	}
	return decodeNop, nil
}

// createBoolParseFn returns a function that converts a string value to boolean according to format "true,yes,1:false,no,0"
func createBoolParseFn(format string) func(string) (interface{}, error) {
	var err error = nil
	truthy := []string{}
	falsy := []string{}
	if !strings.Contains(format, ":") {
		err = fmt.Errorf("unsupported boolean format: %s should be in 'true,yes,1:false,no,0' format, but no ':' is present", format)
	} else {
		colon := strings.Index(format, ":")
		t := format[:colon]
		f := format[colon+1:]
		if t != "" {
			truthy = strings.Split(t, ",")
		}
		if f != "" {
			falsy = strings.Split(f, ",")
		}
	}
	return func(val string) (interface{}, error) {
		if err != nil {
			return nil, err
		}
		for _, s := range falsy {
			if s == val {
				return false, nil
			}
		}
		for _, s := range truthy {
			if s == val {
				return true, nil
			}
		}
		if len(falsy) == 0 {
			return false, nil
		}
		if len(truthy) == 0 {
			return true, nil
		}

		return nil, fmt.Errorf("unsupported boolean value: %s must one of %v or one of %v", val, truthy, falsy)
	}
}

// createStrictLongParseFn returns a function that converts a string value to long and fails also when a fraction digit is detected
func createStrictLongParseFn(dataFormat string) func(string) (interface{}, error) {
	return func(val string) (interface{}, error) {
		normalized, truncated := normalizeNumberString(val, dataFormat, true)
		if truncated {
			return 0, fmt.Errorf("'%s' cannot fit into long data type", val)
		}
		return strconv.ParseInt(normalized, 10, 64)
	}
}

// createStrictUnsignedLongParseFn returns a function that converts a string value to unsigned long and fails when a fraction digit is detected
func createStrictUnsignedLongParseFn(dataFormat string) func(string) (interface{}, error) {
	return func(val string) (interface{}, error) {
		normalized, truncated := normalizeNumberString(val, dataFormat, true)
		if truncated {
			return 0, fmt.Errorf("'%s' cannot fit into unsignedLong data type", val)
		}
		return strconv.ParseUint(normalized, 10, 64)
	}
}
