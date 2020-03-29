package write

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// see https://v2.docs.influxdata.com/v2.0/reference/syntax/annotated-csv/#valid-data-types
const (
	stringDatatype              = "string"
	doubleDatatype              = "double"
	boolDatatype                = "boolean"
	longDatatype                = "long"
	uLongDatatype               = "unsignedLong"
	durationDatatype            = "duration"
	base64BinaryDataType        = "base64Binary"
	dateTimeDatatype            = "dateTime"
	dateTimeDatatypeRFC3339     = "dateTime:RFC3339"
	dateTimeDatatypeRFC3339Nano = "dateTime:RFC3339Nano"
	dateTimeDatatypeNumber      = "dateTime:number" //the same as long, but serialized without i suffix
)

var supportedDataTypes map[string]struct{}

func init() {
	supportedDataTypes = make(map[string]struct{}, 12)
	supportedDataTypes[stringDatatype] = struct{}{}
	supportedDataTypes[doubleDatatype] = struct{}{}
	supportedDataTypes[boolDatatype] = struct{}{}
	supportedDataTypes[longDatatype] = struct{}{}
	supportedDataTypes[uLongDatatype] = struct{}{}
	supportedDataTypes[durationDatatype] = struct{}{}
	supportedDataTypes[base64BinaryDataType] = struct{}{}
	supportedDataTypes[dateTimeDatatype] = struct{}{}
	supportedDataTypes[dateTimeDatatypeRFC3339] = struct{}{}
	supportedDataTypes[dateTimeDatatypeRFC3339Nano] = struct{}{}
	supportedDataTypes[dateTimeDatatypeNumber] = struct{}{}
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

func toTypedValue(val string, dataType string) (interface{}, error) {
	switch dataType {
	case stringDatatype:
		return val, nil
	case dateTimeDatatype:
		t, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return time.Parse(time.RFC3339, val)
		}
		return time.Unix(0, t).UTC(), nil
	case dateTimeDatatypeRFC3339:
		return time.Parse(time.RFC3339, val)
	case dateTimeDatatypeRFC3339Nano:
		return time.Parse(time.RFC3339Nano, val)
	case dateTimeDatatypeNumber:
		t, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, err
		}
		return time.Unix(0, t).UTC(), nil
	case durationDatatype:
		return time.ParseDuration(val)
	case doubleDatatype:
		return strconv.ParseFloat(val, 64)
	case boolDatatype:
		if val == "true" {
			return true, nil
		} else if val == "false" {
			return false, nil
		}
		return nil, errors.New("Unsupported boolean value '" + val + "' , expected 'true' or 'false'")
	case longDatatype:
		return strconv.ParseInt(val, 10, 64)
	case uLongDatatype:
		return strconv.ParseUint(val, 10, 64)
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

func appendConverted(buffer []byte, val string, dataType string) ([]byte, error) {
	if len(dataType) == 0 { // keep the value as it is
		return append(buffer, val...), nil
	}
	typedVal, err := toTypedValue(val, dataType)
	if err != nil {
		return buffer, err
	}
	return appendProtocolValue(buffer, typedVal)
}
