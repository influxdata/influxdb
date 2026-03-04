package query

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
)

const (
	// DatePartString is the name of date_part function
	DatePartString = "date_part"

	// DatePartTimeString is a symbol used to represent a reference variable
	// for the current timestamp from a given point. It is used during time
	// lookup on the query path.
	DatePartTimeString = "date_part_time"

	// DatePartArgCount is the amount of arguments required for date_part function
	DatePartArgCount = 2

	DatePartDimensionsString = "date_part_dimensions"

	// DatePartKeySeparator separates tag IDs from date_part keys in composite
	// grouping keys. Double-null cannot appear in a valid tag ID because
	// InfluxDB disallows empty tag keys and values.
	DatePartKeySeparator = "\x00\x00"
)

type DatePartExpr int

const (
	Year DatePartExpr = iota
	Quarter
	Month
	Week
	Day
	Hour
	Minute
	Second
	Millisecond
	Microsecond
	Nanosecond
	DOW
	DOY
	Epoch
	ISODOW
	Invalid
)

func (d DatePartExpr) String() string {
	switch d {
	case Year:
		return "year"
	case Quarter:
		return "quarter"
	case Month:
		return "month"
	case Week:
		return "week"
	case Day:
		return "day"
	case Hour:
		return "hour"
	case Minute:
		return "minute"
	case Second:
		return "second"
	case Millisecond:
		return "millisecond"
	case Microsecond:
		return "microsecond"
	case Nanosecond:
		return "nanosecond"
	case DOW:
		return "dow"
	case DOY:
		return "doy"
	case Epoch:
		return "epoch"
	case ISODOW:
		return "isodow"
	case Invalid:
		return "invalid"
	}
	return ""
}

var AvailableDatePartExprs = []string{
	"year", "quarter", "month", "week", "day",
	"hour", "minute", "second",
	"millisecond", "microsecond", "nanosecond",
	"dow", "doy", "epoch", "isodow",
}

func ParseDatePartExpr(t string) (DatePartExpr, bool) {
	switch strings.ToLower(t) {
	case "year":
		return Year, true
	case "quarter":
		return Quarter, true
	case "month":
		return Month, true
	case "week":
		return Week, true
	case "day":
		return Day, true
	case "hour":
		return Hour, true
	case "minute":
		return Minute, true
	case "second":
		return Second, true
	case "millisecond":
		return Millisecond, true
	case "microsecond":
		return Microsecond, true
	case "nanosecond":
		return Nanosecond, true
	case "dow":
		return DOW, true
	case "doy":
		return DOY, true
	case "epoch":
		return Epoch, true
	case "isodow":
		return ISODOW, true
	}

	return Invalid, false
}

func ExtractDatePartExpr(t time.Time, expr DatePartExpr) (int64, bool) {
	switch expr {
	case Year:
		return int64(t.Year()), true
	case Quarter:
		month := t.Month()
		return int64((month-1)/3 + 1), true
	case Month:
		return int64(t.Month()), true
	case Week:
		_, week := t.ISOWeek()
		return int64(week), true
	case Day:
		return int64(t.Day()), true
	case Hour:
		return int64(t.Hour()), true
	case Minute:
		return int64(t.Minute()), true
	case Second:
		return int64(t.Second()), true
	case Millisecond:
		return int64(t.Nanosecond() / 1e6), true
	case Microsecond:
		return int64(t.Nanosecond() / 1e3), true
	case Nanosecond:
		return int64(t.Nanosecond()), true
	case DOW:
		return int64(t.Weekday()), true
	case DOY:
		return int64(t.YearDay()), true
	case Epoch:
		return t.Unix(), true
	case ISODOW:
		// Monday=0, Sunday=6
		dow := int64(t.Weekday())
		if dow == 0 {
			return int64(6), true // Sunday
		} else {
			return dow - 1, true
		}
	case Invalid:
		return 0, false
	default:
		return 0, false
	}
}

func ValidateDatePart(args []influxql.Expr) error {
	if exp, got := DatePartArgCount, len(args); exp != got {
		return fmt.Errorf("invalid number of arguments for date_part, expected %d, got %d", exp, got)
	}

	exprStr, ok := args[0].(*influxql.StringLiteral)
	if !ok {
		return errors.New("date_part: first argument must be a string")
	}

	_, ok = ParseDatePartExpr(exprStr.Val)
	if !ok {
		return fmt.Errorf("date_part: first argument must be one of the following: [%s]", strings.Join(AvailableDatePartExprs, ","))
	}

	tstamp, ok := args[1].(*influxql.VarRef)
	if !ok {
		return errors.New("date_part: second argument must be a variable reference")
	} else if !bytes.Equal([]byte(tstamp.Val), models.TimeBytes) {
		// check if tstamp.Val is "time" keyword currently, we only support using time as the second argument
		// this may seem redundant, but we would like to keep consistency with SQL date_part
		return errors.New("date_part: second argument must be time VarRef")
	}

	return nil
}

type DatePartValuer struct {
	Valuer influxql.MapValuer
}

var _ influxql.CallValuer = DatePartValuer{}

func (v DatePartValuer) Value(key string) (interface{}, bool) {
	if v.Valuer == nil {
		return nil, false
	}
	// Convert the special date_part symbol back to "time"
	if key == DatePartTimeString {
		key = models.TimeString
	}
	return v.Valuer.Value(key)
}

func (DatePartValuer) Call(name string, args []interface{}) (interface{}, bool) {
	if name != DatePartString {
		return nil, false
	}
	if len(args) != DatePartArgCount {
		return nil, false
	}

	exprStr, ok := args[0].(string)
	if !ok {
		return nil, false
	}

	expr, ok := ParseDatePartExpr(exprStr)
	if !ok {
		return nil, false
	}

	timestampRaw, ok := args[1].(int64)
	if !ok {
		return nil, false
	}

	timestamp := time.Unix(0, timestampRaw).UTC()
	return ExtractDatePartExpr(timestamp, expr)
}

type DatePartDimension struct {
	Name string
	Expr DatePartExpr
}

type DecodedDatePartKey struct {
	Expr DatePartExpr
	Val  int64
}

func DecodeDatePartDimension(dims []DatePartDimension, datePartKey string) ([]DecodedDatePartKey, error) {
	output := make([]DecodedDatePartKey, len(dims))

	for i, dim := range dims {
		output[i].Expr = dim.Expr
		if len([]byte(datePartKey)) < (i+1)*8 {
			return nil, errors.New("DecodeDatePartDimension(): datePartKey length not within required range")
		}
		output[i].Val = int64(binary.BigEndian.Uint64([]byte(datePartKey[i*8 : (i+1)*8])))
	}

	return output, nil
}

// ComputeDatePartKeyFromValues creates a date_part key from pre-computed values in the Aux field.
// This is used when timestamps have been normalized and we need to extract the grouping key from Aux.
func ComputeDatePartKeyFromValues(dims []DatePartDimension, auxValues []interface{}) (string, error) {
	if len(auxValues) < len(dims) {
		return "", errors.New("ComputeDatePartKeyFromValues: not enough aux values for date_part dimensions")
	}

	buf := make([]byte, len(dims)*8)
	for i := range dims {
		// The aux values are stored as int64 values
		var val int64
		switch v := auxValues[i].(type) {
		case int64:
			val = v
		case float64:
			val = int64(v)
		case *int64:
			if v != nil {
				val = *v
			}
		case DecodedDatePartKey:
			val = v.Val
		default:
			return "", fmt.Errorf("ComputeDatePartKeyFromValues: unexpected aux value type: %T", v)
		}
		binary.BigEndian.PutUint64(buf[i*8:], uint64(val))
	}

	return string(buf), nil
}
