package query

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxql"
)

const (
	DatePartString     = "date_part"
	DatePartTimeString = "date_part_time"
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
)

var AvailableDatePartExprs = []string{
	"year", "quarter", "month", "week", "day",
	"hour", "minute", "second",
	"millisecond", "microsecond", "nanosecond",
	"dow", "doy", "epoch", "isodow",
}

var timeBytes = []byte("time")

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

	return 0, false
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
	default:
		return 0, false
	}
}

func ValidateDatePart(args []influxql.Expr) (*influxql.VarRef, DatePartExpr, error) {
	if exp, got := 2, len(args); exp != got {
		return nil, 0, fmt.Errorf("invalid number of arguments for date_part, expected %d, got %d", exp, got)
	}

	exprStr, ok := args[0].(*influxql.StringLiteral)
	if !ok {
		return nil, 0, errors.New("date_part: first argument must be a string")
	}

	expression, ok := ParseDatePartExpr(exprStr.Val)
	if !ok {
		return nil, 0, fmt.Errorf("date_part: first argument must be one of the following: [%s]", strings.Join(AvailableDatePartExprs, ","))
	}

	tstamp, ok := args[1].(*influxql.VarRef)
	if !ok {
		return nil, 0, errors.New("date_part: second argument must be a variable reference")
	} else if !bytes.Equal([]byte(tstamp.Val), timeBytes) {
		// check if tstamp.Val is "time" keyword currently, we only support using time as the second argument
		// this may seem redundant, but we would like to keep consistency with SQL date_part
		return nil, 0, errors.New("date_part: second argument must be time VarRef")
	}

	return tstamp, expression, nil
}

type DatePartValuer struct {
	Valuer influxql.MapValuer
}

var _ influxql.CallValuer = DatePartValuer{}

func (v DatePartValuer) Value(key string) (interface{}, bool) {
	// Convert the special date_part symbol back to "time"
	if key == DatePartTimeString {
		key = "time"
	}
	return v.Valuer.Value(key)
}

func (DatePartValuer) Call(name string, args []interface{}) (interface{}, bool) {
	if name != DatePartString {
		return nil, false
	}
	if len(args) != 2 {
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
