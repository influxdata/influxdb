package query

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/influxdata/influxql"
	"strings"
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

func ValidateDatePart(args []influxql.Expr) (*influxql.VarRef, DatePartExpr, error) {
	if exp, got := 2, len(args); exp != got {
		return nil, 0, fmt.Errorf("invalid number of arguments for date_part, expected %d, got %d", exp, got)
	}

	tstamp, ok := args[0].(*influxql.VarRef)
	if !ok {
		return nil, 0, errors.New("date_part: first argument must be a string")
		// check if tstamp.Val is "time" keyword or an actual timestamp
	} else if !bytes.Equal([]byte(tstamp.Val), timeBytes) {
		lit := influxql.StringLiteral{Val: tstamp.Val}
		if !lit.IsTimeLiteral() {
			return nil, 0, errors.New("date_part: first argument must be a timestamp or 'time' keyword")
		}
	}

	expressionRef, ok := args[1].(*influxql.VarRef)
	if !ok {
		return nil, 0, errors.New("date_part: second argument must be a string")
	}

	expression, ok := ParseDatePartExpr(expressionRef.Val)
	if !ok {
		return nil, 0, fmt.Errorf("date_part: second argument must be one of the following: [%s]", strings.Join(AvailableDatePartExprs, ","))
	}

	return tstamp, expression, nil
}
