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

// extractVal extracts an int64 from the aux value types used by date_part.
func extractVal(auxVal interface{}) (int64, error) {
	switch v := auxVal.(type) {
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	case *int64:
		if v != nil {
			return *v, nil
		}
		return 0, nil
	case DecodedDatePartKey:
		return v.Val, nil
	default:
		return 0, fmt.Errorf("date_part: unexpected aux value type: %T", v)
	}
}

// DatePartGrouper implements DimensionGrouper for date_part GROUP BY dimensions.
// All methods are safe for concurrent use — no shared mutable state.
// Encoding buffers use stack-allocated fixed-size arrays to avoid heap allocations.
type DatePartGrouper struct {
	dims []DatePartDimension
}

func NewDatePartGrouper(dims []DatePartDimension) *DatePartGrouper {
	return &DatePartGrouper{dims: dims}
}

// computeDimKey builds a grouping key string.
// Format: "exprName:<8-byte big-endian value>" optionally prefixed with "tagID\x00\x00".
// Uses a stack-allocated [8]byte for the binary encoding.
func computeDimKey(expr DatePartExpr, val int64, tagID string, hasTags bool) string {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(val))
	valStr := string(buf[:])
	if hasTags {
		return tagID + DatePartKeySeparator + expr.String() + ":" + valStr
	}
	return expr.String() + ":" + valStr
}

// encodeKey encodes a dimension value into a 9-byte string (1 byte expr + 8 bytes value)
// that can be stored on a reduce point and later decoded.
// Uses a stack-allocated [9]byte for the binary encoding.
func encodeKey(expr DatePartExpr, val int64) string {
	var buf [9]byte
	buf[0] = byte(expr)
	binary.BigEndian.PutUint64(buf[1:], uint64(val))
	return string(buf[:])
}

// decodeKey decodes a 9-byte encoded key back into a DecodedDatePartKey.
func decodeKey(encodedKey string) (DecodedDatePartKey, error) {
	if len(encodedKey) < 9 {
		return DecodedDatePartKey{}, errors.New("date_part: encoded key too short")
	}
	return DecodedDatePartKey{
		Expr: DatePartExpr(encodedKey[0]),
		Val:  int64(binary.BigEndian.Uint64([]byte(encodedKey[1:9]))),
	}, nil
}

func (g *DatePartGrouper) ResolveKeys(aux []interface{}, tagID string, hasTags bool) ([]GroupingEntry, error) {
	// Check for second-level reduce: aux contains DecodedDatePartKey from a prior emit.
	for _, av := range aux {
		if dpk, ok := av.(DecodedDatePartKey); ok {
			return []GroupingEntry{{
				DimKey:     computeDimKey(dpk.Expr, dpk.Val, tagID, hasTags),
				EncodedKey: encodeKey(dpk.Expr, dpk.Val),
			}}, nil
		}
	}

	// First-level reduce: raw int64 values at end of aux.
	if len(aux) < len(g.dims) {
		return nil, nil
	}
	startIdx := len(aux) - len(g.dims)
	entries := make([]GroupingEntry, 0, len(g.dims))

	for i, dim := range g.dims {
		val, err := extractVal(aux[startIdx+i])
		if err != nil {
			return nil, err
		}

		entries = append(entries, GroupingEntry{
			DimKey:     computeDimKey(dim.Expr, val, tagID, hasTags),
			EncodedKey: encodeKey(dim.Expr, val),
		})
	}
	return entries, nil
}

func (g *DatePartGrouper) DecodeEntry(encodedKey string) (interface{}, error) {
	return decodeKey(encodedKey)
}
