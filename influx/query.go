package influx

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/influxdb/influxql"
)

// TimeRangeAsEpochNano extracs the min and max epoch times from the expression
func TimeRangeAsEpochNano(expr influxql.Expr, now time.Time) (min, max int64, err error) {
	tmin, tmax, err := influxql.TimeRange(expr)
	if err != nil {
		return 0, 0, err
	}
	if tmin.IsZero() {
		min = time.Unix(0, influxql.MinTime).UnixNano()
	} else {
		min = tmin.UnixNano()
	}
	if tmax.IsZero() {
		max = now.UnixNano()
	} else {
		max = tmax.UnixNano()
	}
	return
}

// WhereToken is used to parse the time expression from an influxql query
const WhereToken = "WHERE"

// ParseTime extracts the duration of the time range of the query
func ParseTime(influxQL string, now time.Time) (time.Duration, error) {
	start := strings.Index(strings.ToUpper(influxQL), WhereToken)
	if start == -1 {
		return 0, fmt.Errorf("not a relative duration")
	}
	start += len(WhereToken)
	where := influxQL[start:]
	cond, err := influxql.ParseExpr(where)
	if err != nil {
		return 0, err
	}
	nowVal := &influxql.NowValuer{
		Now: now,
	}
	cond = influxql.Reduce(cond, nowVal)
	min, max, err := TimeRangeAsEpochNano(cond, now)
	if err != nil {
		return 0, err
	}
	dur := time.Duration(max - min)
	if dur < 0 {
		dur = 0
	}
	return dur, nil
}

// Convert changes an InfluxQL query to a QueryConfig
func Convert(influxQL string) (chronograf.QueryConfig, error) {
	itsDashboardTime := false
	intervalTime := false
	if strings.Contains(influxQL, ":interval:") {
		influxQL = strings.Replace(influxQL, ":interval:", "time(1234s)", 1)
		intervalTime = true
	}

	if strings.Contains(influxQL, ":dashboardTime:") {
		influxQL = strings.Replace(influxQL, ":dashboardTime:", "now() - 15m", 1)
		itsDashboardTime = true
	}

	query, err := influxql.ParseQuery(influxQL)
	if err != nil {
		return chronograf.QueryConfig{}, err
	}

	if itsDashboardTime {
		influxQL = strings.Replace(influxQL, "now() - 15m", ":dashboardTime:", 1)
	}

	if intervalTime {
		influxQL = strings.Replace(influxQL, "time(1234s)", ":interval:", 1)
	}

	raw := chronograf.QueryConfig{
		RawText: &influxQL,
		Fields:  []chronograf.Field{},
		GroupBy: chronograf.GroupBy{
			Tags: []string{},
		},
		Tags: make(map[string][]string, 0),
	}
	qc := chronograf.QueryConfig{
		GroupBy: chronograf.GroupBy{
			Tags: []string{},
		},
		Tags: make(map[string][]string, 0),
	}

	if len(query.Statements) != 1 {
		return raw, nil
	}

	stmt, ok := query.Statements[0].(*influxql.SelectStatement)
	if !ok {
		return raw, nil
	}

	// Query config doesn't support limits
	if stmt.Limit != 0 || stmt.Offset != 0 || stmt.SLimit != 0 || stmt.SOffset != 0 {
		return raw, nil
	}

	// Query config doesn't support sorting
	if len(stmt.SortFields) > 0 {
		return raw, nil
	}

	// Query config doesn't allow SELECT INTO
	if stmt.Target != nil {
		return raw, nil
	}

	// Query config only allows selecting from one source at a time.
	if len(stmt.Sources) != 1 {
		return raw, nil
	}

	src := stmt.Sources[0]
	measurement, ok := src.(*influxql.Measurement)
	if !ok {
		return raw, nil
	}

	if measurement.Regex != nil {
		return raw, nil
	}
	qc.Database = measurement.Database
	qc.RetentionPolicy = measurement.RetentionPolicy
	qc.Measurement = measurement.Name

	for _, dim := range stmt.Dimensions {
		switch v := dim.Expr.(type) {
		default:
			return raw, nil
		case *influxql.Call:
			if v.Name != "time" {
				return raw, nil
			}
			// Make sure there is exactly one argument.
			if len(v.Args) != 1 {
				return raw, nil
			}
			// Ensure the argument is a duration.
			lit, ok := v.Args[0].(*influxql.DurationLiteral)
			if !ok {
				return raw, nil
			}
			if intervalTime {
				qc.GroupBy.Time = "auto"
			} else {
				qc.GroupBy.Time = lit.String()
			}
			// Add fill to queryConfig only if there's a `GROUP BY time`
			switch stmt.Fill {
			case influxql.NullFill:
				qc.Fill = "null"
			case influxql.NoFill:
				qc.Fill = "none"
			case influxql.NumberFill:
				qc.Fill = fmt.Sprint(stmt.FillValue)
			case influxql.PreviousFill:
				qc.Fill = "previous"
			case influxql.LinearFill:
				qc.Fill = "linear"
			default:
				return raw, nil
			}
		case *influxql.VarRef:
			qc.GroupBy.Tags = append(qc.GroupBy.Tags, v.Val)
		}
	}

	qc.Fields = []chronograf.Field{}
	for _, fld := range stmt.Fields {
		switch f := fld.Expr.(type) {
		default:
			return raw, nil
		case *influxql.Call:
			// only support certain query config functions
			if _, ok = supportedFuncs[f.Name]; !ok {
				return raw, nil
			}

			fldArgs := []chronograf.Field{}
			for _, arg := range f.Args {
				switch ref := arg.(type) {
				case *influxql.VarRef:
					fldArgs = append(fldArgs, chronograf.Field{
						Value: ref.Val,
						Type:  "field",
					})
				case *influxql.IntegerLiteral:
					fldArgs = append(fldArgs, chronograf.Field{
						Value: strconv.FormatInt(ref.Val, 10),
						Type:  "integer",
					})
				case *influxql.NumberLiteral:
					fldArgs = append(fldArgs, chronograf.Field{
						Value: strconv.FormatFloat(ref.Val, 'f', -1, 64),
						Type:  "number",
					})
				case *influxql.RegexLiteral:
					fldArgs = append(fldArgs, chronograf.Field{
						Value: ref.Val.String(),
						Type:  "regex",
					})
				case *influxql.Wildcard:
					fldArgs = append(fldArgs, chronograf.Field{
						Value: "*",
						Type:  "wildcard",
					})
				default:
					return raw, nil
				}
			}

			qc.Fields = append(qc.Fields, chronograf.Field{
				Value: f.Name,
				Type:  "func",
				Alias: fld.Alias,
				Args:  fldArgs,
			})
		case *influxql.VarRef:
			if f.Type != influxql.Unknown {
				return raw, nil
			}
			qc.Fields = append(qc.Fields, chronograf.Field{
				Value: f.Val,
				Type:  "field",
				Alias: fld.Alias,
			})
		}
	}

	if stmt.Condition == nil {
		return qc, nil
	}

	reduced := influxql.Reduce(stmt.Condition, nil)
	logic, ok := isTagLogic(reduced)
	if !ok {
		return raw, nil
	}

	ops := map[string]bool{}
	for _, l := range logic {
		values, ok := qc.Tags[l.Tag]
		if !ok {
			values = []string{}
		}
		ops[l.Op] = true
		values = append(values, l.Value)
		qc.Tags[l.Tag] = values
	}

	if len(logic) > 0 {
		if len(ops) != 1 {
			return raw, nil
		}
		if _, ok := ops["=="]; ok {
			qc.AreTagsAccepted = true
		}
	}

	// If the condition has a time range we report back its duration
	if dur, ok := hasTimeRange(stmt.Condition); ok {
		if !itsDashboardTime {
			qc.Range = &chronograf.DurationRange{
				Lower: "now() - " + shortDur(dur),
			}
		} else {
			strings.Replace(influxQL, "now() - 15m", ":dashboardTime:", 1)
		}
	}

	return qc, nil
}

// tagFilter represents a single tag that is filtered by some condition
type tagFilter struct {
	Op    string
	Tag   string
	Value string
}

func isTime(exp influxql.Expr) bool {
	if p, ok := exp.(*influxql.ParenExpr); ok {
		return isTime(p.Expr)
	} else if ref, ok := exp.(*influxql.VarRef); ok && strings.ToLower(ref.Val) == "time" {
		return true
	}
	return false
}

func isNow(exp influxql.Expr) bool {
	if p, ok := exp.(*influxql.ParenExpr); ok {
		return isNow(p.Expr)
	} else if call, ok := exp.(*influxql.Call); ok && strings.ToLower(call.Name) == "now" && len(call.Args) == 0 {
		return true
	}
	return false
}

func isDuration(exp influxql.Expr) (time.Duration, bool) {
	switch e := exp.(type) {
	case *influxql.ParenExpr:
		return isDuration(e.Expr)
	case *influxql.DurationLiteral:
		return e.Val, true
	case *influxql.NumberLiteral, *influxql.IntegerLiteral, *influxql.TimeLiteral:
		return 0, false
	}
	return 0, false
}

func isPreviousTime(exp influxql.Expr) (time.Duration, bool) {
	if p, ok := exp.(*influxql.ParenExpr); ok {
		return isPreviousTime(p.Expr)
	} else if bin, ok := exp.(*influxql.BinaryExpr); ok {
		now := isNow(bin.LHS) || isNow(bin.RHS) // either side can be now
		op := bin.Op == influxql.SUB
		dur, hasDur := isDuration(bin.LHS)
		if !hasDur {
			dur, hasDur = isDuration(bin.RHS)
		}
		return dur, now && op && hasDur
	} else if isNow(exp) { // just comparing to now
		return 0, true
	}
	return 0, false
}

func isTimeRange(exp influxql.Expr) (time.Duration, bool) {
	if p, ok := exp.(*influxql.ParenExpr); ok {
		return isTimeRange(p.Expr)
	} else if bin, ok := exp.(*influxql.BinaryExpr); ok {
		tm := isTime(bin.LHS) || isTime(bin.RHS) // Either side could be time
		op := false
		switch bin.Op {
		case influxql.LT, influxql.LTE, influxql.GT, influxql.GTE:
			op = true
		}
		dur, prev := isPreviousTime(bin.LHS)
		if !prev {
			dur, prev = isPreviousTime(bin.RHS)
		}
		return dur, tm && op && prev
	}
	return 0, false
}

func hasTimeRange(exp influxql.Expr) (time.Duration, bool) {
	v := &timeRangeVisitor{}
	influxql.Walk(v, exp)
	return v.Duration, v.Ok
}

// timeRangeVisitor implements influxql.Visitor to search for time ranges
type timeRangeVisitor struct {
	Duration time.Duration
	Ok       bool
}

func (v *timeRangeVisitor) Visit(n influxql.Node) influxql.Visitor {
	if exp, ok := n.(influxql.Expr); !ok {
		return nil
	} else if dur, ok := isTimeRange(exp); ok {
		v.Duration = dur
		v.Ok = ok
		return nil
	}
	return v
}

func isTagLogic(exp influxql.Expr) ([]tagFilter, bool) {
	if p, ok := exp.(*influxql.ParenExpr); ok {
		return isTagLogic(p.Expr)
	}

	if _, ok := isTimeRange(exp); ok {
		return nil, true
	} else if tf, ok := isTagFilter(exp); ok {
		return []tagFilter{tf}, true
	}

	bin, ok := exp.(*influxql.BinaryExpr)
	if !ok {
		return nil, false
	}

	lhs, lhsOK := isTagFilter(bin.LHS)
	rhs, rhsOK := isTagFilter(bin.RHS)

	if lhsOK && rhsOK && lhs.Tag == rhs.Tag && lhs.Op == rhs.Op && bin.Op == influxql.OR {
		return []tagFilter{lhs, rhs}, true
	}

	if bin.Op != influxql.AND && bin.Op != influxql.OR {
		return nil, false
	}

	_, tm := isTimeRange(bin.LHS)
	if !tm {
		_, tm = isTimeRange(bin.RHS)
	}
	tf := lhsOK || rhsOK
	if tm && tf {
		if lhsOK {
			return []tagFilter{lhs}, true
		}
		return []tagFilter{rhs}, true
	}

	tlLHS, lhsOK := isTagLogic(bin.LHS)
	tlRHS, rhsOK := isTagLogic(bin.RHS)
	if lhsOK && rhsOK {
		ops := map[string]bool{} // there must only be one kind of ops
		for _, tf := range tlLHS {
			ops[tf.Op] = true
		}
		for _, tf := range tlRHS {
			ops[tf.Op] = true
		}
		if len(ops) > 1 {
			return nil, false
		}
		return append(tlLHS, tlRHS...), true
	}
	return nil, false
}

func isVarRef(exp influxql.Expr) bool {
	if p, ok := exp.(*influxql.ParenExpr); ok {
		return isVarRef(p.Expr)
	} else if _, ok := exp.(*influxql.VarRef); ok {
		return true
	}
	return false
}

func isString(exp influxql.Expr) bool {
	if p, ok := exp.(*influxql.ParenExpr); ok {
		return isString(p.Expr)
	} else if _, ok := exp.(*influxql.StringLiteral); ok {
		return true
	}
	return false
}

func isTagFilter(exp influxql.Expr) (tagFilter, bool) {
	switch expr := exp.(type) {
	default:
		return tagFilter{}, false
	case *influxql.ParenExpr:
		return isTagFilter(expr.Expr)
	case *influxql.BinaryExpr:
		var Op string
		if expr.Op == influxql.EQ {
			Op = "=="
		} else if expr.Op == influxql.NEQ {
			Op = "!="
		} else {
			return tagFilter{}, false
		}

		hasValue := isString(expr.LHS) || isString(expr.RHS)
		hasTag := isVarRef(expr.LHS) || isVarRef(expr.RHS)
		if !(hasValue && hasTag) {
			return tagFilter{}, false
		}

		value := ""
		tag := ""
		// Either tag op value or value op tag
		if isVarRef(expr.LHS) {
			t, _ := expr.LHS.(*influxql.VarRef)
			tag = t.Val
			v, _ := expr.RHS.(*influxql.StringLiteral)
			value = v.Val
		} else {
			t, _ := expr.RHS.(*influxql.VarRef)
			tag = t.Val
			v, _ := expr.LHS.(*influxql.StringLiteral)
			value = v.Val
		}

		return tagFilter{
			Op:    Op,
			Tag:   tag,
			Value: value,
		}, true
	}
}

var supportedFuncs = map[string]bool{
	"mean":       true,
	"median":     true,
	"count":      true,
	"min":        true,
	"max":        true,
	"sum":        true,
	"first":      true,
	"last":       true,
	"spread":     true,
	"stddev":     true,
	"percentile": true,
	"top":        true,
	"bottom":     true,
}

// shortDur converts duration into the queryConfig duration format
func shortDur(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	return s
}
