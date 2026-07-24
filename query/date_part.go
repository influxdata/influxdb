package query

import (
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
	// lookup on the query path. The leading NUL byte makes it impossible to
	// collide with a user field or tag name (those originate from InfluxQL
	// identifiers, which can never contain a NUL), so a field or tag literally
	// named "date_part_time" is not shadowed by this sentinel.
	DatePartTimeString = "\x00date_part_time"

	// DatePartArgCount is the amount of arguments required for date_part function
	DatePartArgCount = 2

	// DatePartDimensionsString is the internal eval-map key under which the active
	// GROUP BY date_part dimension value is published for a scanned row. The
	// leading NUL byte makes it impossible to collide with a user field or tag
	// name (those originate from InfluxQL identifiers, which can never contain a
	// NUL), so selecting a series with a field/tag literally named
	// "date_part_dimensions" is not corrupted by date_part grouping.
	DatePartDimensionsString = "\x00date_part_dimensions"
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

// datePartNames is the single source of truth for the canonical part names;
// String and ParseDatePartExpr both derive from it so they cannot drift.
var datePartNames = [...]string{
	Year:        "year",
	Quarter:     "quarter",
	Month:       "month",
	Week:        "week",
	Day:         "day",
	Hour:        "hour",
	Minute:      "minute",
	Second:      "second",
	Millisecond: "millisecond",
	Microsecond: "microsecond",
	Nanosecond:  "nanosecond",
	DOW:         "dow",
	DOY:         "doy",
	Epoch:       "epoch",
	ISODOW:      "isodow",
	Invalid:     "invalid",
}

var datePartsByName = func() map[string]DatePartExpr {
	m := make(map[string]DatePartExpr, Invalid)
	for part := Year; part < Invalid; part++ {
		m[datePartNames[part]] = part
	}
	return m
}()

func (d DatePartExpr) String() string {
	if d < Year || d > Invalid {
		return ""
	}
	return datePartNames[d]
}

func ParseDatePartExpr(t string) (DatePartExpr, bool) {
	part, ok := datePartsByName[strings.ToLower(t)]
	if !ok {
		return Invalid, false
	}
	return part, true
}

// matchDatePartCall reports whether n is a well-formed date_part call —
// date_part('<part>', time) with a recognized part — and returns the parsed
// part. Anything else, including a date_part call with a malformed argument
// list, does not match.
func matchDatePartCall(n influxql.Node) (DatePartExpr, bool) {
	call, ok := n.(*influxql.Call)
	if !ok || call.Name != DatePartString || len(call.Args) != DatePartArgCount {
		return Invalid, false
	}
	lit, ok := call.Args[0].(*influxql.StringLiteral)
	if !ok {
		return Invalid, false
	}
	ref, ok := call.Args[1].(*influxql.VarRef)
	if !ok || ref.Val != models.TimeString {
		return Invalid, false
	}
	return ParseDatePartExpr(lit.Val)
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
		// Seconds-of-minute scaled to milliseconds, plus the sub-second component.
		return int64(t.Second())*1000 + int64(t.Nanosecond())/1_000_000, true
	case Microsecond:
		// Seconds-of-minute scaled to microseconds, plus the sub-second component.
		return int64(t.Second())*1_000_000 + int64(t.Nanosecond())/1_000, true
	case Nanosecond:
		// Seconds-of-minute scaled to nanoseconds, plus the sub-second component.
		return int64(t.Second())*1_000_000_000 + int64(t.Nanosecond()), true
	case DOW:
		return int64(t.Weekday()), true
	case DOY:
		return int64(t.YearDay()), true
	case Epoch:
		// Whole seconds since the Unix epoch. Sub-second precision is truncated by
		// the int64 return type; select the millisecond/microsecond/nanosecond
		// part for finer resolution.
		return t.Unix(), true
	case ISODOW:
		// ISO 8601 day of the week: Monday=1 ... Sunday=7.
		// Go's time.Weekday() is Sunday=0 ... Saturday=6, so every weekday
		// already maps onto its ISO value except Sunday, which becomes 7.
		dow := int64(t.Weekday())
		if dow == 0 {
			return int64(7), true // Sunday
		}
		return dow, true
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
		valid := make([]string, 0, Invalid)
		for i := Year; i < Invalid; i++ {
			valid = append(valid, i.String())
		}
		return fmt.Errorf("date_part: first argument must be one of the following: [%s]", strings.Join(valid, ", "))
	}

	tstamp, ok := args[1].(*influxql.VarRef)
	if !ok {
		return errors.New("date_part: second argument must be a variable reference")
	} else if tstamp.Val != models.TimeString {
		// check if tstamp.Val is "time" keyword currently, we only support using time as the second argument
		// this may seem redundant, but we would like to keep consistency with SQL date_part
		return errors.New("date_part: second argument must be time VarRef")
	}

	return nil
}

// exprContainsDatePart reports whether expr contains a call to the date_part
// function at any nesting depth. A nil expr contains none.
func exprContainsDatePart(expr influxql.Expr) bool {
	if expr == nil {
		return false
	}
	found := false
	influxql.WalkFunc(expr, func(n influxql.Node) {
		if call, ok := n.(*influxql.Call); ok && call.Name == DatePartString {
			found = true
		}
	})
	return found
}

// Sentinel errors returned by date_part validation. Named values so tests can
// reference them (via export_test.go) instead of duplicating the strings.
var (
	errDatePartRequiresAggregate = errors.New("date_part: GROUP BY date_part requires an aggregate or selector function")
	errDatePartSingleAggregate   = errors.New("date_part: GROUP BY date_part supports only a single aggregate or selector function")
	errDatePartFillPrevious      = errors.New("date_part: fill(previous) is not supported with GROUP BY date_part")
	errDatePartFillLinear        = errors.New("date_part: fill(linear) is not supported with GROUP BY date_part")
	errDatePartFillValue         = errors.New("date_part: fill(<value>) is not supported with GROUP BY date_part")
	errDatePartFillNull          = errors.New("date_part: fill(null) is not supported with GROUP BY time() and date_part; use fill(none)")
)

// validateDatePartSelectFields rejects an explicit date_part('part', time) in the
// SELECT list whose part is not one of the GROUP BY date_part dimensions, when the
// query groups by date_part. Under such grouping the emitted row's timestamp is the
// bucket's representative time (not a per-row time), so a non-grouped date_part has
// no well-defined value for the group and would silently return misleading data.
//
// Queries without a date_part GROUP BY are unaffected: raw queries evaluate
// date_part against each point's real timestamp, and GROUP BY time() buckets carry a
// meaningful timestamp, both of which are correct.
func (c *compiledStatement) validateDatePartSelectFields(stmt *influxql.SelectStatement) error {
	return validateDatePartFields(stmt, c.FillOption, !c.Interval.IsZero())
}

// validateDatePartTree runs validateDatePartFields over a statement and every
// subquery source beneath it, deriving the fill option and interval from each
// statement itself. This is the Prepare-time re-run: RewriteFields rewrites the
// whole statement tree, so a wildcard expanded inside a subquery (which the
// per-statement compile passes ran before expansion) is only visible here.
func validateDatePartTree(stmt *influxql.SelectStatement, subquery bool) error {
	interval, err := stmt.GroupByInterval()
	if err != nil {
		return err
	}
	fill := stmt.Fill
	// Subquery compilation rewrites a redundant fill(null) with an interval to
	// fill(none) (see (*compiledStatement).subquery). Mirror that here so this
	// pass does not reject a shape the executed plan never produces.
	if subquery && interval > 0 && fill == influxql.NullFill {
		fill = influxql.NoFill
	}
	if err := validateDatePartFields(stmt, fill, interval > 0); err != nil {
		return err
	}
	for _, source := range stmt.Sources {
		if sub, ok := source.(*influxql.SubQuery); ok {
			if err := validateDatePartTree(sub.Statement, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// datePartStreamCalls lists the stream-based functions. Their reducers process
// points in timestamp order keyed on tags only and never consult the date_part
// grouper, so combining them with GROUP BY date_part would silently flatten
// the groups into one series. count_hll qualifies too: its inner sum_hll stage
// groups, but the outer stream stage merges the groups back together.
var datePartStreamCalls = map[string]struct{}{
	"count_hll":                         {},
	"derivative":                        {},
	"non_negative_derivative":           {},
	"difference":                        {},
	"non_negative_difference":           {},
	"elapsed":                           {},
	"moving_average":                    {},
	"exponential_moving_average":        {},
	"double_exponential_moving_average": {},
	"triple_exponential_moving_average": {},
	"triple_exponential_derivative":     {},
	"relative_strength_index":           {},
	"kaufmans_efficiency_ratio":         {},
	"kaufmans_adaptive_moving_average":  {},
	"chande_momentum_oscillator":        {},
	"cumulative_sum":                    {},
	"integral":                          {},
	"holt_winters":                      {},
	"holt_winters_with_fit":             {},
}

// datePartAnchorCalls collects the outermost non-math, non-date_part function
// calls in the SELECT fields. Math functions are transparent (their arguments
// may hold the anchoring call); aggregate/selector arguments are not descended
// into, so a nested shape like count(distinct(value)) counts once. Unlike
// c.FunctionCalls this is derived from the statement, so it sees the fields
// RewriteFields expanded from a wildcard.
func datePartAnchorCalls(fields influxql.Fields) []*influxql.Call {
	var calls []*influxql.Call
	var walk func(expr influxql.Expr)
	walk = func(expr influxql.Expr) {
		switch e := expr.(type) {
		case *influxql.Call:
			if e.Name == DatePartString {
				return
			}
			if isMathFunction(e) {
				for _, arg := range e.Args {
					walk(arg)
				}
				return
			}
			calls = append(calls, e)
		case *influxql.BinaryExpr:
			walk(e.LHS)
			walk(e.RHS)
		case *influxql.ParenExpr:
			walk(e.Expr)
		case *influxql.Distinct:
			calls = append(calls, e.NewCall())
		}
	}
	for _, f := range fields {
		walk(f.Expr)
	}
	return calls
}

func validateDatePartFields(stmt *influxql.SelectStatement, fillOption influxql.FillOption, hasInterval bool) error {
	groupByParts := make(map[DatePartExpr]struct{})
	for _, d := range stmt.Dimensions {
		if part, ok := matchDatePartCall(d.Expr); ok {
			groupByParts[part] = struct{}{}
		}
	}
	if len(groupByParts) == 0 {
		return nil
	}

	// GROUP BY date_part is implemented by a single reduce/grouper per query.
	// The raw (no-aggregate) path takes the aux-cursor branch and does no grouping
	// at all, silently returning one flat ungrouped series. The multi-aggregate
	// path aligns the per-call scanners on (ts, name, tags) only and merges their
	// values under a single shared date_part key, so each call's group value
	// overwrites the others (mislabeled results). Require exactly one non-date_part
	// aggregate or selector call so neither broken shape can compile.
	anchorCalls := datePartAnchorCalls(stmt.Fields)
	if len(anchorCalls) == 0 {
		return errDatePartRequiresAggregate
	}
	if len(anchorCalls) > 1 {
		return errDatePartSingleAggregate
	}
	if _, ok := datePartStreamCalls[anchorCalls[0].Name]; ok {
		return fmt.Errorf("date_part: %s() is not supported with GROUP BY date_part", anchorCalls[0].Name)
	}

	// Value-carrying fill modes (previous/linear/<number>) synthesize values for
	// empty windows. For a GROUP BY date_part dimension this would leak a value
	// into a series where that dimension is not active, so reject those modes.
	//
	// fill(null) (the default) is safe for a bare GROUP BY date_part, but when it
	// is combined with a time() interval the fill iterator emits empty-window rows
	// that carry no DecodedDatePartKey: their grouping value is lost and the
	// emitter splits them into spurious extra series, fragmenting the real ones.
	// Reject fill(null) only in that combined case (use fill(none) instead).
	// fill(none) is always unaffected (it produces no fill iterator).
	switch fillOption {
	case influxql.PreviousFill:
		return errDatePartFillPrevious
	case influxql.LinearFill:
		return errDatePartFillLinear
	case influxql.NumberFill:
		return errDatePartFillValue
	case influxql.NullFill:
		if hasInterval {
			return errDatePartFillNull
		}
	}

	// GROUP BY date_part injects an output column named after the canonical part
	// (e.g. "year"). Reject a user-selected field/alias of the same name: the
	// duplicate column names collapse in column-name-keyed result handling (e.g.
	// SELECT INTO via convertRowToPoints), silently dropping data.
	injected := make(map[string]struct{}, len(groupByParts))
	for part := range groupByParts {
		injected[part.String()] = struct{}{}
	}
	for _, f := range stmt.Fields {
		if _, ok := injected[f.Name()]; ok {
			return fmt.Errorf("date_part: output column %q collides with the GROUP BY date_part('%s', time) dimension; alias the field to a different name", f.Name(), f.Name())
		}
		// top/bottom tag arguments become output columns named after the tag
		// (see buildTopBottomIterator), so they collide the same way.
		if call, ok := f.Expr.(*influxql.Call); ok && (call.Name == "top" || call.Name == "bottom") && len(call.Args) > 2 {
			for _, arg := range call.Args[1 : len(call.Args)-1] {
				if ref, ok := arg.(*influxql.VarRef); ok {
					if _, ok := injected[ref.Val]; ok {
						return fmt.Errorf("date_part: %s() tag argument %q collides with the GROUP BY date_part('%s', time) dimension", call.Name, ref.Val, ref.Val)
					}
				}
			}
		}
	}

	// A GROUP BY tag with the same name collides with the injected column too:
	// column-name-keyed handling (e.g. SELECT INTO promoting grouping columns to
	// tags) would silently overwrite the real tag's value with the part value.
	for _, d := range stmt.Dimensions {
		if ref, ok := d.Expr.(*influxql.VarRef); ok {
			if _, ok := injected[ref.Val]; ok {
				return fmt.Errorf("date_part: GROUP BY dimension %q collides with the GROUP BY date_part('%s', time) dimension", ref.Val, ref.Val)
			}
		}
	}

	// Over a subquery source, a reference to an injected part name resolves to
	// the grouping driver (datePartMap) before the subquery's fields (see
	// subqueryBuilder.mapAuxField), so a subquery column of the same name is
	// silently shadowed by the extracted part value. Reject the reference when
	// the subquery actually emits the colliding column; an unreferenced column
	// is harmless and aliasing it in the subquery lifts the restriction.
	referenced := make(map[string]struct{})
	collectRefs := func(n influxql.Node) {
		if ref, ok := n.(*influxql.VarRef); ok {
			referenced[ref.Val] = struct{}{}
		}
	}
	for _, f := range stmt.Fields {
		influxql.WalkFunc(f.Expr, collectRefs)
	}
	if stmt.Condition != nil {
		influxql.WalkFunc(stmt.Condition, collectRefs)
	}
	for _, src := range stmt.Sources {
		sub, ok := src.(*influxql.SubQuery)
		if !ok {
			continue
		}
		for _, col := range sub.Statement.ColumnNames() {
			if _, ok := injected[col]; !ok {
				continue
			}
			if _, ok := referenced[col]; ok {
				return fmt.Errorf("date_part: subquery column %q is shadowed by the GROUP BY date_part('%s', time) dimension; alias the column in the subquery to a different name", col, col)
			}
		}
	}

	var badPart string
	for _, f := range stmt.Fields {
		influxql.WalkFunc(f.Expr, func(n influxql.Node) {
			if badPart != "" {
				return
			}
			part, ok := matchDatePartCall(n)
			if !ok {
				return
			}
			if _, ok := groupByParts[part]; !ok {
				badPart = part.String()
			}
		})
		if badPart != "" {
			return fmt.Errorf("date_part: SELECT date_part('%s', time) requires '%s' to be a GROUP BY date_part dimension", badPart, badPart)
		}
	}
	return nil
}

// validateDatePartAnchor rejects a SELECT that uses date_part(...) but has no
// real anchor to drive the scan. date_part derives its value purely from the row
// timestamp, so it cannot itself produce points; it must be paired with a stored
// field or a non-date_part aggregate/selector. A bare tag reference is not an
// anchor (the storage engine cannot emit timestamps from a tag-only cursor), so a
// query like `SELECT host, date_part('year', time) FROM cpu` would otherwise plan
// as an aux-only iterator and silently return no rows.
//
// This runs after RewriteFields, once VarRef types (field vs tag) are known: that
// distinction is not available during compilation, where HasAuxiliaryFields is set
// for any bare VarRef including tags, so the compile-time check cannot catch it.
func validateDatePartAnchor(stmt *influxql.SelectStatement) error {
	var hasDatePart, hasAnchor bool
	for _, f := range stmt.Fields {
		influxql.WalkFunc(f.Expr, func(n influxql.Node) {
			switch n := n.(type) {
			case *influxql.Call:
				if n.Name == DatePartString {
					hasDatePart = true
				} else if !isMathFunction(n) {
					// An aggregate or selector (count, max, ...) anchors the scan.
					hasAnchor = true
				}
			case *influxql.VarRef:
				// Only a stored field anchors the scan. Tags, the time column
				// (the date_part argument, typed Time/Unknown here), and untyped
				// refs do not, so match the concrete stored-field types explicitly.
				switch n.Type {
				case influxql.Float, influxql.Integer, influxql.Unsigned, influxql.String, influxql.Boolean:
					hasAnchor = true
				}
			}
		})
	}
	if hasDatePart && !hasAnchor {
		return errAtLeastOneNonTimeField
	}

	// Recurse into subquery sources. RewriteFields rewrites the whole statement
	// tree, so inner VarRef types are resolved by the time this runs in Prepare.
	// Without this, a tag-only-anchor inner query (e.g.
	// SELECT host, date_part('year', time) AS yr FROM cpu) escapes the check: it
	// plans as an aux-only iterator emitting no points, so an outer aggregate over
	// it silently returns nothing even though the equivalent top-level query is
	// rejected.
	for _, source := range stmt.Sources {
		if sub, ok := source.(*influxql.SubQuery); ok {
			if err := validateDatePartAnchor(sub.Statement); err != nil {
				return err
			}
		}
	}
	return nil
}

type DatePartValuer struct {
	Valuer influxql.MapValuer
	// Location is the timezone in which calendar fields are computed.
	// A nil Location is treated as UTC (see LocationOrUTC).
	Location *time.Location
}

// LocationOrUTC returns loc, or time.UTC when loc is nil. Shared with the TSM
// iterator so date_part extraction defaults consistently.
func LocationOrUTC(loc *time.Location) *time.Location {
	if loc == nil {
		return time.UTC
	}
	return loc
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

func (v DatePartValuer) Call(name string, args []interface{}) (interface{}, bool) {
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

	// Under GROUP BY date_part(...), the active grouped dimension value is
	// authoritative for the series; the row timestamp is only a bucket
	// representative and must not be used. Resolving from the grouped value here
	// keeps nested expressions (e.g. date_part('year', time) + 1) consistent with
	// top-level date_part fields: the active part yields its grouped value, and a
	// non-active grouped part is undefined for this series (nil).
	if v.Valuer != nil {
		if raw, ok := v.Valuer.Value(DatePartDimensionsString); ok {
			if dpk, ok := raw.(DecodedDatePartKey); ok {
				if expr == dpk.Expr {
					return dpk.Val, true
				}
				return nil, false
			}
		}
	}

	timestampRaw, ok := args[1].(int64)
	if !ok {
		return nil, false
	}

	timestamp := time.Unix(0, timestampRaw).In(LocationOrUTC(v.Location))
	return ExtractDatePartExpr(timestamp, expr)
}

// datePartCondKeyPrefix prefixes the reserved eval-map keys written by
// DatePartCondition.SetTime. The NUL byte keeps the names out of the space of
// real field and tag names, following DatePartDimensionsString.
const datePartCondKeyPrefix = "\x00date_part:"

type datePartCondPart struct {
	part DatePartExpr
	name string

	// Boxing cache: the extracted value is converted to interface{} only when
	// it changes between points, so repeated scans of the same hour/day/year
	// reuse the previous boxed value instead of allocating.
	lastVal   int64
	lastBoxed interface{}
}

// DatePartCondition evaluates date_part references in a condition without
// per-point function-call evaluation. It rewrites each date_part call to a
// reserved variable reference once at construction; SetTime then extracts the
// referenced parts from a point's timestamp and publishes them to the
// condition-evaluation map. A DatePartCondition carries per-point state and
// must not be shared between concurrently scanning iterators.
type DatePartCondition struct {
	expr  influxql.Expr
	parts []datePartCondPart
	loc   *time.Location
}

// NewDatePartCondition returns a DatePartCondition for cond, or nil when cond
// is nil or contains no date_part call. cond itself is never modified; the
// rewrite operates on a clone.
func NewDatePartCondition(cond influxql.Expr, loc *time.Location) *DatePartCondition {
	if cond == nil {
		return nil
	}
	c := &DatePartCondition{loc: loc}
	rewritten := influxql.RewriteExpr(influxql.CloneExpr(cond), func(e influxql.Expr) influxql.Expr {
		part, ok := matchDatePartCall(e)
		if !ok {
			return e
		}
		return &influxql.VarRef{Val: c.varName(part)}
	})
	if len(c.parts) == 0 {
		return nil
	}
	c.expr = rewritten
	return c
}

// varName returns the reserved variable name for part, registering it on
// first use so SetTime knows which parts to extract.
func (c *DatePartCondition) varName(part DatePartExpr) string {
	for i := range c.parts {
		if c.parts[i].part == part {
			return c.parts[i].name
		}
	}
	name := datePartCondKeyPrefix + part.String()
	c.parts = append(c.parts, datePartCondPart{part: part, name: name})
	return name
}

// Expr returns the rewritten condition. Every date_part call has been replaced
// by a reserved variable reference resolved through SetTime.
func (c *DatePartCondition) Expr() influxql.Expr { return c.expr }

// SetTime extracts each date_part referenced by the condition from ts and
// stores the values in m under the reserved names.
func (c *DatePartCondition) SetTime(ts int64, m map[string]interface{}) {
	t := time.Unix(0, ts).In(LocationOrUTC(c.loc))
	for i := range c.parts {
		p := &c.parts[i]
		v, ok := ExtractDatePartExpr(t, p.part)
		if !ok {
			m[p.name] = nil
			continue
		}
		if p.lastBoxed == nil || v != p.lastVal {
			p.lastVal = v
			p.lastBoxed = v
		}
		m[p.name] = p.lastBoxed
	}
}

// DatePartDimension is a GROUP BY date_part dimension. Its output column is
// named by Expr.String() — the canonical part name (e.g. "dow"), regardless of
// how the user spelled the literal (e.g. "DOW").
type DatePartDimension struct {
	Expr DatePartExpr
}

type DecodedDatePartKey struct {
	Expr DatePartExpr
	Val  int64
}

// extractVal extracts an int64 from the aux value at the first-level reduce.
// The TSM iterator always appends int64 values from ExtractDatePartExpr.
func extractVal(auxVal interface{}) (int64, error) {
	v, ok := auxVal.(int64)
	if !ok {
		return 0, fmt.Errorf("date_part: unexpected aux value type: %T", auxVal)
	}
	return v, nil
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

// computeDimKey builds a grouping key string that uniquely identifies a
// (tag subset, expr, val) tuple; it is used as a map key and is never decoded.
// Note the reduce path SORTS these keys to order the output series, so the
// format is observable: the leading expr.String() makes series sort by part
// name, which the GROUP BY date_part result ordering depends on. When tags are
// present the tag subset ID is length-prefixed (8-byte big-endian) so the
// encoding stays unambiguous even if the ID contains NUL bytes — which it can,
// e.g. when a series has empty tag values.
func computeDimKey(expr DatePartExpr, val int64, tags TagSubset) string {
	var buf [8]byte
	// Flip the sign bit so lexicographic byte order matches signed numeric
	// order; without this a negative value (e.g. a pre-1970 'epoch') encodes
	// with its high bit set and sorts after every non-negative value.
	binary.BigEndian.PutUint64(buf[:], uint64(val)^(1<<63))
	valStr := string(buf[:])
	if tags.HasTags {
		var lenBuf [8]byte
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(tags.ID)))
		return string(lenBuf[:]) + tags.ID + expr.String() + ":" + valStr
	}
	return expr.String() + ":" + valStr
}

// newGroupingEntry builds the paired keys for one resolved dimension value:
// DimKey for in-memory grouping and series ordering, EncodedKey for aux
// transport across reduce levels.
func newGroupingEntry(expr DatePartExpr, val int64, tags TagSubset) GroupingEntry {
	return GroupingEntry{
		DimKey:     computeDimKey(expr, val, tags),
		EncodedKey: encodeKey(expr, val),
	}
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
	if len(encodedKey) != 9 {
		return DecodedDatePartKey{}, fmt.Errorf("date_part: encoded key must be exactly 9 bytes, got %d", len(encodedKey))
	}
	expr := DatePartExpr(encodedKey[0])
	if expr < Year || expr >= Invalid {
		return DecodedDatePartKey{}, fmt.Errorf("date_part: encoded key has invalid expr byte %d", encodedKey[0])
	}
	var b [8]byte
	copy(b[:], encodedKey[1:9])
	return DecodedDatePartKey{
		Expr: expr,
		Val:  int64(binary.BigEndian.Uint64(b[:])),
	}, nil
}

func (g *DatePartGrouper) ResolveKeys(aux []interface{}, tags TagSubset) ([]GroupingEntry, error) {
	// Check for second-level reduce: aux contains DecodedDatePartKey from a prior emit.
	for _, av := range aux {
		if dpk, ok := av.(DecodedDatePartKey); ok {
			return []GroupingEntry{newGroupingEntry(dpk.Expr, dpk.Val, tags)}, nil
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

		entries = append(entries, newGroupingEntry(dim.Expr, val, tags))
	}
	return entries, nil
}

func (g *DatePartGrouper) DecodeEntry(encodedKey string) (interface{}, error) {
	return decodeKey(encodedKey)
}
