package query_test

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/require"
)

func TestParseDatePartExpr(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected query.DatePartExpr
		ok       bool
	}{
		{"year lowercase", "year", query.Year, true},
		{"year uppercase", "YEAR", query.Year, true},
		{"year mixed", "YeAr", query.Year, true},
		{"quarter", "quarter", query.Quarter, true},
		{"month", "month", query.Month, true},
		{"week", "week", query.Week, true},
		{"day", "day", query.Day, true},
		{"hour", "hour", query.Hour, true},
		{"minute", "minute", query.Minute, true},
		{"second", "second", query.Second, true},
		{"millisecond", "millisecond", query.Millisecond, true},
		{"microsecond", "microsecond", query.Microsecond, true},
		{"nanosecond", "nanosecond", query.Nanosecond, true},
		{"dow", "dow", query.DOW, true},
		{"DOW uppercase", "DOW", query.DOW, true},
		{"doy", "doy", query.DOY, true},
		{"epoch", "epoch", query.Epoch, true},
		{"isodow", "isodow", query.ISODOW, true},
		{"invalid", "invalid", query.Invalid, false},
		{"empty", "", query.Invalid, false},
		{"random", "foobar", query.Invalid, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := query.ParseDatePartExpr(tt.input)
			require.Equal(t, tt.ok, ok, "ok should match")
			require.Equal(t, tt.expected, result, "parsed value should match")
		})
	}
}

func TestValidateDatePart(t *testing.T) {
	tests := []struct {
		name        string
		args        []influxql.Expr
		expectError bool
		errorMsg    string
		expectField string
		expectExpr  query.DatePartExpr
	}{
		{
			name: "valid with time and DOW",
			args: []influxql.Expr{
				&influxql.StringLiteral{Val: "DOW"},
				&influxql.VarRef{Val: models.TimeString},
			},
			expectError: false,
			expectField: models.TimeString,
			expectExpr:  query.DOW,
		},
		{
			name: "valid with time and string literal",
			args: []influxql.Expr{
				&influxql.StringLiteral{Val: "year"},
				&influxql.VarRef{Val: models.TimeString},
			},
			expectError: false,
			expectField: models.TimeString,
			expectExpr:  query.Year,
		},
		{
			name:        "invalid - wrong number of args (0)",
			args:        []influxql.Expr{},
			expectError: true,
			errorMsg:    "invalid number of arguments",
		},
		{
			name: "invalid - wrong number of args (1)",
			args: []influxql.Expr{
				&influxql.VarRef{Val: models.TimeString},
			},
			expectError: true,
			errorMsg:    "invalid number of arguments",
		},
		{
			name: "invalid - wrong number of args (3)",
			args: []influxql.Expr{
				&influxql.VarRef{Val: models.TimeString},
				&influxql.VarRef{Val: "DOW"},
				&influxql.VarRef{Val: "extra"},
			},
			expectError: true,
			errorMsg:    "invalid number of arguments",
		},
		{
			name: "invalid - first arg not StringLiteral",
			args: []influxql.Expr{
				&influxql.IntegerLiteral{Val: 123},
				&influxql.VarRef{Val: models.TimeString},
			},
			expectError: true,
			errorMsg:    "first argument must be",
		},
		{
			name: "invalid - second arg not a VarRef",
			args: []influxql.Expr{
				&influxql.StringLiteral{Val: "dow"},
				&influxql.IntegerLiteral{Val: 123},
			},
			expectError: true,
			errorMsg:    "second argument must be a variable reference",
		},
		{
			name: "invalid - second arg is a non-time VarRef",
			args: []influxql.Expr{
				&influxql.StringLiteral{Val: "dow"},
				&influxql.VarRef{Val: "value"},
			},
			expectError: true,
			errorMsg:    "second argument must be time VarRef",
		},
		{
			name: "invalid - first arg is a non-string literal",
			args: []influxql.Expr{
				&influxql.VarRef{Val: "invalid_expr"},
				&influxql.VarRef{Val: models.TimeString},
			},
			expectError: true,
			errorMsg:    "first argument must be a string",
		},
		{
			// A valid string literal whose value is not a known part exercises the
			// ParseDatePartExpr-failure branch (distinct from the non-string branch
			// above), including the construction of the valid-parts list.
			name: "invalid - unknown date part name",
			args: []influxql.Expr{
				&influxql.StringLiteral{Val: "bogus"},
				&influxql.VarRef{Val: models.TimeString},
			},
			expectError: true,
			errorMsg:    "first argument must be one of the following",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := query.ValidateDatePart(tt.args)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDatePartValuer_Call(t *testing.T) {
	valuer := query.DatePartValuer{}

	// Test timestamp: 2024-01-15 14:30:45.123456789 UTC (Monday)
	testTime := time.Date(2024, 1, 15, 14, 30, 45, 123456789, time.UTC)
	testTimestamp := testTime.UnixNano()

	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		expected interface{}
		ok       bool
	}{
		{
			name:     "dow - Monday",
			funcName: "date_part",
			args:     []interface{}{"dow", testTimestamp},
			expected: int64(1), // Monday
			ok:       true,
		},
		{
			name:     "doy - 15th day of year",
			funcName: "date_part",
			args:     []interface{}{"doy", testTimestamp},
			expected: int64(15),
			ok:       true,
		},
		{
			name:     "year",
			funcName: "date_part",
			args:     []interface{}{"year", testTimestamp},
			expected: int64(2024),
			ok:       true,
		},
		{
			name:     "month - January",
			funcName: "date_part",
			args:     []interface{}{"month", testTimestamp},
			expected: int64(1),
			ok:       true,
		},
		{
			name:     "day",
			funcName: "date_part",
			args:     []interface{}{"day", testTimestamp},
			expected: int64(15),
			ok:       true,
		},
		{
			name:     "hour",
			funcName: "date_part",
			args:     []interface{}{"hour", testTimestamp},
			expected: int64(14),
			ok:       true,
		},
		{
			name:     "minute",
			funcName: "date_part",
			args:     []interface{}{"minute", testTimestamp},
			expected: int64(30),
			ok:       true,
		},
		{
			name:     "second",
			funcName: "date_part",
			args:     []interface{}{"second", testTimestamp},
			expected: int64(45),
			ok:       true,
		},
		{
			name:     "millisecond",
			funcName: "date_part",
			args:     []interface{}{"millisecond", testTimestamp},
			expected: int64(45123), // 45s*1000 + 123456789ns/1e6
			ok:       true,
		},
		{
			name:     "microsecond",
			funcName: "date_part",
			args:     []interface{}{"microsecond", testTimestamp},
			expected: int64(45123456), // 45s*1e6 + 123456789ns/1e3
			ok:       true,
		},
		{
			name:     "nanosecond",
			funcName: "date_part",
			args:     []interface{}{"nanosecond", testTimestamp},
			expected: int64(45123456789), // 45s*1e9 + 123456789ns
			ok:       true,
		},
		{
			name:     "epoch",
			funcName: "date_part",
			args:     []interface{}{"epoch", testTimestamp},
			expected: testTime.Unix(),
			ok:       true,
		},
		{
			name:     "isodow - Monday",
			funcName: "date_part",
			args:     []interface{}{"isodow", testTimestamp},
			expected: int64(1), // Monday = 1 in ISO 8601
			ok:       true,
		},
		{
			name:     "quarter - Q1",
			funcName: "date_part",
			args:     []interface{}{"quarter", testTimestamp},
			expected: int64(1),
			ok:       true,
		},
		{
			name:     "week",
			funcName: "date_part",
			args:     []interface{}{"week", testTimestamp},
			expected: int64(3), // 2024-01-15 is in ISO week 3
			ok:       true,
		},
		{
			name:     "wrong function name",
			funcName: "other_function",
			args:     []interface{}{"dow", testTimestamp},
			expected: nil,
			ok:       false,
		},
		{
			name:     "wrong number of args - 0",
			funcName: "date_part",
			args:     []interface{}{},
			expected: nil,
			ok:       false, // Returns false for wrong arg count
		},
		{
			name:     "wrong number of args - 1",
			funcName: "date_part",
			args:     []interface{}{testTimestamp},
			expected: nil,
			ok:       false,
		},
		{
			name:     "wrong number of args - 3",
			funcName: "date_part",
			args:     []interface{}{"dow", testTimestamp, "extra"},
			expected: nil,
			ok:       false,
		},
		{
			name:     "invalid timestamp type",
			funcName: "date_part",
			args:     []interface{}{"dow", "not a timestamp"},
			expected: nil,
			ok:       false,
		},
		{
			name:     "invalid expression type",
			funcName: "date_part",
			args:     []interface{}{123, testTimestamp},
			expected: nil,
			ok:       false,
		},
		{
			name:     "unknown date part expression",
			funcName: "date_part",
			args:     []interface{}{"invalid", testTimestamp},
			expected: nil,
			ok:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := valuer.Call(tt.funcName, tt.args)
			require.Equal(t, tt.ok, ok, "ok should match")
			if tt.ok && tt.expected != nil {
				require.Equal(t, tt.expected, result, "result should match")
			}
		})
	}
}

func TestDatePartValuer_Call_Sunday(t *testing.T) {
	valuer := query.DatePartValuer{}

	// Sunday: 2024-01-14 10:00:00 UTC
	sunday := time.Date(2024, 1, 14, 10, 0, 0, 0, time.UTC)
	sundayTimestamp := sunday.UnixNano()

	t.Run("dow - Sunday is 0", func(t *testing.T) {
		result, ok := valuer.Call("date_part", []interface{}{"dow", sundayTimestamp})
		require.True(t, ok)
		require.Equal(t, int64(0), result, "dow check") // Sunday = 0
	})

	t.Run("isodow - Sunday is 7", func(t *testing.T) {
		result, ok := valuer.Call("date_part", []interface{}{"isodow", sundayTimestamp})
		require.True(t, ok)
		require.Equal(t, int64(7), result, "isodow check") // Sunday = 7 in ISO 8601
	})
}

func TestDatePartValuer_Call_ISOWeekBoundary(t *testing.T) {
	valuer := query.DatePartValuer{}

	// 2023-01-01 is a Sunday that falls in ISO week 52 of 2022.
	// This is the classic ISO-week footgun: date_part('week') follows
	// ISOWeek() while date_part('year') follows the calendar year, so they
	// disagree across the year boundary.
	jan1 := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)

	week, ok := valuer.Call("date_part", []interface{}{"week", jan1.UnixNano()})
	require.True(t, ok)
	require.Equal(t, int64(52), week, "2023-01-01 is in ISO week 52 of the prior year")

	year, ok := valuer.Call("date_part", []interface{}{"year", jan1.UnixNano()})
	require.True(t, ok)
	require.Equal(t, int64(2023), year, "year follows the calendar year, not the ISO week-year")

	// 2021-01-01 is a Friday that falls in ISO week 53 of 2020.
	week53 := time.Date(2021, 1, 1, 12, 0, 0, 0, time.UTC)
	week, ok = valuer.Call("date_part", []interface{}{"week", week53.UnixNano()})
	require.True(t, ok)
	require.Equal(t, int64(53), week, "2021-01-01 is in ISO week 53 of the prior year")
}

func TestDatePartValuer_Call_PreEpoch(t *testing.T) {
	valuer := query.DatePartValuer{}

	// One hour before the Unix epoch: 1969-12-31 23:00:00 UTC (a Wednesday).
	preEpoch := time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC)
	ts := preEpoch.UnixNano()

	tests := []struct {
		part     string
		expected int64
	}{
		{"epoch", -3600}, // one hour before epoch
		{"year", 1969},
		{"month", 12},
		{"day", 31},
		{"hour", 23},
		{"dow", 3},    // Wednesday
		{"isodow", 3}, // Wednesday = 3 in ISO 8601 (Monday=1)
	}

	for _, tt := range tests {
		t.Run(tt.part, func(t *testing.T) {
			result, ok := valuer.Call("date_part", []interface{}{tt.part, ts})
			require.True(t, ok)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDatePartValuer_Value(t *testing.T) {
	now := time.Now().UnixNano()
	mapValuer := influxql.MapValuer{}
	mapValuer[models.TimeString] = now

	valuer := query.DatePartValuer{
		Valuer: mapValuer,
	}

	// Valuer should return nil when passed a string that isn't DatePartTimeString
	val, ok := valuer.Value("foo")
	require.False(t, ok)
	require.Nil(t, val)

	val, ok = valuer.Value(query.DatePartTimeString)
	require.True(t, ok)
	require.Equal(t, now, val)
}

func TestDatePartValuer_Call_GroupedDimension(t *testing.T) {
	// Under GROUP BY date_part the active grouped value is authoritative for the
	// series and is published via the DatePartDimensionsString key. date_part for
	// the active part must return that grouped value; date_part for any other
	// (non-active) part is undefined for the series and must return (nil, false)
	// rather than recomputing from the bucket-representative timestamp.
	m := influxql.MapValuer{}
	m[query.DatePartDimensionsString] = query.DecodedDatePartKey{Expr: query.Year, Val: 2024}
	valuer := query.DatePartValuer{Valuer: m}

	// args[1] is irrelevant on the grouped path; it returns before timestamp use.
	got, ok := valuer.Call("date_part", []interface{}{"year", int64(0)})
	require.True(t, ok, "active dimension should resolve")
	require.Equal(t, int64(2024), got, "active dimension returns the grouped value")

	got, ok = valuer.Call("date_part", []interface{}{"month", int64(0)})
	require.False(t, ok, "non-active dimension is undefined for the series")
	require.Nil(t, got)
}

func TestDatePartGrouper_ResolveKeys_FirstLevel(t *testing.T) {
	g := query.NewDatePartGrouper([]query.DatePartDimension{
		{Name: "month", Expr: query.Month},
	})

	aux := []interface{}{int64(3)}
	entries, err := g.ResolveKeys(aux, "", false)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.NotEmpty(t, entries[0].DimKey)
	require.NotEmpty(t, entries[0].EncodedKey)
}

func TestDatePartGrouper_ResolveKeys_FirstLevel_WithTags(t *testing.T) {
	g := query.NewDatePartGrouper([]query.DatePartDimension{
		{Name: "month", Expr: query.Month},
	})

	aux := []interface{}{int64(3)}
	entries, err := g.ResolveKeys(aux, "host=server01", true)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.NotEmpty(t, entries[0].DimKey)
}

func TestDatePartGrouper_DimKey_NoCollisionWithNulBytesInTagID(t *testing.T) {
	// Tag IDs can contain NUL bytes (e.g. empty tag values), so the grouping key
	// must not collide for distinct tag IDs. The length-prefixed encoding keeps
	// them unambiguous even when a tag ID ends in / contains the bytes that the
	// old separator scheme used.
	g := query.NewDatePartGrouper([]query.DatePartDimension{
		{Name: "month", Expr: query.Month},
	})

	a, err := g.ResolveKeys([]interface{}{int64(3)}, "a\x00\x00b", true)
	require.NoError(t, err)
	b, err := g.ResolveKeys([]interface{}{int64(3)}, "a\x00\x00b\x00\x00c", true)
	require.NoError(t, err)
	require.NotEqual(t, a[0].DimKey, b[0].DimKey, "distinct tag IDs must yield distinct grouping keys")
}

func TestDatePartGrouper_ResolveKeys_SecondLevel(t *testing.T) {
	g := query.NewDatePartGrouper([]query.DatePartDimension{
		{Name: "month", Expr: query.Month},
	})

	aux := []interface{}{query.DecodedDatePartKey{Expr: query.Month, Val: 3}}
	entries, err := g.ResolveKeys(aux, "", false)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestDatePartGrouper_DecodeEntry(t *testing.T) {
	g := query.NewDatePartGrouper([]query.DatePartDimension{
		{Name: "month", Expr: query.Month},
	})

	aux := []interface{}{int64(7)}
	entries, err := g.ResolveKeys(aux, "", false)
	require.NoError(t, err)

	decoded, err := g.DecodeEntry(entries[0].EncodedKey)
	require.NoError(t, err)
	dpk, ok := decoded.(query.DecodedDatePartKey)
	require.True(t, ok, "expected DecodedDatePartKey, got %T", decoded)
	require.Equal(t, int64(7), dpk.Val)
}

func TestDatePartGrouper_ResolveKeys_AuxShorterThanDims(t *testing.T) {
	// Two dimensions but only one raw value and no DecodedDatePartKey present:
	// ResolveKeys cannot map values to dims, so it returns (nil, nil).
	g := query.NewDatePartGrouper([]query.DatePartDimension{
		{Name: "year", Expr: query.Year},
		{Name: "month", Expr: query.Month},
	})

	entries, err := g.ResolveKeys([]interface{}{int64(3)}, "", false)
	require.NoError(t, err)
	require.Nil(t, entries)
}

func TestDatePartGrouper_ResolveKeys_UnexpectedAuxType(t *testing.T) {
	// A first-level aux value that is neither int64 nor DecodedDatePartKey
	// must surface an error rather than silently mis-grouping.
	g := query.NewDatePartGrouper([]query.DatePartDimension{
		{Name: "month", Expr: query.Month},
	})

	entries, err := g.ResolveKeys([]interface{}{"not an int"}, "", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected aux value type")
	require.Nil(t, entries)
}

func TestDatePartGrouper_DecodeEntry_InvalidLength(t *testing.T) {
	// The encoding is exactly 9 bytes (1 byte expr + 8 byte value). Both shorter
	// and longer keys must be rejected rather than read out of bounds or silently
	// truncated.
	g := query.NewDatePartGrouper([]query.DatePartDimension{
		{Name: "month", Expr: query.Month},
	})

	for _, key := range []string{"short", "this key is far too long"} {
		_, err := g.DecodeEntry(key)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be exactly 9 bytes")
	}
}

func TestDatePartGrouper_DecodeEntry_InvalidExprByte(t *testing.T) {
	// A 9-byte key whose first byte is not a valid DatePartExpr must be rejected
	// rather than decoded into an out-of-range expr (whose String() is empty and
	// would silently misroute the output column).
	g := query.NewDatePartGrouper([]query.DatePartDimension{
		{Name: "month", Expr: query.Month},
	})

	key := string([]byte{200, 0, 0, 0, 0, 0, 0, 0, 0})
	_, err := g.DecodeEntry(key)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid expr byte")
}

func TestDatePartGrouper_RoundTrip_MultiDimension(t *testing.T) {
	g := query.NewDatePartGrouper([]query.DatePartDimension{
		{Name: "year", Expr: query.Year},
		{Name: "month", Expr: query.Month},
	})

	aux := []interface{}{int64(2026), int64(3)}
	entries, err := g.ResolveKeys(aux, "", false)
	require.NoError(t, err)
	require.Len(t, entries, 2)

	for _, e := range entries {
		_, err := g.DecodeEntry(e.EncodedKey)
		require.NoError(t, err)
	}
}

func TestDatePartValuer_Call_Timezone(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// 2023-07-01T03:30:00Z is EDT (UTC-4) → local 2023-06-30 23:30.
	ts := time.Date(2023, 7, 1, 3, 30, 0, 0, time.UTC).UnixNano()

	utc := query.DatePartValuer{} // nil Location → UTC
	local := query.DatePartValuer{Location: ny}

	hUTC, ok := utc.Call("date_part", []interface{}{"hour", ts})
	require.True(t, ok)
	require.Equal(t, int64(3), hUTC, "UTC hour")

	hNY, ok := local.Call("date_part", []interface{}{"hour", ts})
	require.True(t, ok)
	require.Equal(t, int64(23), hNY, "New York local hour (previous day)")

	dNY, ok := local.Call("date_part", []interface{}{"day", ts})
	require.True(t, ok)
	require.Equal(t, int64(30), dNY, "New York local day rolls back to June 30")

	// epoch is an absolute instant — identical regardless of zone.
	eUTC, _ := utc.Call("date_part", []interface{}{"epoch", ts})
	eNY, _ := local.Call("date_part", []interface{}{"epoch", ts})
	require.Equal(t, eUTC, eNY, "epoch must be zone-independent")
	require.Equal(t, time.Date(2023, 7, 1, 3, 30, 0, 0, time.UTC).Unix(), eNY)
}

func TestDatePartValuer_Call_DST(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	v := query.DatePartValuer{Location: ny}

	// Spring forward: 2023-03-12 02:00 EST → 03:00 EDT (transition at 07:00Z).
	// 07:30Z is EDT (UTC-4) → 03:30 local. (Naive EST would give hour 2.)
	spring := time.Date(2023, 3, 12, 7, 30, 0, 0, time.UTC).UnixNano()
	h, ok := v.Call("date_part", []interface{}{"hour", spring})
	require.True(t, ok)
	require.Equal(t, int64(3), h, "spring-forward: DST offset applied")

	// Fall back: 2023-11-05 02:00 EDT → 01:00 EST (transition at 06:00Z).
	// 07:30Z is EST (UTC-5) → 02:30 local. (Naive EDT would give hour 3.)
	fall := time.Date(2023, 11, 5, 7, 30, 0, 0, time.UTC).UnixNano()
	h, ok = v.Call("date_part", []interface{}{"hour", fall})
	require.True(t, ok)
	require.Equal(t, int64(2), h, "fall-back: standard time resumed")
}

func TestLocationOrUTC(t *testing.T) {
	require.Equal(t, time.UTC, query.LocationOrUTC(nil))
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	require.Equal(t, ny, query.LocationOrUTC(ny))
}
