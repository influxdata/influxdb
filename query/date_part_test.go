package query_test

import (
	"encoding/binary"
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
			errorMsg:    "second argument must be",
		},
		{
			name: "invalid - unknown date part expression",
			args: []influxql.Expr{
				&influxql.VarRef{Val: "invalid_expr"},
				&influxql.VarRef{Val: models.TimeString},
			},
			expectError: true,
			errorMsg:    "first argument must be a string",
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
			expected: int64(0), // Monday in ISO
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
			expected: int64(3), // Approximate
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

	t.Run("isodow - Sunday is 6", func(t *testing.T) {
		result, ok := valuer.Call("date_part", []interface{}{"isodow", sundayTimestamp})
		require.True(t, ok)
		require.Equal(t, int64(6), result, "isdow check") // Sunday = 6 in ISO
	})
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

func TestComputeDatePartDimensions(t *testing.T) {
	makeKey := func(values ...int64) string {
		buf := make([]byte, len(values)*8)
		for i, v := range values {
			binary.BigEndian.PutUint64(buf[i*8:], uint64(v))
		}
		return string(buf)
	}

	tests := []struct {
		name        string
		dimensions  []query.DatePartDimension
		timestamp   int64
		expectedKey string
		expectError bool
	}{
		{
			name: "single dimension - year",
			dimensions: []query.DatePartDimension{
				{Name: "year", Expr: query.Year},
			},
			timestamp:   time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC).UnixNano(),
			expectedKey: makeKey(2024),
			expectError: false,
		},
		{
			name: "single dimension - month",
			dimensions: []query.DatePartDimension{
				{Name: "month", Expr: query.Month},
			},
			timestamp:   time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC).UnixNano(),
			expectedKey: makeKey(3),
			expectError: false,
		},
		{
			name: "single dimension - day",
			dimensions: []query.DatePartDimension{
				{Name: "day", Expr: query.Day},
			},
			timestamp:   time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC).UnixNano(),
			expectedKey: makeKey(15),
			expectError: false,
		},
		{
			name: "single dimension - dow (Friday)",
			dimensions: []query.DatePartDimension{
				{Name: "dow", Expr: query.DOW},
			},
			timestamp:   time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC).UnixNano(), // Friday
			expectedKey: makeKey(5),
			expectError: false,
		},
		{
			name: "single dimension - hour",
			dimensions: []query.DatePartDimension{
				{Name: "hour", Expr: query.Hour},
			},
			timestamp:   time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC).UnixNano(),
			expectedKey: makeKey(14),
			expectError: false,
		},
		{
			name: "multiple dimensions - year and month",
			dimensions: []query.DatePartDimension{
				{Name: "year", Expr: query.Year},
				{Name: "month", Expr: query.Month},
			},
			timestamp:   time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC).UnixNano(),
			expectedKey: makeKey(2024, 3),
			expectError: false,
		},
		{
			name: "multiple dimensions - year, month, day",
			dimensions: []query.DatePartDimension{
				{Name: "year", Expr: query.Year},
				{Name: "month", Expr: query.Month},
				{Name: "day", Expr: query.Day},
			},
			timestamp:   time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC).UnixNano(),
			expectedKey: makeKey(2024, 3, 15),
			expectError: false,
		},
		{
			name: "multiple dimensions - year, month, dow",
			dimensions: []query.DatePartDimension{
				{Name: "year", Expr: query.Year},
				{Name: "month", Expr: query.Month},
				{Name: "dow", Expr: query.DOW},
			},
			timestamp:   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), // Sunday
			expectedKey: makeKey(2023, 1, 0),
			expectError: false,
		},
		{
			name: "multiple dimensions - dow and hour",
			dimensions: []query.DatePartDimension{
				{Name: "dow", Expr: query.DOW},
				{Name: "hour", Expr: query.Hour},
			},
			timestamp:   time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC).UnixNano(), // Friday 14:30
			expectedKey: makeKey(5, 14),
			expectError: false,
		},
		{
			name: "quarter dimension",
			dimensions: []query.DatePartDimension{
				{Name: "quarter", Expr: query.Quarter},
			},
			timestamp:   time.Date(2024, 7, 15, 0, 0, 0, 0, time.UTC).UnixNano(), // Q3
			expectedKey: makeKey(3),
			expectError: false,
		},
		{
			name: "epoch dimension",
			dimensions: []query.DatePartDimension{
				{Name: "epoch", Expr: query.Epoch},
			},
			timestamp:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), // 1704067200
			expectedKey: makeKey(1704067200),
			expectError: false,
		},
		{
			name: "isodow dimension - Monday",
			dimensions: []query.DatePartDimension{
				{Name: "isodow", Expr: query.ISODOW},
			},
			timestamp:   time.Date(2024, 3, 18, 0, 0, 0, 0, time.UTC).UnixNano(), // Monday
			expectedKey: makeKey(0),
			expectError: false,
		},
		{
			name: "isodow dimension - Sunday",
			dimensions: []query.DatePartDimension{
				{Name: "isodow", Expr: query.ISODOW},
			},
			timestamp:   time.Date(2024, 3, 17, 0, 0, 0, 0, time.UTC).UnixNano(), // Sunday
			expectedKey: makeKey(6),
			expectError: false,
		},
		{
			name:        "empty dimensions",
			dimensions:  []query.DatePartDimension{},
			timestamp:   time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC).UnixNano(),
			expectedKey: "",
			expectError: false,
		},
		{
			name: "year boundary - Dec 31",
			dimensions: []query.DatePartDimension{
				{Name: "year", Expr: query.Year},
				{Name: "month", Expr: query.Month},
				{Name: "day", Expr: query.Day},
			},
			timestamp:   time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC).UnixNano(),
			expectedKey: makeKey(2023, 12, 31),
			expectError: false,
		},
		{
			name: "year boundary - Jan 1",
			dimensions: []query.DatePartDimension{
				{Name: "year", Expr: query.Year},
				{Name: "month", Expr: query.Month},
				{Name: "day", Expr: query.Day},
			},
			timestamp:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
			expectedKey: makeKey(2024, 1, 1),
			expectError: false,
		},
		{
			name: "leap year - Feb 29",
			dimensions: []query.DatePartDimension{
				{Name: "year", Expr: query.Year},
				{Name: "month", Expr: query.Month},
				{Name: "day", Expr: query.Day},
			},
			timestamp:   time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC).UnixNano(),
			expectedKey: makeKey(2024, 2, 29),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := query.ComputeDatePartDimensions(tt.dimensions, tt.timestamp)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedKey, result, "computed key should match expected key")
				expectedLen := len(tt.dimensions) * 8
				require.Equal(t, expectedLen, len(result), "key length should be 8 bytes per dimension")
			}
		})
	}
}

func TestComputeDatePartDimensions_KeyConsistency(t *testing.T) {
	// Test that computing the same dimensions for the same timestamp produces identical keys
	dims := []query.DatePartDimension{
		{Name: "year", Expr: query.Year},
		{Name: "month", Expr: query.Month},
		{Name: "dow", Expr: query.DOW},
	}

	timestamp := time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC).UnixNano()

	key1, err1 := query.ComputeDatePartDimensions(dims, timestamp)
	require.NoError(t, err1)

	key2, err2 := query.ComputeDatePartDimensions(dims, timestamp)
	require.NoError(t, err2)

	require.Equal(t, key1, key2, "keys should be identical for same inputs")
}

func TestComputeDatePartDimensions_KeyUniqueness(t *testing.T) {
	// Test that different timestamps produce different keys
	dims := []query.DatePartDimension{
		{Name: "year", Expr: query.Year},
		{Name: "month", Expr: query.Month},
	}

	ts1 := time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC).UnixNano()
	ts2 := time.Date(2024, 4, 15, 14, 30, 0, 0, time.UTC).UnixNano() // Different month

	key1, err1 := query.ComputeDatePartDimensions(dims, ts1)
	require.NoError(t, err1)

	key2, err2 := query.ComputeDatePartDimensions(dims, ts2)
	require.NoError(t, err2)

	require.NotEqual(t, key1, key2, "keys should be different for different months")
}

func TestComputeDatePartDimensions_KeyOrdering(t *testing.T) {
	// Test that keys can be compared for ordering
	dims := []query.DatePartDimension{
		{Name: "year", Expr: query.Year},
		{Name: "month", Expr: query.Month},
	}

	// Create keys for Jan 2024, Feb 2024, Mar 2024
	keyJan, _ := query.ComputeDatePartDimensions(dims, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	keyFeb, _ := query.ComputeDatePartDimensions(dims, time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	keyMar, _ := query.ComputeDatePartDimensions(dims, time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC).UnixNano())

	// Keys should be comparable and maintain order
	require.True(t, keyJan < keyFeb, "Jan key should be less than Feb key")
	require.True(t, keyFeb < keyMar, "Feb key should be less than Mar key")
	require.True(t, keyJan < keyMar, "Jan key should be less than Mar key")
}
