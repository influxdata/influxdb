package query_test

import (
	"testing"
	"time"

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
		{"invalid", "invalid", 0, false},
		{"empty", "", 0, false},
		{"random", "foobar", 0, false},
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
				&influxql.VarRef{Val: "time"},
			},
			expectError: false,
			expectField: "time",
			expectExpr:  query.DOW,
		},
		{
			name: "valid with time and string literal",
			args: []influxql.Expr{
				&influxql.StringLiteral{Val: "year"},
				&influxql.VarRef{Val: "time"},
			},
			expectError: false,
			expectField: "time",
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
				&influxql.VarRef{Val: "time"},
			},
			expectError: true,
			errorMsg:    "invalid number of arguments",
		},
		{
			name: "invalid - wrong number of args (3)",
			args: []influxql.Expr{
				&influxql.VarRef{Val: "time"},
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
				&influxql.VarRef{Val: "time"},
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
				&influxql.VarRef{Val: "time"},
			},
			expectError: true,
			errorMsg:    "first argument must be a string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field, expr, err := query.ValidateDatePart(tt.args)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, field)
				require.Equal(t, tt.expectField, field.Val)
				require.Equal(t, tt.expectExpr, expr)
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
			expected: int64(1), // Monday in ISO
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

	t.Run("isodow - Sunday is 7", func(t *testing.T) {
		result, ok := valuer.Call("date_part", []interface{}{"isodow", sundayTimestamp})
		require.True(t, ok)
		require.Equal(t, int64(7), result, "isdow check") // Sunday = 7 in ISO
	})
}

func TestDatePartTypeMapper_CallType(t *testing.T) {
	mapper := query.DatePartTypeMapper{}

	tests := []struct {
		name     string
		funcName string
		args     []influxql.DataType
		expected influxql.DataType
		hasError bool
	}{
		{
			name:     "date_part returns integer",
			funcName: "date_part",
			args:     []influxql.DataType{influxql.Integer, influxql.String},
			expected: influxql.Integer,
			hasError: false,
		},
		{
			name:     "date_part with time field",
			funcName: "date_part",
			args:     []influxql.DataType{influxql.Time, influxql.String},
			expected: influxql.Integer,
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := mapper.CallType(tt.funcName, tt.args)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}
