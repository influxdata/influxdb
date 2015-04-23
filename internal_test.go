package influxdb

// This file is run within the "influxdb" package and allows for internal unit tests.

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

// Ensure a measurement can return a set of unique tag values specified by an expression.
func TestMeasurement_uniqueTagValues(t *testing.T) {
	// Create a measurement to run against.
	m := NewMeasurement("cpu")
	m.createFieldIfNotExists("value", influxql.Float)

	for i, tt := range []struct {
		expr   string
		values map[string][]string
	}{
		{expr: `1`, values: map[string][]string{}},
		{expr: `foo = 'bar'`, values: map[string][]string{"foo": {"bar"}}},
		{expr: `(region = 'us-west' AND value > 10) OR ('us-east' = region AND value > 20) OR (host = 'serverA' AND value > 30)`, values: map[string][]string{"region": {"us-east", "us-west"}, "host": {"serverA"}}},
	} {
		// Extract unique tag values from the expression.
		values := m.uniqueTagValues(MustParseExpr(tt.expr))
		if !reflect.DeepEqual(tt.values, values) {
			t.Errorf("%d. %s: mismatch: exp=%+v, got=%+v", i, tt.expr, tt.values, values)
		}
	}
}

// Ensure a measurement can expand an expression for all possible tag values used.
func TestMeasurement_expandExpr(t *testing.T) {
	m := NewMeasurement("cpu")
	m.createFieldIfNotExists("value", influxql.Float)

	type tagSetExprString struct {
		tagExpr []tagExpr
		expr    string
	}

	for i, tt := range []struct {
		expr  string
		exprs []tagSetExprString
	}{
		// Single tag key, single value.
		{
			expr: `region = 'us-east' AND value > 10`,
			exprs: []tagSetExprString{
				{tagExpr: []tagExpr{{"region", []string{"us-east"}, influxql.EQ}}, expr: `value > 10.000`},
			},
		},

		// Single tag key, multiple values.
		{
			expr: `(region = 'us-east' AND value > 10) OR (region = 'us-west' AND value > 20)`,
			exprs: []tagSetExprString{
				{tagExpr: []tagExpr{{"region", []string{"us-east"}, influxql.EQ}}, expr: `value > 10.000`},
				{tagExpr: []tagExpr{{"region", []string{"us-west"}, influxql.EQ}}, expr: `value > 20.000`},
			},
		},

		// Multiple tag keys, multiple values.
		{
			expr: `(region = 'us-east' AND value > 10) OR ((host = 'serverA' OR host = 'serverB') AND value > 20)`,
			exprs: []tagSetExprString{
				{tagExpr: []tagExpr{{key: "host", values: []string{"serverA"}, op: influxql.EQ}, {key: "region", values: []string{"us-east"}, op: influxql.EQ}}, expr: "(value > 10.000) OR (value > 20.000)"},
				{tagExpr: []tagExpr{{key: "host", values: []string{"serverA"}, op: influxql.EQ}, {key: "region", values: []string{"us-east"}, op: influxql.NEQ}}, expr: "value > 20.000"},
				{tagExpr: []tagExpr{{key: "host", values: []string{"serverB"}, op: influxql.EQ}, {key: "region", values: []string{"us-east"}, op: influxql.EQ}}, expr: "(value > 10.000) OR (value > 20.000)"},
				{tagExpr: []tagExpr{{key: "host", values: []string{"serverB"}, op: influxql.EQ}, {key: "region", values: []string{"us-east"}, op: influxql.NEQ}}, expr: "value > 20.000"},
				{tagExpr: []tagExpr{{key: "host", values: []string{"serverA", "serverB"}, op: influxql.NEQ}, {key: "region", values: []string{"us-east"}, op: influxql.EQ}}, expr: "value > 10.000"},
			},
		},
	} {
		// Expand out an expression to all possible expressions based on tag values.
		tagExprs := m.expandExpr(MustParseExpr(tt.expr))

		// Convert to intermediate representation.
		var a []tagSetExprString
		for _, tagExpr := range tagExprs {
			a = append(a, tagSetExprString{tagExpr: tagExpr.values, expr: tagExpr.expr.String()})
		}

		// Validate that the expanded expressions are what we expect.
		if !reflect.DeepEqual(tt.exprs, a) {
			t.Errorf("%d. %s: mismatch:\n\nexp=%#v\n\ngot=%#v\n\ns", i, tt.expr, tt.exprs, a)
		}
	}
}

// Ensure the createMeasurementsIfNotExistsCommand operates correctly.
func TestCreateMeasurementsCommand(t *testing.T) {
	var err error
	var n int
	c := newCreateMeasurementsIfNotExistsCommand("foo")
	if c == nil {
		t.Fatal("createMeasurementsIfNotExistsCommand is nil")
	}

	// Add Measurement twice, to make sure nothing blows up.
	c.addMeasurementIfNotExists("bar")
	c.addMeasurementIfNotExists("bar")
	n = len(c.Measurements)
	if n != 1 {
		t.Fatalf("wrong number of measurements, expected 1, got %d", n)
	}

	// Add Series, no tags.
	c.addSeriesIfNotExists("bar", nil)

	// Add Series, some tags.
	tags := map[string]string{"host": "server01"}
	c.addSeriesIfNotExists("bar", tags)

	// Add Series, same tags again.
	c.addSeriesIfNotExists("bar", tags)

	for _, m := range c.Measurements {
		if m.Name == "bar" {
			if len(m.Tags) != 2 {
				t.Fatalf("measurement has wrong number of tags, expected 2, got %d", n)
			}
		}
	}

	// Add a field.
	err = c.addFieldIfNotExists("bar", "value", influxql.Integer)
	if err != nil {
		t.Fatal("error adding field \"value\"")
	}

	// Add same field again.
	err = c.addFieldIfNotExists("bar", "value", influxql.Integer)
	if err != nil {
		t.Fatal("error re-adding field \"value\"")
	}

	// Add another field.
	err = c.addFieldIfNotExists("bar", "value2", influxql.String)
	if err != nil {
		t.Fatal("error re-adding field \"value2\"")
	}

	for _, m := range c.Measurements {
		if m.Name == "bar" {
			if len(m.Fields) != 2 {
				t.Fatalf("measurement has wrong number of fields, expected 2, got %d", n)
			}
		}
	}
}

// Ensure the createMeasurementsIfNotExistsCommand returns expected errors.
func TestCreateMeasurementsCommand_Errors(t *testing.T) {
	var err error
	c := newCreateMeasurementsIfNotExistsCommand("foo")
	if c == nil {
		t.Fatal("createMeasurementsIfNotExistsCommand is nil")
	}

	// Ensure fields can be added to non-existent Measurements. The
	// Measurements should be created automatically.
	c.addSeriesIfNotExists("bar", nil)

	err = c.addFieldIfNotExists("bar", "value", influxql.Float)
	if err != nil {
		t.Fatalf("unexpected error got %s", err.Error())
	}

	// Add Measurement. Adding it now should be OK.
	c.addMeasurementIfNotExists("bar")

	// Test type conflicts
	err = c.addFieldIfNotExists("bar", "value", influxql.Float)
	if err != nil {
		t.Fatal("error adding field \"value\"")
	}
	err = c.addFieldIfNotExists("bar", "value", influxql.String)
	if err != ErrFieldTypeConflict {
		t.Fatalf("expected ErrFieldTypeConflict got %s", err.Error())
	}
}

// Test comparing seriesIDs for equality.
func Test_seriesIDs_equals(t *testing.T) {
	ids1 := seriesIDs{1, 2, 3}
	ids2 := seriesIDs{1, 2, 3}
	ids3 := seriesIDs{4, 5, 6}

	if !ids1.equals(ids2) {
		t.Fatal("expected ids1 == ids2")
	} else if ids1.equals(ids3) {
		t.Fatal("expected ids1 != ids3")
	}
}

// Test intersecting sets of seriesIDs.
func Test_seriesIDs_intersect(t *testing.T) {
	// Test swaping l & r, all branches of if-else, and exit loop when 'j < len(r)'
	ids1 := seriesIDs{1, 3, 4, 5, 6}
	ids2 := seriesIDs{1, 2, 3, 7}
	exp := seriesIDs{1, 3}
	got := ids1.intersect(ids2)

	if !exp.equals(got) {
		t.Fatalf("exp=%v, got=%v", exp, got)
	}

	// Test exit for loop when 'i < len(l)'
	ids1 = seriesIDs{1}
	ids2 = seriesIDs{1, 2}
	exp = seriesIDs{1}
	got = ids1.intersect(ids2)

	if !exp.equals(got) {
		t.Fatalf("exp=%v, got=%v", exp, got)
	}
}

// Test union sets of seriesIDs.
func Test_seriesIDs_union(t *testing.T) {
	// Test all branches of if-else, exit loop because of 'j < len(r)', and append remainder from left.
	ids1 := seriesIDs{1, 2, 3, 7}
	ids2 := seriesIDs{1, 3, 4, 5, 6}
	exp := seriesIDs{1, 2, 3, 4, 5, 6, 7}
	got := ids1.union(ids2)

	if !exp.equals(got) {
		t.Fatalf("exp=%v, got=%v", exp, got)
	}

	// Test exit because of 'i < len(l)' and append remainder from right.
	ids1 = seriesIDs{1}
	ids2 = seriesIDs{1, 2}
	exp = seriesIDs{1, 2}
	got = ids1.union(ids2)

	if !exp.equals(got) {
		t.Fatalf("exp=%v, got=%v", exp, got)
	}
}

// Test removing one set of seriesIDs from another.
func Test_seriesIDs_reject(t *testing.T) {
	// Test all branches of if-else, exit loop because of 'j < len(r)', and append remainder from left.
	ids1 := seriesIDs{1, 2, 3, 7}
	ids2 := seriesIDs{1, 3, 4, 5, 6}
	exp := seriesIDs{2, 7}
	got := ids1.reject(ids2)

	if !exp.equals(got) {
		t.Fatalf("exp=%v, got=%v", exp, got)
	}

	// Test exit because of 'i < len(l)'.
	ids1 = seriesIDs{1}
	ids2 = seriesIDs{1, 2}
	exp = seriesIDs{}
	got = ids1.reject(ids2)

	if !exp.equals(got) {
		t.Fatalf("exp=%v, got=%v", exp, got)
	}
}

// Test shard group selection.
func TestShardGroup_Contains(t *testing.T) {
	// Make a shard group 1 hour in duration
	tm, _ := time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
	g := newShardGroup(tm, time.Hour)

	if !g.Contains(g.StartTime.Add(-time.Minute), g.EndTime) {
		t.Fatal("shard group not selected when min before start time")
	}

	if !g.Contains(g.StartTime, g.EndTime.Add(time.Minute)) {
		t.Fatal("shard group not selected when max after after end time")
	}

	if !g.Contains(g.StartTime.Add(-time.Minute), g.EndTime.Add(time.Minute)) {
		t.Fatal("shard group not selected when min before start time and when max after end time")
	}

	if !g.Contains(g.StartTime.Add(time.Minute), g.EndTime.Add(-time.Minute)) {
		t.Fatal("shard group not selected when min after start time and when max before end time")
	}

	if !g.Contains(g.StartTime, g.EndTime) {
		t.Fatal("shard group not selected when min at start time and when max at end time")
	}

	if !g.Contains(g.StartTime, g.StartTime) {
		t.Fatal("shard group not selected when min and max set to start time")
	}

	if !g.Contains(g.EndTime, g.EndTime) {
		t.Fatal("shard group not selected when min and max set to end time")
	}

	if g.Contains(g.StartTime.Add(-10*time.Hour), g.EndTime.Add(-9*time.Hour)) {
		t.Fatal("shard group selected when both min and max before shard times")
	}

	if g.Contains(g.StartTime.Add(24*time.Hour), g.EndTime.Add(25*time.Hour)) {
		t.Fatal("shard group selected when both min and max after shard times")
	}
}

// Ensure tags can be marshaled into a byte slice.
func TestMarshalTags(t *testing.T) {
	for i, tt := range []struct {
		tags   map[string]string
		result []byte
	}{
		{
			tags:   nil,
			result: nil,
		},
		{
			tags:   map[string]string{"foo": "bar"},
			result: []byte(`foo|bar`),
		},
		{
			tags:   map[string]string{"foo": "bar", "baz": "battttt"},
			result: []byte(`baz|foo|battttt|bar`),
		},
	} {
		result := marshalTags(tt.tags)
		if !bytes.Equal(result, tt.result) {
			t.Fatalf("%d. unexpected result: exp=%s, got=%s", i, tt.result, result)
		}
	}
}

func BenchmarkMarshalTags_KeyN1(b *testing.B)  { benchmarkMarshalTags(b, 1) }
func BenchmarkMarshalTags_KeyN3(b *testing.B)  { benchmarkMarshalTags(b, 3) }
func BenchmarkMarshalTags_KeyN5(b *testing.B)  { benchmarkMarshalTags(b, 5) }
func BenchmarkMarshalTags_KeyN10(b *testing.B) { benchmarkMarshalTags(b, 10) }

func benchmarkMarshalTags(b *testing.B, keyN int) {
	const keySize, valueSize = 8, 15

	// Generate tag map.
	tags := make(map[string]string)
	for i := 0; i < keyN; i++ {
		tags[fmt.Sprintf("%0*d", keySize, i)] = fmt.Sprintf("%0*d", valueSize, i)
	}

	// Unmarshal map into byte slice.
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		marshalTags(tags)
	}
}

// MustParseExpr parses an expression string and returns its AST representation.
func MustParseExpr(s string) influxql.Expr {
	expr, err := influxql.ParseExpr(s)
	if err != nil {
		panic(err.Error())
	}
	return expr
}

func strref(s string) *string {
	return &s
}
