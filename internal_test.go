package influxdb

// This file is run within the "influxdb" package and allows for internal unit tests.

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/influxdb/influxdb/influxql"
)

// Ensure that we can get the series object by the series ID.
func TestDatabase_SeriesBySeriesID(t *testing.T) {
	db := newDatabase()

	// now test that we can add another
	s := &Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}

	db.addSeriesToIndex("foo", s)

	if ss := db.series[2]; string(mustMarshalJSON(s)) != string(mustMarshalJSON(ss)) {
		t.Fatalf("series not equal:\n%v\n%v", s, ss)
	}
}

// Ensure that we can get the measurement and series objects out based on measurement and tags.
func TestDatabase_MeasurementAndSeries(t *testing.T) {
	db := newDatabase()
	m := &Measurement{
		Name: "cpu_load",
	}
	s := &Series{
		ID:   uint32(1),
		Tags: map[string]string{"host": "servera.influx.com", "region": "uswest"},
	}

	// add it and see if we can look it up by name and tags
	db.addSeriesToIndex(m.Name, s)
	mm, ss := db.MeasurementAndSeries(m.Name, s.Tags)
	if string(mustMarshalJSON(m)) != string(mustMarshalJSON(mm)) {
		t.Fatalf("mesurement not equal:\n%v\n%v", m, mm)
	} else if string(mustMarshalJSON(s)) != string(mustMarshalJSON(ss)) {
		t.Fatalf("series not equal:\n%v\n%v", s, ss)
	}

	// now test that we can add another
	s = &Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}

	db.addSeriesToIndex(m.Name, s)
	mm, ss = db.MeasurementAndSeries(m.Name, s.Tags)
	if string(mustMarshalJSON(m)) != string(mustMarshalJSON(mm)) {
		t.Fatalf("mesurement not equal:\n%v\n%v", m, mm)
	} else if string(mustMarshalJSON(s)) != string(mustMarshalJSON(ss)) {
		t.Fatalf("series not equal:\n%v\n%v", s, ss)
	}
}

// Ensure that we can get the series IDs for measurements without any filters.
func TestDatabase_SeriesIDs(t *testing.T) {
	db := newDatabase()
	s := &Series{
		ID:   uint32(1),
		Tags: map[string]string{"host": "servera.influx.com", "region": "uswest"}}

	// add it and see if we can look it up
	added := db.addSeriesToIndex("cpu_load", s)
	if !added {
		t.Fatal("couldn't add series")
	}

	// test that we can't add it again
	added = db.addSeriesToIndex("cpu_load", s)
	if added {
		t.Fatal("shoulnd't be able to add duplicate series")
	}

	// now test that we can add another
	s = &Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}
	added = db.addSeriesToIndex("cpu_load", s)
	if !added {
		t.Fatalf("couldn't add series")
	}

	l := db.seriesIDs([]string{"cpu_load"}, nil)
	r := []uint32{1, 2}
	if !l.equals(r) {
		t.Fatalf("series IDs not the same:\n%d\n%d", l, r)
	}

	// now add another in a different measurement
	s = &Series{
		ID:   uint32(3),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}
	added = db.addSeriesToIndex("network_in", s)
	if !added {
		t.Fatalf("couldn't add series")
	}

	l = db.seriesIDs([]string{"cpu_load"}, nil)
	r = []uint32{1, 2, 3}
	if !l.equals(r) {
		t.Fatalf("series IDs not the same:\n%d\n%d", l, r)
	}
}

func TestDatabase_SeriesIDsWhereTagFilter(t *testing.T) {
	// TODO corylanou: this test is intermittently failing.  Fix and re-enable
	// trace can be found here for failing test: https://gist.github.com/corylanou/afaf7d5e8508a3e559ea
	t.Skip()
	db := databaseWithFixtureData()

	var tests = []struct {
		names   []string
		filters []*TagFilter
		result  []uint32
	}{
		// match against no tags
		{
			names:  []string{"cpu_load", "redis"},
			result: []uint32{uint32(1), uint32(2), uint32(3), uint32(4), uint32(5), uint32(6), uint32(7), uint32(8)},
		},

		// match against all tags
		{
			names: []string{"cpu_load"},
			filters: []*TagFilter{
				&TagFilter{Key: "host", Value: "servera.influx.com"},
				&TagFilter{Key: "region", Value: "uswest"},
			},
			result: []uint32{uint32(1)},
		},

		// match against one tag
		{
			names: []string{"cpu_load"},
			filters: []*TagFilter{
				&TagFilter{Key: "region", Value: "uswest"},
			},
			result: []uint32{uint32(1), uint32(2)},
		},

		// match against one tag, single result
		{
			names: []string{"cpu_load"},
			filters: []*TagFilter{
				&TagFilter{Key: "host", Value: "servera.influx.com"},
			},
			result: []uint32{uint32(1)},
		},

		// query against tag key that doesn't exist returns empty
		{
			names: []string{"cpu_load"},
			filters: []*TagFilter{
				&TagFilter{Key: "foo", Value: "bar"},
			},
			result: []uint32{},
		},

		// query against tag value that doesn't exist returns empty
		{
			names: []string{"cpu_load"},
			filters: []*TagFilter{
				&TagFilter{Key: "host", Value: "foo"},
			},
			result: []uint32{},
		},

		// query against a tag NOT value
		{
			names: []string{"key_count"},
			filters: []*TagFilter{
				&TagFilter{Key: "region", Value: "useast", Not: true},
			},
			result: []uint32{uint32(3)},
		},

		// query against a tag NOT null
		{
			names: []string{"queue_depth"},
			filters: []*TagFilter{
				&TagFilter{Key: "app", Value: "", Not: true},
			},
			result: []uint32{uint32(6)},
		},

		// query against a tag value and another tag NOT value
		{
			names: []string{"queue_depth"},
			filters: []*TagFilter{
				&TagFilter{Key: "name", Value: "high priority"},
				&TagFilter{Key: "app", Value: "paultown", Not: true},
			},
			result: []uint32{uint32(5), uint32(7)},
		},

		// query against a tag value matching regex
		{
			names: []string{"queue_depth"},
			filters: []*TagFilter{
				&TagFilter{Key: "app", Regex: regexp.MustCompile("paul.*")},
			},
			result: []uint32{uint32(6), uint32(7)},
		},

		// query against a tag value matching regex and other tag value matching value
		{
			names: []string{"queue_depth"},
			filters: []*TagFilter{
				&TagFilter{Key: "name", Value: "high priority"},
				&TagFilter{Key: "app", Regex: regexp.MustCompile("paul.*")},
			},
			result: []uint32{uint32(6), uint32(7)},
		},

		// query against a tag value NOT matching regex
		{
			names: []string{"queue_depth"},
			filters: []*TagFilter{
				&TagFilter{Key: "app", Regex: regexp.MustCompile("paul.*"), Not: true},
			},
			result: []uint32{uint32(5)},
		},

		// query against a tag value NOT matching regex and other tag value matching value
		{
			names: []string{"queue_depth"},
			filters: []*TagFilter{
				&TagFilter{Key: "app", Regex: regexp.MustCompile("paul.*"), Not: true},
				&TagFilter{Key: "name", Value: "high priority"},
			},
			result: []uint32{uint32(5)},
		},

		// query against multiple measurements
		{
			names: []string{"cpu_load", "key_count"},
			filters: []*TagFilter{
				&TagFilter{Key: "region", Value: "uswest"},
			},
			result: []uint32{uint32(1), uint32(2), uint32(3)},
		},
	}

	for i, tt := range tests {
		r := db.seriesIDs(tt.names, tt.filters)
		expectedIDs := seriesIDs(tt.result)
		if !r.equals(expectedIDs) {
			t.Fatalf("%d: filters: %s: result mismatch:\n  exp=%s\n  got=%s", i, mustMarshalJSON(tt.filters), mustMarshalJSON(expectedIDs), mustMarshalJSON(r))
		}
	}
}

func TestDatabase_TagKeys(t *testing.T) {
	db := databaseWithFixtureData()

	var tests = []struct {
		names  []string
		result []string
	}{
		{
			names:  nil,
			result: []string{"a", "app", "host", "name", "region", "service"},
		},
		{
			names:  []string{"cpu_load"},
			result: []string{"host", "region"},
		},
		{
			names:  []string{"key_count", "queue_depth"},
			result: []string{"app", "host", "name", "region", "service"},
		},
	}

	for i, tt := range tests {
		r := db.TagKeys(tt.names)
		if !reflect.DeepEqual(r, tt.result) {
			t.Fatalf("%d: names: %s: result mismatch:\n  exp=%s\n  got=%s", i, tt.names, tt.result, r)
		}
	}
}

func TestDatabase_TagValuesWhereTagFilter(t *testing.T) {
	db := databaseWithFixtureData()

	var tests = []struct {
		names   []string
		key     string
		filters []*TagFilter
		result  []string
	}{
		// get the tag values across multiple measurements

		// get the tag values for a single measurement
		{
			names:  []string{"key_count"},
			key:    "region",
			result: []string{"useast", "uswest"},
		},

		// get the tag values for a single measurement with where filter
		{
			names: []string{"key_count"},
			key:   "region",
			filters: []*TagFilter{
				&TagFilter{Key: "host", Value: "serverc.influx.com"},
			},
			result: []string{"uswest"},
		},

		// get the tag values for a single measurement with a not where filter
		{
			names: []string{"key_count"},
			key:   "region",
			filters: []*TagFilter{
				&TagFilter{Key: "host", Value: "serverc.influx.com", Not: true},
			},
			result: []string{"useast"},
		},

		// get the tag values for a single measurement with multiple where filters
		{
			names: []string{"key_count"},
			key:   "region",
			filters: []*TagFilter{
				&TagFilter{Key: "host", Value: "serverc.influx.com"},
				&TagFilter{Key: "service", Value: "redis"},
			},
			result: []string{"uswest"},
		},

		// get the tag values for a single measurement with regex filter
		{
			names: []string{"queue_depth"},
			key:   "name",
			filters: []*TagFilter{
				&TagFilter{Key: "app", Regex: regexp.MustCompile("paul.*")},
			},
			result: []string{"high priority"},
		},

		// get the tag values for a single measurement with a not regex filter
		{
			names: []string{"key_count"},
			key:   "region",
			filters: []*TagFilter{
				&TagFilter{Key: "host", Regex: regexp.MustCompile("serverd.*"), Not: true},
			},
			result: []string{"uswest"},
		},
	}

	for i, tt := range tests {
		r := db.TagValues(tt.names, tt.key, tt.filters).Slice()
		if !reflect.DeepEqual(r, tt.result) {
			t.Fatalf("%d: filters: %s: result mismatch:\n  exp=%s\n  got=%s", i, mustMarshalJSON(tt.filters), tt.result, r)
		}
	}
}

func TestDatabase_DropSeries(t *testing.T) {
	t.Skip("pending")
}

func TestDatabase_DropMeasurement(t *testing.T) {
	t.Skip("pending")
}

func TestDatabase_FieldKeys(t *testing.T) {
	t.Skip("pending")
}

// databaseWithFixtureData returns a populated Index for use in many of the filtering tests
func databaseWithFixtureData() *database {
	db := newDatabase()
	s := &Series{
		ID:   uint32(1),
		Tags: map[string]string{"host": "servera.influx.com", "region": "uswest"}}

	added := db.addSeriesToIndex("cpu_load", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}

	added = db.addSeriesToIndex("cpu_load", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(3),
		Tags: map[string]string{"host": "serverc.influx.com", "region": "uswest", "service": "redis"}}

	added = db.addSeriesToIndex("key_count", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(4),
		Tags: map[string]string{"host": "serverd.influx.com", "region": "useast", "service": "redis"}}

	added = db.addSeriesToIndex("key_count", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(5),
		Tags: map[string]string{"name": "high priority"}}

	added = db.addSeriesToIndex("queue_depth", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(6),
		Tags: map[string]string{"name": "high priority", "app": "paultown"}}

	added = db.addSeriesToIndex("queue_depth", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(7),
		Tags: map[string]string{"name": "high priority", "app": "paulcountry"}}

	added = db.addSeriesToIndex("queue_depth", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(8),
		Tags: map[string]string{"a": "b"}}

	added = db.addSeriesToIndex("another_thing", s)
	if !added {
		return nil
	}

	return db
}

func TestSeriesIDs_intersect(t *testing.T) {
	var tests = []struct {
		expected []uint32
		left     []uint32
		right    []uint32
	}{
		// both sets empty
		{
			expected: []uint32{},
			left:     []uint32{},
			right:    []uint32{},
		},

		// right set empty
		{
			expected: []uint32{},
			left:     []uint32{uint32(1)},
			right:    []uint32{},
		},

		// left set empty
		{
			expected: []uint32{},
			left:     []uint32{},
			right:    []uint32{uint32(1)},
		},

		// both sides same size
		{
			expected: []uint32{uint32(1), uint32(4)},
			left:     []uint32{uint32(1), uint32(2), uint32(4), uint32(5)},
			right:    []uint32{uint32(1), uint32(3), uint32(4), uint32(7)},
		},

		// both sides same size, right boundary checked.
		{
			expected: []uint32{},
			left:     []uint32{uint32(2)},
			right:    []uint32{uint32(1)},
		},

		// left side bigger
		{
			expected: []uint32{uint32(2)},
			left:     []uint32{uint32(1), uint32(2), uint32(3)},
			right:    []uint32{uint32(2)},
		},

		// right side bigger
		{
			expected: []uint32{uint32(4), uint32(8)},
			left:     []uint32{uint32(2), uint32(3), uint32(4), uint32(8)},
			right:    []uint32{uint32(1), uint32(4), uint32(7), uint32(8), uint32(9)},
		},
	}

	for i, tt := range tests {
		a := seriesIDs(tt.left).intersect(tt.right)
		if !a.equals(tt.expected) {
			t.Fatalf("%d: %v intersect %v: result mismatch:\n  exp=%v\n  got=%v", i, seriesIDs(tt.left), seriesIDs(tt.right), seriesIDs(tt.expected), seriesIDs(a))
		}
	}
}

func TestSeriesIDs_union(t *testing.T) {
	var tests = []struct {
		expected []uint32
		left     []uint32
		right    []uint32
	}{
		// both sets empty
		{
			expected: []uint32{},
			left:     []uint32{},
			right:    []uint32{},
		},

		// right set empty
		{
			expected: []uint32{uint32(1)},
			left:     []uint32{uint32(1)},
			right:    []uint32{},
		},

		// left set empty
		{
			expected: []uint32{uint32(1)},
			left:     []uint32{},
			right:    []uint32{uint32(1)},
		},

		// both sides same size
		{
			expected: []uint32{uint32(1), uint32(2), uint32(3), uint32(4), uint32(5), uint32(7)},
			left:     []uint32{uint32(1), uint32(2), uint32(4), uint32(5)},
			right:    []uint32{uint32(1), uint32(3), uint32(4), uint32(7)},
		},

		// left side bigger
		{
			expected: []uint32{uint32(1), uint32(2), uint32(3)},
			left:     []uint32{uint32(1), uint32(2), uint32(3)},
			right:    []uint32{uint32(2)},
		},

		// right side bigger
		{
			expected: []uint32{uint32(1), uint32(2), uint32(3), uint32(4), uint32(7), uint32(8), uint32(9)},
			left:     []uint32{uint32(2), uint32(3), uint32(4), uint32(8)},
			right:    []uint32{uint32(1), uint32(4), uint32(7), uint32(8), uint32(9)},
		},
	}

	for i, tt := range tests {
		a := seriesIDs(tt.left).union(tt.right)
		if !a.equals(tt.expected) {
			t.Fatalf("%d: %v union %v: result mismatch:\n  exp=%v\n  got=%v", i, seriesIDs(tt.left), seriesIDs(tt.right), seriesIDs(tt.expected), seriesIDs(a))
		}
	}
}

func TestSeriesIDs_reject(t *testing.T) {
	var tests = []struct {
		expected []uint32
		left     []uint32
		right    []uint32
	}{
		// both sets empty
		{
			expected: []uint32{},
			left:     []uint32{},
			right:    []uint32{},
		},

		// right set empty
		{
			expected: []uint32{uint32(1)},
			left:     []uint32{uint32(1)},
			right:    []uint32{},
		},

		// left set empty
		{
			expected: []uint32{},
			left:     []uint32{},
			right:    []uint32{uint32(1)},
		},

		// both sides same size
		{
			expected: []uint32{uint32(2), uint32(5)},
			left:     []uint32{uint32(1), uint32(2), uint32(4), uint32(5)},
			right:    []uint32{uint32(1), uint32(3), uint32(4), uint32(7)},
		},

		// left side bigger
		{
			expected: []uint32{uint32(1), uint32(3)},
			left:     []uint32{uint32(1), uint32(2), uint32(3)},
			right:    []uint32{uint32(2)},
		},

		// right side bigger
		{
			expected: []uint32{uint32(2), uint32(3)},
			left:     []uint32{uint32(2), uint32(3), uint32(4), uint32(8)},
			right:    []uint32{uint32(1), uint32(4), uint32(7), uint32(8), uint32(9)},
		},
	}

	for i, tt := range tests {
		a := seriesIDs(tt.left).reject(tt.right)
		if !a.equals(tt.expected) {
			t.Fatalf("%d: %v reject %v: result mismatch:\n  exp=%v\n  got=%v", i, seriesIDs(tt.left), seriesIDs(tt.right), seriesIDs(tt.expected), seriesIDs(a))
		}
	}
}

// Ensure a measurement can return a set of unique tag values specified by an expression.
func TestMeasurement_uniqueTagValues(t *testing.T) {
	// Create a measurement to run against.
	m := NewMeasurement("cpu")
	m.createFieldIfNotExists("value", influxql.Number)

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
	m.createFieldIfNotExists("value", influxql.Number)

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
