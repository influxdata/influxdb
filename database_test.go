package influxdb

import (
	"reflect"
	"regexp"
	"sort"
	"testing"
)

// Ensure that the index will return a sorted array of measurement names.
func TestDatabase_Names(t *testing.T) {
	idx := databaseWithFixtureData()

	r := idx.Names()
	exp := []string{"another_thing", "cpu_load", "key_count", "queue_depth"}
	if !reflect.DeepEqual(r, exp) {
		t.Fatalf("Names not equal:\n  got: %s\n  exp: %s", r, exp)
	}
}

// Ensure that we can get the measurement by the series ID.
func TestDatabase_MeasurementBySeriesID(t *testing.T) {
	idx := newDatabase()
	m := &Measurement{
		Name: "cpu_load",
	}
	s := &Series{
		ID:   uint32(1),
		Tags: map[string]string{"host": "servera.influx.com", "region": "uswest"},
	}

	// add it and see if we can look it up
	idx.addSeriesToIndex(m.Name, s)
	mm := idx.MeasurementBySeriesID(uint32(1))
	if string(mustMarshalJSON(m)) != string(mustMarshalJSON(mm)) {
		t.Fatalf("mesurement not equal:\n%v\n%v", m, mm)
	}

	// now test that we can add another
	s = &Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}

	idx.addSeriesToIndex(m.Name, s)
	mm = idx.MeasurementBySeriesID(uint32(2))
	if string(mustMarshalJSON(m)) != string(mustMarshalJSON(mm)) {
		t.Fatalf("mesurement not equal:\n%v\n%v", m, mm)
	}

	mm = idx.MeasurementBySeriesID(uint32(1))
	if string(mustMarshalJSON(m)) != string(mustMarshalJSON(mm)) {
		t.Fatalf("mesurement not equal:\n%v\n%v", m, mm)
	}
}

// Ensure that we can get an array of unique measurements by a collection of series IDs.
func TestDatabase_MeasurementsBySeriesIDs(t *testing.T) {
	idx := databaseWithFixtureData()

	ids := SeriesIDs([]uint32{uint32(1), uint32(4)})
	names := make([]string, 0)
	for _, m := range idx.MeasurementsBySeriesIDs(ids) {
		names = append(names, m.Name)
	}
	sort.Strings(names)
	expected := []string{"cpu_load", "key_count"}
	if !reflect.DeepEqual(names, expected) {
		t.Fatalf("wrong measurements:\n  exp: %s\n  got: %s", expected, names)
	}
}

// Ensure that we can get the series object by the series ID.
func TestDatabase_SeriesBySeriesID(t *testing.T) {
	idx := newDatabase()

	// now test that we can add another
	s := &Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}

	idx.addSeriesToIndex("foo", s)
	ss := idx.SeriesByID(uint32(2))
	if string(mustMarshalJSON(s)) != string(mustMarshalJSON(ss)) {
		t.Fatalf("series not equal:\n%v\n%v", s, ss)
	}
}

// Ensure that we can get the measurement and series objects out based on measurement and tags.
func TestDatabase_MeasurementAndSeries(t *testing.T) {
	idx := newDatabase()
	m := &Measurement{
		Name: "cpu_load",
	}
	s := &Series{
		ID:   uint32(1),
		Tags: map[string]string{"host": "servera.influx.com", "region": "uswest"},
	}

	// add it and see if we can look it up by name and tags
	idx.addSeriesToIndex(m.Name, s)
	mm, ss := idx.MeasurementAndSeries(m.Name, s.Tags)
	if string(mustMarshalJSON(m)) != string(mustMarshalJSON(mm)) {
		t.Fatalf("mesurement not equal:\n%v\n%v", m, mm)
	} else if string(mustMarshalJSON(s)) != string(mustMarshalJSON(ss)) {
		t.Fatalf("series not equal:\n%v\n%v", s, ss)
	}

	// now test that we can add another
	s = &Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}

	idx.addSeriesToIndex(m.Name, s)
	mm, ss = idx.MeasurementAndSeries(m.Name, s.Tags)
	if string(mustMarshalJSON(m)) != string(mustMarshalJSON(mm)) {
		t.Fatalf("mesurement not equal:\n%v\n%v", m, mm)
	} else if string(mustMarshalJSON(s)) != string(mustMarshalJSON(ss)) {
		t.Fatalf("series not equal:\n%v\n%v", s, ss)
	}
}

// Ensure that we can get the series IDs for measurements without any filters.
func TestDatabase_SeriesIDs(t *testing.T) {
	idx := newDatabase()
	s := &Series{
		ID:   uint32(1),
		Tags: map[string]string{"host": "servera.influx.com", "region": "uswest"}}

	// add it and see if we can look it up
	added := idx.addSeriesToIndex("cpu_load", s)
	if !added {
		t.Fatal("couldn't add series")
	}

	// test that we can't add it again
	added = idx.addSeriesToIndex("cpu_load", s)
	if added {
		t.Fatal("shoulnd't be able to add duplicate series")
	}

	// now test that we can add another
	s = &Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}
	added = idx.addSeriesToIndex("cpu_load", s)
	if !added {
		t.Fatalf("couldn't add series")
	}

	l := idx.SeriesIDs([]string{"cpu_load"}, nil)
	r := []uint32{1, 2}
	if !l.Equals(r) {
		t.Fatalf("series IDs not the same:\n%d\n%d", l, r)
	}

	// now add another in a different measurement
	s = &Series{
		ID:   uint32(3),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}
	added = idx.addSeriesToIndex("network_in", s)
	if !added {
		t.Fatalf("couldn't add series")
	}

	l = idx.SeriesIDs([]string{"cpu_load"}, nil)
	r = []uint32{1, 2, 3}
	if !l.Equals(r) {
		t.Fatalf("series IDs not the same:\n%d\n%d", l, r)
	}
}

func TestDatabase_SeriesIDsWhereTagFilter(t *testing.T) {
	idx := databaseWithFixtureData()

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
		r := idx.SeriesIDs(tt.names, tt.filters)
		expectedIDs := SeriesIDs(tt.result)
		if !r.Equals(expectedIDs) {
			t.Fatalf("%d: filters: %s: result mismatch:\n  exp=%s\n  got=%s", i, mustMarshalJSON(tt.filters), mustMarshalJSON(expectedIDs), mustMarshalJSON(r))
		}
	}
}

func TestDatabase_TagKeys(t *testing.T) {
	idx := databaseWithFixtureData()

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
		r := idx.TagKeys(tt.names)
		if !reflect.DeepEqual(r, tt.result) {
			t.Fatalf("%d: names: %s: result mismatch:\n  exp=%s\n  got=%s", i, tt.names, tt.result, r)
		}
	}
}

func TestDatabase_TagValuesWhereTagFilter(t *testing.T) {
	idx := databaseWithFixtureData()

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
		r := idx.TagValues(tt.names, tt.key, tt.filters).ToSlice()
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
	idx := newDatabase()
	s := &Series{
		ID:   uint32(1),
		Tags: map[string]string{"host": "servera.influx.com", "region": "uswest"}}

	added := idx.addSeriesToIndex("cpu_load", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}

	added = idx.addSeriesToIndex("cpu_load", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(3),
		Tags: map[string]string{"host": "serverc.influx.com", "region": "uswest", "service": "redis"}}

	added = idx.addSeriesToIndex("key_count", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(4),
		Tags: map[string]string{"host": "serverd.influx.com", "region": "useast", "service": "redis"}}

	added = idx.addSeriesToIndex("key_count", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(5),
		Tags: map[string]string{"name": "high priority"}}

	added = idx.addSeriesToIndex("queue_depth", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(6),
		Tags: map[string]string{"name": "high priority", "app": "paultown"}}

	added = idx.addSeriesToIndex("queue_depth", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(7),
		Tags: map[string]string{"name": "high priority", "app": "paulcountry"}}

	added = idx.addSeriesToIndex("queue_depth", s)
	if !added {
		return nil
	}

	s = &Series{
		ID:   uint32(8),
		Tags: map[string]string{"a": "b"}}

	added = idx.addSeriesToIndex("another_thing", s)
	if !added {
		return nil
	}

	return idx
}

func TestDatabase_SeriesIDsIntersect(t *testing.T) {
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
		a := SeriesIDs(tt.left).Intersect(tt.right)
		if !a.Equals(tt.expected) {
			t.Fatalf("%d: %v intersect %v: result mismatch:\n  exp=%v\n  got=%v", i, SeriesIDs(tt.left), SeriesIDs(tt.right), SeriesIDs(tt.expected), SeriesIDs(a))
		}
	}
}

func TestDatabase_SeriesIDsUnion(t *testing.T) {
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
		a := SeriesIDs(tt.left).Union(tt.right)
		if !a.Equals(tt.expected) {
			t.Fatalf("%d: %v union %v: result mismatch:\n  exp=%v\n  got=%v", i, SeriesIDs(tt.left), SeriesIDs(tt.right), SeriesIDs(tt.expected), SeriesIDs(a))
		}
	}
}

func TestDatabase_SeriesIDsReject(t *testing.T) {
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
		a := SeriesIDs(tt.left).Reject(tt.right)
		if !a.Equals(tt.expected) {
			t.Fatalf("%d: %v reject %v: result mismatch:\n  exp=%v\n  got=%v", i, SeriesIDs(tt.left), SeriesIDs(tt.right), SeriesIDs(tt.expected), SeriesIDs(a))
		}
	}
}
