package influxdb_test

import (
	"testing"

	"github.com/influxdb/influxdb"
)

// Ensure that we can get the measurement by the series ID.
func TestIndex_MeasurementBySeriesID(t *testing.T) {
	idx := influxdb.NewIndex()
	m := &influxdb.Measurement{
		Name: "cpu_load",
		Series: []*influxdb.Series{
			&influxdb.Series{
				ID:   uint32(1),
				Tags: map[string]string{"host": "servera.influx.com", "region": "uswest"}}}}

	// add it and see if we can look it up
	idx.AddSeries(m.Name, m.Series[0])
	mm := idx.MeasurementBySeriesID(uint32(1))
	if mustMarshalJSON(m) != mustMarshalJSON(mm) {
		t.Fatalf("mesurement not equal:\n%s\n%s", m, mm)
	}

	// now test that we can add another
	s := &influxdb.Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}
	m.Series = append(m.Series, s)

	idx.AddSeries(m.Name, s)
	mm = idx.MeasurementBySeriesID(uint32(2))
	if mustMarshalJSON(m) != mustMarshalJSON(mm) {
		t.Fatalf("mesurement not equal:\n%s\n%s", m, mm)
	}

	mm = idx.MeasurementBySeriesID(uint32(1))
	if mustMarshalJSON(m) != mustMarshalJSON(mm) {
		t.Fatalf("mesurement not equal:\n%s\n%s", m, mm)
	}
}

// Ensure that we can get an array of unique measurements by a collection of series IDs.
func TestIndex_MeasurementsBySeriesIDs(t *testing.T) {
	t.Skip("pending")
}

// Ensure that we can get the series object by the series ID.
func TestIndex_SeriesBySeriesID(t *testing.T) {
	idx := influxdb.NewIndex()

	// now test that we can add another
	s := &influxdb.Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}

	idx.AddSeries("foo", s)
	ss := idx.SeriesByID(uint32(2))
	if mustMarshalJSON(s) != mustMarshalJSON(ss) {
		t.Fatalf("series not equal:\n%s\n%s", s, ss)
	}
}

// Ensure that we can get the measurement and series objects out based on measurement and tags.
func TestIndex_MeasurementAndSeries(t *testing.T) {
	idx := influxdb.NewIndex()
	m := &influxdb.Measurement{
		Name: "cpu_load",
		Series: []*influxdb.Series{
			&influxdb.Series{
				ID:   uint32(1),
				Tags: map[string]string{"host": "servera.influx.com", "region": "uswest"}}}}
	s := m.Series[0]

	// add it and see if we can look it up by name and tags
	idx.AddSeries(m.Name, s)
	mm, ss := idx.MeasurementAndSeries(m.Name, s.Tags)
	if mustMarshalJSON(m) != mustMarshalJSON(mm) {
		t.Fatalf("mesurement not equal:\n%s\n%s", m, mm)
	} else if mustMarshalJSON(s) != mustMarshalJSON(ss) {
		t.Fatalf("series not equal:\n%s\n%s", s, ss)
	}

	// now test that we can add another
	s = &influxdb.Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}
	m.Series = append(m.Series, s)

	idx.AddSeries(m.Name, s)
	mm, ss = idx.MeasurementAndSeries(m.Name, s.Tags)
	if mustMarshalJSON(m) != mustMarshalJSON(mm) {
		t.Fatalf("mesurement not equal:\n%s\n%s", m, mm)
	} else if mustMarshalJSON(s) != mustMarshalJSON(ss) {
		t.Fatalf("series not equal:\n%s\n%s", s, ss)
	}
}

// Ensure that we can get the series IDs for measurements without any filters.
func TestIndex_SeriesIDs(t *testing.T) {
	idx := influxdb.NewIndex()
	s := &influxdb.Series{
		ID:   uint32(1),
		Tags: map[string]string{"host": "servera.influx.com", "region": "uswest"}}

	// add it and see if we can look it up
	added := idx.AddSeries("cpu_load", s)
	if !added {
		t.Fatal("couldn't add series")
	}

	// test that we can't add it again
	added = idx.AddSeries("cpu_load", s)
	if added {
		t.Fatal("shoulnd't be able to add duplicate series")
	}

	// now test that we can add another
	s = &influxdb.Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}
	added = idx.AddSeries("cpu_load", s)
	if !added {
		t.Fatalf("couldn't add series")
	}

	l := idx.SeriesIDs([]string{"cpu_load"}, nil)
	r := []uint32{1, 2}
	if !l.Equals(r) {
		t.Fatalf("series IDs not the same:\n%s\n%s", l, r)
	}

	// now add another in a different measurement
	s = &influxdb.Series{
		ID:   uint32(3),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}
	added = idx.AddSeries("network_in", s)
	if !added {
		t.Fatalf("couldn't add series")
	}

	l = idx.SeriesIDs([]string{"cpu_load"}, nil)
	r = []uint32{1, 2, 3}
	if !l.Equals(r) {
		t.Fatalf("series IDs not the same:\n%s\n%s", l, r)
	}
}

func TestIndex_SeriesIDsWhereFilter(t *testing.T) {
	idx := indexWithFixtureData()

	var tests = []struct {
		names   []string
		filters []*influxdb.Filter
		result  []uint32
	}{
		// match against no tags
		{
			names:  []string{"cpu_load", "redis"},
			result: []uint32{uint32(1), uint32(2), uint32(3), uint32(4), uint32(5)},
		},

		// match against all tags
		{
			names: []string{"cpu_load"},
			filters: []*influxdb.Filter{
				&influxdb.Filter{Key: "host", Value: "servera.influx.com"},
				&influxdb.Filter{Key: "region", Value: "uswest"},
			},
			result: []uint32{uint32(1)},
		},

		// match against one tag
		{
			names: []string{"cpu_load"},
			filters: []*influxdb.Filter{
				&influxdb.Filter{Key: "region", Value: "uswest"},
			},
			result: []uint32{uint32(1), uint32(2)},
		},

		// match against one tag, single result
		{
			names: []string{"cpu_load"},
			filters: []*influxdb.Filter{
				&influxdb.Filter{Key: "host", Value: "servera.influx.com"},
			},
			result: []uint32{uint32(1)},
		},

		// query against tag key that doesn't exist returns empty
		{
			names: []string{"cpu_load"},
			filters: []*influxdb.Filter{
				&influxdb.Filter{Key: "foo", Value: "bar"},
			},
			result: []uint32{},
		},

		// query against tag value that doesn't exist returns empty
		{
			names: []string{"cpu_load"},
			filters: []*influxdb.Filter{
				&influxdb.Filter{Key: "host", Value: "foo"},
			},
			result: []uint32{},
		},

		// query against a tag NOT value

		// query against a tag NOT null

		// query against a tag value and a NOT value on the same key

		// query against a tag value and another tag NOT value

		// query against a tag value matching regex

		// query against a tag value matching regex and other tag value matching value

		// query against a tag value NOT matching regex

		// query against a tag value NOT matching regex and other tag value matching value
	}

	for i, tt := range tests {
		r := idx.SeriesIDs(tt.names, tt.filters)
		expectedIDs := influxdb.SeriesIDs(tt.result)
		if !r.Equals(expectedIDs) {
			t.Fatalf("%d: filters: %s: result mismatch:\n  exp=%s\n  got=%s", i, influxdb.Filters(tt.filters), expectedIDs, r)
		}
	}
}

func TestIndex_FieldKeys(t *testing.T) {
	t.Skip("pending")
}

func TestIndex_TagKeys(t *testing.T) {
	t.Skip("pending")
}

func TestIndex_TagKeysForMeasurement(t *testing.T) {
	t.Skip("pending")
}

func TestIndex_TagValuesWhereFilter(t *testing.T) {
	t.Skip("pending")
}

func TestIndex_TagValuesWhereFilterMultiple(t *testing.T) {
	t.Skip("pending")
}

func TestIndex_TagValuesWhereNot(t *testing.T) {
	t.Skip("pending")
}

func TestIndex_TagValuesWhereFilterAndNot(t *testing.T) {
	t.Skip("pending")
}

func TestIndex_MeasurementsWhereFilter(t *testing.T) {
	t.Skip("pending")
}

func TestIndex_DropSeries(t *testing.T) {
	t.Skip("pending")
}

func TestIndex_DropMeasurement(t *testing.T) {
	t.Skip("pending")
}

// indexWithFixtureData returns a populated Index for use in many of the filtering tests
func indexWithFixtureData() *influxdb.Index {
	idx := influxdb.NewIndex()
	s := &influxdb.Series{
		ID:   uint32(1),
		Tags: map[string]string{"host": "servera.influx.com", "region": "uswest"}}

	added := idx.AddSeries("cpu_load", s)
	if !added {
		return nil
	}

	s = &influxdb.Series{
		ID:   uint32(2),
		Tags: map[string]string{"host": "serverb.influx.com", "region": "uswest"}}

	added = idx.AddSeries("cpu_load", s)
	if !added {
		return nil
	}

	s = &influxdb.Series{
		ID:   uint32(3),
		Tags: map[string]string{"host": "serverc.influx.com", "region": "uswest", "service": "redis"}}

	added = idx.AddSeries("key_count", s)
	if !added {
		return nil
	}

	s = &influxdb.Series{
		ID:   uint32(4),
		Tags: map[string]string{"host": "serverd.influx.com", "region": "useast", "service": "redis"}}

	added = idx.AddSeries("key_count", s)
	if !added {
		return nil
	}

	s = &influxdb.Series{
		ID:   uint32(5),
		Tags: map[string]string{"name": "high priority"}}

	added = idx.AddSeries("queue_depth", s)
	if !added {
		return nil
	}

	return idx
}

func TestIndex_SeriesIDsIntersect(t *testing.T) {
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
		a := influxdb.SeriesIDs(tt.left).Intersect(tt.right)
		if !a.Equals(tt.expected) {
			t.Fatalf("%d: %s intersect %s: result mismatch:\n  exp=%s\n  got=%s", i, influxdb.SeriesIDs(tt.left), influxdb.SeriesIDs(tt.right), influxdb.SeriesIDs(tt.expected), influxdb.SeriesIDs(a))
		}
	}
}
