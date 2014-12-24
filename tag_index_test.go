package influxdb_test

import (
	"testing"

	"github.com/influxdb/influxdb"
)

func TestTagIndex_MeasurementBySeriesID(t *testing.T) {
	idx := influxdb.NewTagIndex()
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

func TestTagIndex_MeasurementsBySeriesIDs(t *testing.T) {
	t.Skip("pending")
}

func TestTagIndex_SeriesBySeriesID(t *testing.T) {
	idx := influxdb.NewTagIndex()

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

func TestTagIndex_MeasurementAndSeries(t *testing.T) {
	idx := influxdb.NewTagIndex()
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

func TestTagIndex_SeriesIDs(t *testing.T) {
	idx := influxdb.NewTagIndex()
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

func TestTagIndex_SeriesIDsWhereFilter(t *testing.T) {
	t.Skip("pending")
}

func TestTagIndex_SeriesIDsWhereFilterMultiple(t *testing.T) {
	t.Skip("pending")
}

func TestTagIndex_SeriesIDsWhereNot(t *testing.T) {
	t.Skip("pending")
}

func TestTagIndex_SeriesIDsWhereFilterAndNot(t *testing.T) {
	t.Skip("pending")
}

func TestTagIndex_FieldKeys(t *testing.T) {
	t.Skip("pending")
}

func TestTagIndex_TagKeys(t *testing.T) {
	t.Skip("pending")
}

func TestTagIndex_TagKeysForMeasurement(t *testing.T) {
	t.Skip("pending")
}

func TestTagIndex_TagValuesWhereFilter(t *testing.T) {
	t.Skip("pending")
}

func TestTagIndex_TagValuesWhereFilterMultiple(t *testing.T) {
	t.Skip("pending")
}

func TestTagIndex_TagValuesWhereNot(t *testing.T) {
	t.Skip("pending")
}

func TestTagIndex_TagValuesWhereFilterAndNot(t *testing.T) {
	t.Skip("pending")
}

func TestTagIndex_DropSeries(t *testing.T) {
	t.Skip("pending")
}

func TestTagIndex_DropMeasurement(t *testing.T) {
	t.Skip("pending")
}
