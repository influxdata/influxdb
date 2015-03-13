package influxdb_test

import (
	"testing"

	"github.com/influxdb/influxdb"
)

func TestStats_SetAndGet(t *testing.T) {
	s := influxdb.NewStats()

	s.Set("a", 100)
	if s.Get("a") != 100 {
		t.Fatalf("stats set failed, expected 100, got %d", s.Get("a"))
	}
}

func TestStats_Add(t *testing.T) {
	s := influxdb.NewStats()

	s.Add("a", 200)
	if s.Get("a") != 200 {
		t.Fatalf("stats set failed, expected 200, got %d", s.Get("a"))
	}
}

func TestStats_Inc(t *testing.T) {
	s := influxdb.NewStats()

	s.Set("a", 100)
	s.Inc("a")
	if s.Get("a") != 101 {
		t.Fatalf("stats Inc failed, expected 101, got %d", s.Get("a"))
	}

	s.Inc("b")
	if s.Get("b") != 1 {
		t.Fatalf("stats Inc failed, expected 1, got %d", s.Get("b"))
	}
}

func TestStats_AddNegative(t *testing.T) {
	s := influxdb.NewStats()

	s.Add("a", -200)
	if s.Get("a") != -200 {
		t.Fatalf("stats set failed, expected -200, got %d", s.Get("a"))
	}
}

func TestStats_SetAndAdd(t *testing.T) {
	s := influxdb.NewStats()

	s.Set("a", 100)
	s.Add("a", 200)
	if s.Get("a") != 300 {
		t.Fatalf("stats set failed, expected 300, got %d", s.Get("a"))
	}
}

func TestStats_Diff(t *testing.T) {
	foo := influxdb.NewStats()
	bar := influxdb.NewStats()

	foo.Set("a", 100)
	foo.Set("b", 600)
	bar.Set("a", 450)
	bar.Set("b", 525)

	qux := bar.Diff(foo)
	if qux.Get("a") != 350 || qux.Get("b") != -75 {
		t.Fatalf("stats diff returned unexpedted result: %s", qux)
	}
}
