package influxdb_test

import (
	"testing"

	"github.com/influxdb/influxdb"
)

// Ensure the stats can set and retrieve a value for a key.
func TestStats_Set(t *testing.T) {
	s := influxdb.NewStats("foo")
	s.Set("a", 100)
	if s.Get("a") != 100 {
		t.Fatalf("stats set error, expected 100, got %d", s.Get("a"))
	}
}

// Ensure that stats can add a value to a key.
func TestStats_Add(t *testing.T) {
	s := influxdb.NewStats("foo")

	s.Add("a", 200)
	if s.Get("a") != 200 {
		t.Fatalf("stats set error, expected 200, got %d", s.Get("a"))
	}
}

// Ensure that stats can subtract by adding a negative number.
func TestStats_Add_Negative(t *testing.T) {
	s := influxdb.NewStats("foo")

	s.Add("a", -200)
	if s.Get("a") != -200 {
		t.Fatalf("stats set error, expected -200, got %d", s.Get("a"))
	}
}

// Ensure that stats can increment a value by 1.
func TestStats_Inc(t *testing.T) {
	s := influxdb.NewStats("foo")

	s.Set("a", 100)
	s.Inc("a")
	if s.Get("a") != 101 {
		t.Fatalf("stats Inc error, expected 101, got %d", s.Get("a"))
	}

	s.Inc("b")
	if s.Get("b") != 1 {
		t.Fatalf("stats Inc error, expected 1, got %d", s.Get("b"))
	}
}

func TestStats_SetAndAdd(t *testing.T) {
	s := influxdb.NewStats("foo")

	s.Set("a", 100)
	s.Add("a", 200)
	if s.Get("a") != 300 {
		t.Fatalf("stats set error, expected 300, got %d", s.Get("a"))
	}
}

func TestStats_Diff(t *testing.T) {
	foo := influxdb.NewStats("server")
	bar := influxdb.NewStats("server")

	foo.Set("a", 100)
	foo.Set("b", 600)
	bar.Set("a", 450)
	bar.Set("b", 525)

	qux := bar.Diff(foo)
	if qux.Name() != "server" {
		t.Fatalf("stats diff has unexpected name: %s", qux.Name())
	}
	if qux.Get("a") != 350 || qux.Get("b") != -75 {
		t.Fatalf("stats diff returned unexpected result: %v", qux)
	}
}

func TestStats_Snapshot(t *testing.T) {
	s := influxdb.NewStats("server")
	s.Set("a", 100)
	s.Set("b", 600)

	other := s.Clone()
	if other.Name() != "server" || other.Get("a") != 100 || other.Get("b") != 600 {
		t.Fatalf("stats snapshot returned unexpected result: %#v", other)
	}
}

func TestStats_String(t *testing.T) {
	s := influxdb.NewStats("server")
	s.Set("a", 100)
	s.Set("b", 600)

	if exp, got := `{"server":[{"a":100},{"b":600}]}`, s.String(); exp != got {
		t.Log("exp: ", exp)
		t.Log("got: ", got)
		t.Fatalf("error to get string")
	}
}
