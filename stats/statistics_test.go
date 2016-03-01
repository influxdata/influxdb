package stats_test

import (
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/stats"
)

func TestSetsAndAdds(t *testing.T) {
	builder := stats.Root.
		NewBuilder("k", "n", map[string]string{"tags": "T"})

	for _, d := range []TestData{
		TestData{method: "DeclareInt", name: "intv", value: int64(1), err: nil},
		TestData{method: "DeclareFloat", name: "floatv", value: float64(1.5), err: nil},
		TestData{method: "DeclareString", name: "stringv", value: "1", err: nil},
	} {
		d.applyToBuilder(t, builder)
	}

	open := builder.MustBuild().Open()
	defer open.Close()

	expected := map[string]interface{}{
		"stringv": "1",
		"floatv":  1.5,
		"intv":    int64(1),
	}
	values := open.Values()

	if !reflect.DeepEqual(values, expected) {
		t.Fatalf("incorrect values found. got: %v, expected: %v", values, expected)
	}

	for _, d := range []TestData{
		TestData{method: "SetInt", name: "intv", value: int64(2), err: nil},
		TestData{method: "SetFloat", name: "floatv", value: float64(2.5), err: nil},
		TestData{method: "SetString", name: "stringv", value: "2", err: nil},
	} {
		d.applyToStatistics(t, open)
	}

	expected = map[string]interface{}{
		"stringv": "2",
		"floatv":  2.5,
		"intv":    int64(2),
	}
	values = open.Values()

	if !reflect.DeepEqual(values, expected) {
		t.Fatalf("incorrect values found. got: %v, expected: %v", values, expected)
	}

	for _, d := range []TestData{
		TestData{method: "AddInt", name: "intv", value: int64(3), err: nil},
		TestData{method: "AddFloat", name: "floatv", value: float64(3.5), err: nil},
	} {
		d.applyToStatistics(t, open)
	}

	expected = map[string]interface{}{
		"stringv": "2",
		"floatv":  6.0,
		"intv":    int64(5),
	}
	values = open.Values()

	if !reflect.DeepEqual(values, expected) {
		t.Fatalf("incorrect values found. got: %v, expected: %v", values, expected)
	}

	for _, d := range []TestData{
		TestData{method: "SetFloat", name: "intv", value: float64(4.0), err: stats.ErrStatDeclaredWithDifferentType},
		TestData{method: "SetString", name: "floatv", value: "4.6", err: stats.ErrStatDeclaredWithDifferentType},
		TestData{method: "SetInt", name: "stringv", value: int64(4), err: stats.ErrStatDeclaredWithDifferentType},
	} {
		d.applyToStatistics(t, open)
	}
}
