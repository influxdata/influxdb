package stats_test

import (
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/stats"
)

func TestDeclares(t *testing.T) {
	builder := stats.Root.
		NewBuilder("k", "n", map[string]string{"tags": "T"})

	for _, d := range []TestData{
		TestData{method: "DeclareInt", name: "intv", value: int64(1), err: nil},
		TestData{method: "DeclareFloat", name: "floatv", value: float64(1.5), err: nil},
		TestData{method: "DeclareString", name: "stringv", value: "1", err: nil},
		TestData{method: "DeclareInt", name: "intv", value: int64(2), err: stats.ErrStatAlreadyDeclared},
		TestData{method: "DeclareFloat", name: "floatv", value: float64(2.0), err: stats.ErrStatAlreadyDeclared},
		TestData{method: "DeclareString", name: "stringv", value: "2", err: stats.ErrStatAlreadyDeclared},
	} {
		d.applyToBuilder(t, builder)
	}

	open := builder.MustBuild().Open()
	values := open.Values()
	open.Close()

	expected := map[string]interface{}{
		"stringv": "1",
		"floatv":  1.5,
		"intv":    int64(1),
	}

	if !reflect.DeepEqual(values, expected) {
		t.Fatalf("incorrect values found. got: %v, expected: %v", values, expected)
	}

	for _, d := range []TestData{
		TestData{method: "DeclareInt", name: "intv", value: int64(2), err: stats.ErrAlreadyBuilt},
		TestData{method: "DeclareFloat", name: "floatv", value: float64(2.0), err: stats.ErrAlreadyBuilt},
		TestData{method: "DeclareString", name: "stringv", value: "2", err: stats.ErrAlreadyBuilt},
		TestData{method: "DeclareInt", name: "newintv", value: int64(2), err: stats.ErrAlreadyBuilt},
		TestData{method: "DeclareFloat", name: "newfloatv", value: float64(2.0), err: stats.ErrAlreadyBuilt},
		TestData{method: "DeclareString", name: "newstringv", value: "2", err: stats.ErrAlreadyBuilt},
	} {
		d.applyToBuilder(t, builder)
	}
}

func TestMustBuild(t *testing.T) {
	builder := stats.Root.
		NewBuilder("k", "n", map[string]string{"tags": "T"})

	builder.MustBuild()

	if err := dontPanic(func() {
		builder.MustBuild()
	}); err != stats.ErrAlreadyBuilt {
		t.Fatalf("MustBuild should fail on second attempt. got: %v, expected: %v", err, stats.ErrAlreadyBuilt)
	}
}

func TestBuild(t *testing.T) {
	builder := stats.Root.
		NewBuilder("k", "n", map[string]string{"tags": "T"})

	_, err := builder.Build()
	if err != nil {
		t.Fatalf("First Build should succeed. got: %v, expected: %v", err, nil)
	}

	_, err = builder.Build()
	if err != stats.ErrAlreadyBuilt {
		t.Fatalf("Second Build should fail. got: %v, expected: %v", err, stats.ErrAlreadyBuilt)
	}
}
