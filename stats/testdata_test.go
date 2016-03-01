package stats_test

import (
	"testing"

	"github.com/influxdata/influxdb/stats"
)

// TestData is used to create data driven tests.
type TestData struct {
	method string
	name   string
	value  interface{}
	err    error
}

// dontPanic if there is a panic, instead return the error
func dontPanic(f func()) (result error) {

	defer func() {
		e := recover()
		if e != nil {
			result = e.(error)
		}
	}()

	f()
	return nil
}

// applyToBuilder applies the receiver's data to the specified Builder
func (d *TestData) applyToBuilder(t *testing.T, b stats.Builder) {
	result := dontPanic(func() {

		switch d.method {
		case "DeclareInt":
			b.DeclareInt(d.name, d.value.(int64))
		case "DeclareFloat":
			b.DeclareFloat(d.name, d.value.(float64))
		case "DeclareString":
			b.DeclareString(d.name, d.value.(string))
		default:
			t.Fatalf("unsupported method: %s", d.method)
		}
	})
	if d.err != result {
		t.Fatalf("unexpected result for method call: %s(%s, %v). got: %v, expected: %v", d.method, d.name, d.value, result, d.err)
	}
}

// applyToBuilder applies the receiver's data to the specified Statistics object
func (d *TestData) applyToStatistics(t *testing.T, s stats.Recorder) {
	result := dontPanic(func() {

		switch d.method {
		case "AddInt":
			s.AddInt(d.name, d.value.(int64))
		case "AddFloat":
			s.AddFloat(d.name, d.value.(float64))
		case "SetInt":
			s.SetInt(d.name, d.value.(int64))
		case "SetFloat":
			s.SetFloat(d.name, d.value.(float64))
		case "SetString":
			s.SetString(d.name, d.value.(string))
		default:
			t.Fatalf("unsupported method: %s", d.method)
		}
	})
	if d.err != result {
		t.Fatalf("unexpected result for method call: %s(%s, %v). got: %v, expected: %v", d.method, d.name, d.value, result, d.err)
	}
}
