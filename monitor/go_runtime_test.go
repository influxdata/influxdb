package monitor_test

import (
	"reflect"
	"runtime"
	"testing"

	"github.com/influxdata/influxdb/monitor"
)

func TestDiagnostics_GoRuntime(t *testing.T) {
	s := monitor.New(nil, monitor.Config{})
	if err := s.Open(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer s.Close()

	d, err := s.Diagnostics()
	if err != nil {
		t.Errorf("unexpected error: %s", err)
		return
	}

	diags, ok := d["runtime"]
	if !ok {
		t.Error("no diagnostics found for 'runtime'")
		return
	}

	if got, exp := diags.Columns, []string{"GOARCH", "GOMAXPROCS", "GOOS", "version"}; !reflect.DeepEqual(got, exp) {
		t.Errorf("unexpected columns: got=%v exp=%v", got, exp)
	}

	if got, exp := diags.Rows, [][]interface{}{
		[]interface{}{runtime.GOARCH, runtime.GOMAXPROCS(-1), runtime.GOOS, runtime.Version()},
	}; !reflect.DeepEqual(got, exp) {
		t.Errorf("unexpected rows: got=%v exp=%v", got, exp)
	}
}
