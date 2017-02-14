package monitor_test

import (
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/monitor"
)

func TestDiagnostics_BuildInfo(t *testing.T) {
	s := monitor.New(nil, monitor.Config{})
	s.Version = "1.2.0"
	s.Commit = "b7bb7e8359642b6e071735b50ae41f5eb343fd42"
	s.Branch = "1.2"
	s.BuildTime = "10m30s"

	if err := s.Open(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer s.Close()

	d, err := s.Diagnostics()
	if err != nil {
		t.Errorf("unexpected error: %s", err)
		return
	}

	diags, ok := d["build"]
	if !ok {
		t.Error("no diagnostics found for 'build'")
		return
	}

	if got, exp := diags.Columns, []string{"Branch", "Build Time", "Commit", "Version"}; !reflect.DeepEqual(got, exp) {
		t.Errorf("unexpected columns: got=%v exp=%v", got, exp)
	}

	if got, exp := diags.Rows, [][]interface{}{
		[]interface{}{"1.2", "10m30s", "b7bb7e8359642b6e071735b50ae41f5eb343fd42", "1.2.0"},
	}; !reflect.DeepEqual(got, exp) {
		t.Errorf("unexpected rows: got=%v exp=%v", got, exp)
	}
}
