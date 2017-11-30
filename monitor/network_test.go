package monitor_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/monitor"
)

func TestDiagnostics_Network(t *testing.T) {
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("unexpected error retrieving hostname: %s", err)
	}

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

	diags, ok := d["network"]
	if !ok {
		t.Error("no diagnostics found for 'network'")
		return
	}

	if got, exp := diags.Columns, []string{"hostname"}; !reflect.DeepEqual(got, exp) {
		t.Errorf("unexpected columns: got=%v exp=%v", got, exp)
	}

	if got, exp := diags.Rows, [][]interface{}{
		[]interface{}{hostname},
	}; !reflect.DeepEqual(got, exp) {
		t.Errorf("unexpected rows: got=%v exp=%v", got, exp)
	}
}
