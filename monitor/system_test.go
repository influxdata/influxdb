package monitor_test

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb/monitor"
)

func TestDiagnostics_System(t *testing.T) {
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

	diags, ok := d["system"]
	if !ok {
		t.Fatal("no diagnostics found for 'system'")
	}

	if got, exp := diags.Columns, []string{"PID", "currentTime", "started", "uptime"}; !reflect.DeepEqual(got, exp) {
		t.Errorf("unexpected columns: got=%v exp=%v", got, exp)
	}

	// So this next part is nearly impossible to match, so just check if they look correct.
	if exp, got := 1, len(diags.Rows); exp != got {
		t.Fatalf("expected exactly %d row, got %d", exp, got)
	}

	if got, exp := diags.Rows[0][0].(int), os.Getpid(); got != exp {
		t.Errorf("unexpected pid: got=%v exp=%v", got, exp)
	}

	currentTime := diags.Rows[0][1].(time.Time)
	startTime := diags.Rows[0][2].(time.Time)
	if !startTime.Before(currentTime) {
		t.Errorf("start time is not before the current time: %s (start), %s (current)", startTime, currentTime)
	}

	uptime, err := time.ParseDuration(diags.Rows[0][3].(string))
	if err != nil {
		t.Errorf("unable to parse uptime duration: %s: %s", diags.Rows[0][3], err)
	} else if got, exp := uptime, currentTime.Sub(startTime); got != exp {
		t.Errorf("uptime does not match the difference between start time and current time: got=%v exp=%v", got, exp)
	}
}
