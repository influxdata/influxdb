package stats_test

import (
	"reflect"
	"sync"
	"testing"

	"github.com/influxdata/influxdb/stats"
)

// testMonitor mimics the essential behaviour of the monitor for the
// purposes of test cases. In particular, it registers an observer for
// new Statistics objects, acquires a reference upon notification and
// releases its own reference once it detects that the number of references
// has dropped to one.
//
// The output of its Observe() method can used to observe the set of Statistics
// the monitor would see if it polled the Registry at a particular point in time.
type testMonitor struct {
	mu    sync.Mutex
	view  stats.View
	count int
}

func newTestMonitor() *testMonitor {
	monitor := &testMonitor{}
	monitor.view = stats.Root.Open()
	return monitor
}

func (m *testMonitor) Close() {
	m.view.Close()
	m.Observe()
}

// Observe returns the set of statistics the monitor would
// see on its nets call to Do. The expected result after
// Statistics.Close() call is that the first call to Observe()
// will include a set of statistics that includes the closed
// Statistics objectnd the text one will not.
func (m *testMonitor) Observe() stats.Collection {
	return stats.Collect(m.view, false)
}

func TestSimulateMonitorBehaviour(t *testing.T) {
	// check that a monitor of an idle registry sees nothing
	monitor := newTestMonitor()
	observed := monitor.Observe()
	expected := stats.Collection{}
	if !reflect.DeepEqual(observed, expected) {
		t.Fatalf("monitor with no activity should be empty. got: %v, expected: %v", observed, expected)
	}
	monitor.Close()

	monitor2 := newTestMonitor()
	defer monitor2.Close()

	// check that a monitor of an idle registry that is closed
	// does not see anything, even if the registry subsequently becomes busy.

	newStat := stats.Root.
		NewBuilder("k", "n", map[string]string{"tag": "T"}).
		MustBuild().
		Open()

	observed = monitor2.Observe()
	expected = []stats.Statistics{newStat}
	if !reflect.DeepEqual(observed, expected) {
		t.Fatalf("open monitor should not be empty. got: %v, expected: %v", observed, expected)
	}

	newStat.Close()

	// The following tests demonstrate the required behaviour that a Statistic is visible to observers
	// on the first observation following its closure, but not on the second.

	observed = monitor2.Observe()
	expected = []stats.Statistics{newStat}
	if !reflect.DeepEqual(observed, expected) {
		t.Fatalf("open monitor should see recently closed stat on first observation. got: %v, expected: %v", observed, expected)
	}

	observed = monitor2.Observe()
	expected = []stats.Statistics{}
	if !reflect.DeepEqual(observed, expected) {
		t.Fatalf("open monitor should see recently closed stat on first observation but not second. got: %v, expected: %v", observed, expected)
	}
}
