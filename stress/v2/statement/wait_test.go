package statement

import (
	"testing"

	"github.com/influxdata/influxdb/stress/v2/ponyExpress"
)

func TestWaitSetID(t *testing.T) {
	e := newTestWait()
	newID := "oaijnifo"
	e.SetID(newID)
	if e.StatementID != newID {
		t.Errorf("Expected: %v\ngott: %v\n", newID, e.StatementID)
	}
}

func TestWaitRun(t *testing.T) {
	e := newTestWait()
	s, _, _ := ponyExpress.NewTestStoreFront()
	e.Run(s)
}

func TestWaitReport(t *testing.T) {
	e := newTestWait()
	s, _, _ := ponyExpress.NewTestStoreFront()
	e.Report(s)
}

func newTestWait() *WaitStatement {
	return &WaitStatement{
		StatementID: "fooID",
	}
}
