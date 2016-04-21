package statement

import (
	"testing"

	"github.com/influxdata/influxdb/stress/v2/ponyExpress"
)

func TestGoSetID(t *testing.T) {
	e := newTestGo()
	newID := "oaijnifo"
	e.SetID(newID)
	if e.StatementID != newID {
		t.Errorf("Expected: %v\nGot: %v\n", newID, e.StatementID)
	}
}

func TestGoRun(t *testing.T) {
	e := newTestGo()
	s, _, _ := ponyExpress.NewTestStoreFront()
	e.Run(s)
}

func TestGoReport(t *testing.T) {
	e := newTestGo()
	s, _, _ := ponyExpress.NewTestStoreFront()
	e.Report(s)
}

func newTestGo() *GoStatement {
	return &GoStatement{
		Statement:   newTestExec(),
		StatementID: "fooID",
	}
}
