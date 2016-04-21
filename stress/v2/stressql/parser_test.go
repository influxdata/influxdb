package stressql

import (
	"fmt"
	"os"
	"testing"
)

// Pulls the default configFile and makes sure it parses
func TestParseStatements(t *testing.T) {
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		t.Error("$GOPATH not set")
	}
	stmts, err := ParseStatements(fmt.Sprintf("%v/src/github.com/influxdata/influxdb/stress/v2/file.iql", gopath))
	if err != nil {
		t.Error(err)
	}
	expected := 15
	got := len(stmts)
	if expected != got {
		t.Errorf("expected: %v\ngot: %v\n", expected, got)
	}
}
