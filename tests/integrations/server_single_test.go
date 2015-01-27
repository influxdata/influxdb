package integrations_test

import (
	"testing"

	"github.com/influxdb/influxdb/tests/integrations"
)

func TestNewServer(t *testing.T) {
	s, err := integrations.NewServer()
	if err != nil {
		t.Fatalf("Unable to create a new server: %v", err)
	}
	err = s.Start()
	if err != nil {
		t.Fatalf("Unable to start server: %v", err)
	}
}
