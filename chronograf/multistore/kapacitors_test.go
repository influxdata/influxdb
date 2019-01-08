package multistore

import (
	"testing"

	"github.com/influxdata/influxdb/chronograf"
)

func TestInterfaceImplementation(t *testing.T) {
	var _ chronograf.ServersStore = &KapacitorStore{}
}
