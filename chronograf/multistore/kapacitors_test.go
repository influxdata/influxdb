package multistore

import (
	"testing"

	"github.com/influxdata/influxdb/v2/chronograf"
)

func TestInterfaceImplementation(t *testing.T) {
	var _ chronograf.ServersStore = &KapacitorStore{}
}
