package multistore

import (
	"testing"

	"github.com/influxdata/platform/chronograf"
)

func TestInterfaceImplementation(t *testing.T) {
	var _ chronograf.ServersStore = &KapacitorStore{}
}
