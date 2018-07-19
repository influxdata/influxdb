package multistore

import (
	"testing"

	"github.com/influxdata/chronograf"
)

func TestInterfaceImplementation(t *testing.T) {
	var _ chronograf.ServersStore = &KapacitorStore{}
}
