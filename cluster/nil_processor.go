package cluster

import (
	"fmt"

	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/protocol"
)

type NilProcessor struct{}

func (np NilProcessor) Name() string {
	return "NilProcessor"
}

func (np NilProcessor) Yield(s *protocol.Series) (bool, error) {
	return false, fmt.Errorf("Shouldn't get any data")
}

func (np NilProcessor) Close() error {
	return nil
}

func (np NilProcessor) Next() engine.Processor {
	return nil
}
