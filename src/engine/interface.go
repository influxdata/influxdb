package engine

import (
	"coordinator"
	"parser"
	"protocol"
)

type EngineI interface {
	RunQuery(query *parser.Query, yield func(*protocol.Series) error) error
}

type QueryEngine struct {
	coordinator coordinator.Coordinator
}

func (self *QueryEngine) RunQuery(query *parser.Query, yield func(*protocol.Series) error) error {
	return nil
}

func NewQueryEngine(c coordinator.Coordinator) (EngineI, error) {
	return &QueryEngine{c}, nil
}
