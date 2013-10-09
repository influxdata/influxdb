package engine

import (
	"coordinator"
	"parser"
	"protocol"
)

type EngineI interface {
	RunQuery(query *parser.Query, yield func(*protocol.Series) error) error
}

func NewQueryEngine(c coordinator.Coordinator) (EngineI, error) {
	return nil, nil
}
