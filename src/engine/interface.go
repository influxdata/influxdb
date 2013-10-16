package engine

import (
	"parser"
	"protocol"
)

type EngineI interface {
	RunQuery(database string, query *parser.Query, yield func(*protocol.Series) error) error
}
