package engine

import (
	"parser"
	"protocol"
)

type EngineI interface {
	RunQuery(query *parser.Query, yield func(*protocol.Series) error) error
}
