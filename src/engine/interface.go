package engine

import (
	"protocol"
)

type EngineI interface {
	RunQuery(database string, query string, yield func(*protocol.Series) error) error
}
