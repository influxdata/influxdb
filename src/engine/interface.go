package engine

import (
	"common"
	"protocol"
)

type EngineI interface {
	RunQuery(user common.User, database string, query string, yield func(*protocol.Series) error) error
}
