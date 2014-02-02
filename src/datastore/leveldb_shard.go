package datastore

import (
	"github.com/jmhodges/levigo"
	"parser"
	"protocol"
)

type LevelDbShard struct {
	db *levigo.DB
}

func NewLevelDbShard(db *levigo.DB) *LevelDbShard {
	return &LevelDbShard{db: db}
}

func (self *LevelDbShard) Write([]*protocol.Series) error {
	return nil
}

func (self *LevelDbShard) Query(*parser.Query, chan *protocol.Response) error {
	return nil
}
