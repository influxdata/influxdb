package schema

import (
	"sync"

	"github.com/influxdb/influxdb/datastore/storage"
)

var (
	TRUE = true
)

type schemaBase struct {
	db        storage.Engine
	closed    bool
	closeLock sync.RWMutex
}

func (self *schemaBase) Close() {
	self.closeLock.Lock()
	defer self.closeLock.Unlock()
	self.closed = true
	self.db.Close()
	self.db = nil
}

func (self *schemaBase) IsClosed() bool {
	return self.closed
}
