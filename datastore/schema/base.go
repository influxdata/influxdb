package schema

import "sync"

var (
	TRUE = true
)

type schemaBase struct {
	closed    bool
	closeLock sync.RWMutex
}

func (self *schemaBase) IsClosed() bool {
	return self.closed
}
