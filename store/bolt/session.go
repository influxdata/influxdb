package bolt

import (
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdata/mrfusion"
)

// Session is a connection to a boltDB database.
// TODO: Hook up authentication here.
type Session struct {
	db  *bolt.DB
	now time.Time

	// Services
	explorationStore ExplorationStore
}

func newSession(db *bolt.DB) *Session {
	s := &Session{db: db}
	s.explorationStore.session = s
	return s
}

func (s *Session) ExplorationStore() mrfusion.ExplorationStore { return &s.explorationStore }
