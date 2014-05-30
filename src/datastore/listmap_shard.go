package datastore

import (
	"cluster"
	"parser"
	"protocol"

	"github.com/PreetamJinka/listmap"
)

// ListmapShard represents a set of Listmaps for
// a chunk of contiguous time.
type ListmapShard struct {
	dbs    map[string]*listmap.Listmap
	closed bool
}

func (s *ListmapShard) NewListmapShard() (*ListmapShard, error) {
	return &ListmapShard{
		closed: false,
	}, nil
}

func (s *ListmapShard) DropDatabase(database string) error {
	if m, present := s.dbs[database]; present {
		m.Destroy()
		delete(s.dbs, database)
	}
	return nil
}

func (s *ListmapShard) close() {
	for _, m := range s.dbs {
		m.Close()
	}
	s.closed = true
}

func (s *ListmapShard) IsClosed() bool {
	return s.closed
}

func (s *ListmapShard) Query(querySpec *parser.QuerySpec, processor cluster.QueryProcessor) error {
	return nil
}

func (s *ListmapShard) Write(database string, series []*protocol.Series) error {
	m, present := s.dbs[database]
	if !present {
		s.dbs[database] = listmap.NewListmap("/tmp/listmap." + database)
		m = s.dbs[database]
	}
	m.Close()
	return nil
}
