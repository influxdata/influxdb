package execute

import (
	"sync"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/plan"
)

// result implements both the Transformation and Result interfaces,
// mapping the pushed based Transformation API to the pull based Result interface.
type result struct {
	name string

	mu     sync.Mutex
	tables chan resultMessage

	abortErr chan error
	aborted  chan struct{}
}

type resultMessage struct {
	table query.Table
	err   error
}

func newResult(name string, spec plan.YieldSpec) *result {
	return &result{
		name: name,
		// TODO(nathanielc): Currently this buffer needs to be big enough hold all result tables :(
		tables:   make(chan resultMessage, 1000),
		abortErr: make(chan error, 1),
		aborted:  make(chan struct{}),
	}
}

func (s *result) Name() string {
	return s.name
}
func (s *result) RetractTable(DatasetID, query.GroupKey) error {
	//TODO implement
	return nil
}

func (s *result) Process(id DatasetID, tbl query.Table) error {
	select {
	case s.tables <- resultMessage{
		table: tbl,
	}:
	case <-s.aborted:
	}
	return nil
}

func (s *result) Tables() query.TableIterator {
	return s
}

func (s *result) Do(f func(query.Table) error) error {
	for {
		select {
		case err := <-s.abortErr:
			return err
		case msg, more := <-s.tables:
			if !more {
				return nil
			}
			if msg.err != nil {
				return msg.err
			}
			if err := f(msg.table); err != nil {
				return err
			}
		}
	}
}

func (s *result) UpdateWatermark(id DatasetID, mark Time) error {
	//Nothing to do
	return nil
}
func (s *result) UpdateProcessingTime(id DatasetID, t Time) error {
	//Nothing to do
	return nil
}

func (s *result) setTrigger(Trigger) {
	//TODO: Change interfaces so that resultSink, does not need to implement this method.
}

func (s *result) Finish(id DatasetID, err error) {
	if err != nil {
		select {
		case s.tables <- resultMessage{
			err: err,
		}:
		case <-s.aborted:
		}
	}
	close(s.tables)
}

// Abort the result with the given error
func (s *result) abort(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if we have already aborted
	aborted := false
	select {
	case <-s.aborted:
		aborted = true
	default:
	}
	if aborted {
		return // already aborted
	}

	s.abortErr <- err
	close(s.aborted)
}
