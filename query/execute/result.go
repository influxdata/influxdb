package execute

import (
	"sync"

	"github.com/influxdata/ifql/query/plan"
)

type Result interface {
	// Blocks returns a BlockIterator for iterating through results
	Blocks() BlockIterator
}

// result implements both the Transformation and Result interfaces,
// mapping the pushed based Transformation API to the pull based Result interface.
type result struct {
	mu     sync.Mutex
	blocks chan resultMessage

	abortErr chan error
	aborted  chan struct{}
}

type resultMessage struct {
	block Block
	err   error
}

func newResult(plan.YieldSpec) *result {
	return &result{
		// TODO(nathanielc): Currently this buffer needs to be big enough hold all result blocks :(
		blocks:   make(chan resultMessage, 1000),
		abortErr: make(chan error, 1),
		aborted:  make(chan struct{}),
	}
}

func (s *result) RetractBlock(DatasetID, PartitionKey) error {
	//TODO implement
	return nil
}

func (s *result) Process(id DatasetID, b Block) error {
	select {
	case s.blocks <- resultMessage{
		block: b,
	}:
	case <-s.aborted:
	}
	return nil
}

func (s *result) Blocks() BlockIterator {
	return s
}

func (s *result) Do(f func(Block) error) error {
	for {
		select {
		case err := <-s.abortErr:
			return err
		case msg, more := <-s.blocks:
			if !more {
				return nil
			}
			if msg.err != nil {
				return msg.err
			}
			if err := f(msg.block); err != nil {
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
		case s.blocks <- resultMessage{
			err: err,
		}:
		case <-s.aborted:
		}
	}
	close(s.blocks)
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
