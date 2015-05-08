package influxdb

import (
	"errors"
	"time"
)

const defaultReadTimeout = 5 * time.Second

var ErrTimeout = errors.New("timeout")

// Coordinator handle queries and writes across multiple local and remote
// data nodes.
type Coordinator struct {
}

// Write is coordinates multiple writes across local and remote data nodes
// according the request consistency level
func (c *Coordinator) Write(p *WritePointsRequest) error {

	// FIXME: use the consistency level specified by the WritePointsRequest
	pol := newConsistencyPolicyN(1)

	// FIXME: build set of local and remote point writers
	ws := []PointsWriter{}

	type result struct {
		writerID int
		err      error
	}
	ch := make(chan result, len(ws))
	for i, w := range ws {
		go func(id int, w PointsWriter) {
			err := w.Write(p)
			ch <- result{id, err}
		}(i, w)
	}
	timeout := time.After(defaultReadTimeout)
	for range ws {
		select {
		case <-timeout:
			// return timeout error to caller
			return ErrTimeout
		case res := <-ch:
			if !pol.IsDone(res.writerID, res.err) {
				continue
			}
			if res.err != nil {
				return res.err
			}
			return nil
		}

	}
	panic("unreachable or bad policy impl")
}

func (c *Coordinator) Execute(q *QueryRequest) (chan *Result, error) {
	return nil, nil
}

// remoteWriter is a PointWriter for a remote data node
type remoteWriter struct {
	//ShardInfo []ShardInfo
	//DataNodes DataNodes
}

func (w *remoteWriter) Write(p *WritePointsRequest) error {
	return nil
}
