package engine

import "github.com/influxdb/influxdb/protocol"

type CommonMergeEngine struct {
	merger  *Merger
	streams map[string]StreamUpdate
	next    Processor
}

// returns a yield function that will sort points from table1 and
// table2 no matter what the order in which they are received.
func NewCommonMergeEngine(tables []string, mergeColumns bool, ascending bool, next Processor) *CommonMergeEngine {
	cme := &CommonMergeEngine{
		streams: make(map[string]StreamUpdate, len(tables)),
		next:    next,
	}
	streams := make([]StreamQuery, len(tables))
	for i, t := range tables {
		s := NewStream()
		streams[i] = s
		cme.streams[t] = s
	}
	h := &SeriesHeap{Ascending: ascending}
	cme.merger = NewCME("Engine", streams, h, next, mergeColumns)
	return cme
}

func (cme *CommonMergeEngine) Close() error {
	for _, s := range cme.streams {
		s.Close()
	}

	_, err := cme.merger.Update()
	if err != nil {
		return err
	}
	return cme.next.Close()
}

func (cme *CommonMergeEngine) Yield(s *protocol.Series) (bool, error) {
	stream := cme.streams[*s.Name]
	stream.Yield(s)
	return cme.merger.Update()
}

func (cme *CommonMergeEngine) Name() string {
	return "CommonMergeEngine"
}
