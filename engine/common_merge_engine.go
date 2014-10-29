package engine

import (
	"code.google.com/p/log4go"
	"github.com/influxdb/influxdb/protocol"
)

type CommonMergeEngine struct {
	merger  *Merger
	streams map[uint32]StreamUpdate
	next    Processor
}

// returns a yield function that will sort points from table1 and
// table2 no matter what the order in which they are received.
func NewCommonMergeEngine(shards []uint32, mergeColumns bool, ascending bool, next Processor) *CommonMergeEngine {
	cme := &CommonMergeEngine{
		streams: make(map[uint32]StreamUpdate, len(shards)),
		next:    next,
	}
	streams := make([]StreamQuery, len(shards))
	for i, sh := range shards {
		s := NewStream()
		streams[i] = s
		cme.streams[sh] = s
	}
	h := NewSeriesHeap(ascending)
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
	log4go.Fine("CommonMergeEngine.Yield(): %s", s)
	stream := cme.streams[s.GetShardId()]
	stream.Yield(s)
	return cme.merger.Update()
}

func (cme *CommonMergeEngine) Name() string {
	return "CommonMergeEngine"
}

func (self *CommonMergeEngine) Next() Processor {
	return self.next
}
