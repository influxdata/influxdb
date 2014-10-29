package cluster

import (
	"fmt"

	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/protocol"
)

// A processor to set the ShardId on the series to `id`
type ShardIdInserterProcessor struct {
	id   uint32
	next engine.Processor
}

func NewShardIdInserterProcessor(id uint32, next engine.Processor) ShardIdInserterProcessor {
	return ShardIdInserterProcessor{id, next}
}

func (sip ShardIdInserterProcessor) Yield(s *protocol.Series) (bool, error) {
	s.ShardId = &sip.id
	return sip.next.Yield(s)
}

func (sip ShardIdInserterProcessor) Close() error {
	return sip.next.Close()
}

func (sip ShardIdInserterProcessor) Name() string {
	return fmt.Sprintf("ShardIdInserterProcessor (%d)", sip.id)
}
func (sip ShardIdInserterProcessor) Next() engine.Processor {
	return sip.next
}
