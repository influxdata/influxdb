package engine

// This engine buffers points and passes them through without modification. Works for queries
// that can't be aggregated locally or queries that don't require it like deletes and drops.
import (
	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/protocol"
)

type Passthrough struct {
	next                Processor
	series              *protocol.Series
	maxPointsInResponse int
	limiter             *Limiter

	// query statistics
	runStartTime  float64
	runEndTime    float64
	pointsRead    int64
	pointsWritten int64
	shardId       int
	shardLocal    bool
}

func NewPassthroughEngine(next Processor, maxPointsInResponse int) *Passthrough {
	return NewPassthroughEngineWithLimit(next, maxPointsInResponse, 0)
}

func NewPassthroughEngineWithLimit(next Processor, maxPointsInResponse, limit int) *Passthrough {
	passthroughEngine := &Passthrough{
		next:                next,
		maxPointsInResponse: maxPointsInResponse,
		limiter:             NewLimiter(limit),
		runStartTime:        0,
		runEndTime:          0,
		pointsRead:          0,
		pointsWritten:       0,
		shardId:             0,
		shardLocal:          false, //that really doesn't matter if it is not EXPLAIN query
	}

	return passthroughEngine
}

func (self *Passthrough) Yield(seriesIncoming *protocol.Series) (bool, error) {
	log.Debug("PassthroughEngine YieldSeries %d", len(seriesIncoming.Points))

	self.limiter.calculateLimitAndSlicePoints(seriesIncoming)
	if len(seriesIncoming.Points) == 0 {
		return false, nil
	}

	if self.series == nil {
		self.series = seriesIncoming
	} else if self.series.GetName() != seriesIncoming.GetName() {
		log.Debug("Yielding to %s: %s", self.next.Name(), self.series)
		ok, err := self.next.Yield(self.series)
		if !ok || err != nil {
			return ok, err
		}
		self.series = seriesIncoming
	} else if len(self.series.Points) > self.maxPointsInResponse {
		log.Debug("Yielding to %s: %s", self.next.Name(), self.series)
		ok, err := self.next.Yield(self.series)
		if !ok || err != nil {
			return ok, err
		}
		self.series = seriesIncoming
	} else {
		self.series = common.MergeSeries(self.series, seriesIncoming)
	}
	return !self.limiter.hitLimit(seriesIncoming.GetName()), nil
}

func (self *Passthrough) Close() error {
	if self.series != nil && self.series.Name != nil {
		log.Debug("Passthrough Yielding to %s: %s", self.next.Name(), self.series)
		_, err := self.next.Yield(self.series)
		if err != nil {
			return err
		}
	}
	return self.next.Close()
}

func (self *Passthrough) Name() string {
	return "PassthroughEngine"
}

func (self *Passthrough) Next() Processor {
	return self.next
}
