package influxdb

import (
	"sync/atomic"
	"time"
)

// PointBatcher accepts Points and will emit a batch of those points when either
// a) the batch reaches a certain size, or b) a certain time passes.
type PointBatcher struct {
	size     int
	duration time.Duration

	stats PointBatcherStats
}

// NewPointBatcher returns a new PointBatcher.
func NewPointBatcher(sz int, d time.Duration) *PointBatcher {
	return &PointBatcher{size: sz, duration: d}
}

// PointBatcherStats are the statistics each batcher tracks.
type PointBatcherStats struct {
	BatchTotal   uint64 // Total count of batches transmitted.
	PointTotal   uint64 // Total count of points processed.
	SizeTotal    uint64 // Nubmer of batches that reached size threshold.
	TimeoutTotal uint64 // Nubmer of timeouts that occurred.
}

// Start starts the batching process. Returns the in and out channels for points
// and point-batches respectively. It also returns a flush channel, to which can be
// sent an empty struct. Doing so will flush the batcher, if there is at least one
// point pending.
func (b *PointBatcher) Start() (chan<- Point, <-chan []Point, chan<- struct{}) {
	var timer *time.Timer
	var batch []Point
	var timerCh <-chan time.Time

	in := make(chan Point)
	flush := make(chan struct{})
	out := make(chan []Point)

	emit := func() {
		out <- batch
		atomic.AddUint64(&b.stats.BatchTotal, 1)
		batch = nil
		timerCh = nil
	}

	go func() {
		for {
			select {
			case p := <-in:
				atomic.AddUint64(&b.stats.PointTotal, 1)
				if batch == nil {
					batch = make([]Point, 0, b.size)
					timer = time.NewTimer(b.duration)
					timerCh = timer.C
				}

				batch = append(batch, p)
				if len(batch) >= b.size { // 0 means send immediately.
					atomic.AddUint64(&b.stats.SizeTotal, 1)
					emit()
				}

			case <-flush:
				if len(batch) > 0 {
					emit()
				}

			case <-timerCh:
				atomic.AddUint64(&b.stats.TimeoutTotal, 1)
				emit()
			}
		}
	}()

	return in, out, flush
}

// Stats returns a PointBatcherStats object for the PointBatcher. While the each statistic should be
// closely correlated with each other statistic, it is not guaranteed.
func (b *PointBatcher) Stats() *PointBatcherStats {
	stats := PointBatcherStats{}
	stats.BatchTotal = atomic.LoadUint64(&b.stats.BatchTotal)
	stats.PointTotal = atomic.LoadUint64(&b.stats.PointTotal)
	stats.SizeTotal = atomic.LoadUint64(&b.stats.SizeTotal)
	stats.TimeoutTotal = atomic.LoadUint64(&b.stats.TimeoutTotal)
	return &stats
}
