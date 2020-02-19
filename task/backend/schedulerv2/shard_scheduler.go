package schedulerv2

import (
	"context"
	"encoding/binary"
	"sync"

	"github.com/cespare/xxhash"
)

type ShardScheduler struct {
	schedulers []Scheduler
	wg         sync.WaitGroup
	sm         *SchedulerMetrics
}

// NewShardedScheduler creates a new ShardScheduler. To ensure even
// distribution, a power of two should be selected for the shard count.
func NewShardedTreeScheduler(count int, executor Executor, checkpointer SchedulableService, opts ...treeSchedulerOptFunc) (*ShardScheduler, *SchedulerMetrics, error) {
	metrics := NewSchedulerMetrics()

	s := &ShardScheduler{
		schedulers: make([]Scheduler, count),
		sm:         metrics,
	}

	opts = append(append([]treeSchedulerOptFunc(nil), WithScheduleMetrics(metrics)), opts...)

	for i := range s.schedulers {
		scheduler, _, err := NewTreeScheduler(executor, checkpointer, opts...)
		if err != nil {
			return nil, nil, err
		}
		s.schedulers[i] = scheduler
	}

	return s, metrics, nil
}

func (s *ShardScheduler) Schedule(task Schedulable) error {
	return s.schedulers[s.hash(task.ID())].Schedule(task)
}

func (s *ShardScheduler) Release(taskID ID) error {
	return s.schedulers[s.hash(taskID)].Release(taskID)
}

func (s *ShardScheduler) hash(taskID ID) uint64 {
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], uint64(taskID))
	return xxhash.Sum64(buf[:]) % uint64(len(s.schedulers)) // we just hash so that the number is uniformly distributed
}

func (s *ShardScheduler) Process(ctx context.Context) {
	s.wg.Add(len(s.schedulers))
	for _, shard := range s.schedulers {
		go func(shard Scheduler) {
			shard.Process(ctx)
			s.wg.Done()
		}(shard)
	}
	s.wg.Wait()
}

func (s *ShardScheduler) State() SchedulerState {
	states := [6]int{}
	for _, s := range s.schedulers {
		states[s.State()]++
	}

	// Rules for calculating state. Ordering matters.
	switch {
	case states[SchedulerStateStopped] == len(s.schedulers):
		return SchedulerStateStopped
	case states[SchedulerStateStopping] > 0 || states[SchedulerStateStopped] > 0:
		return SchedulerStateStopping
	case states[SchedulerStateProcessing] > 0:
		return SchedulerStateProcessing
	case states[SchedulerStateWaiting] == len(s.schedulers):
		return SchedulerStateWaiting
	}

	return SchedulerStateReady
}
