package tsm1

import (
	"sync/atomic"
)

// Level 5 (optimize) is set so much lower because level 5 compactions take so much longer.
var defaultWeights = [TotalCompactionLevels]float64{0.4, 0.3, 0.2, 0.1, 0.01}

type Scheduler struct {
	maxConcurrency    int
	activeCompactions *compactionCounter

	// queues is the depth of work pending for each compaction level
	queues  [TotalCompactionLevels]int
	weights [TotalCompactionLevels]float64
}

func newScheduler(activeCompactions *compactionCounter, maxConcurrency int) *Scheduler {
	return &Scheduler{
		activeCompactions: activeCompactions,
		maxConcurrency:    maxConcurrency,
		weights:           defaultWeights,
	}
}

func (s *Scheduler) SetDepth(level, depth int) {
	level = level - 1
	if level < 0 || level > len(s.queues) {
		return
	}

	s.queues[level] = depth
}

func (s *Scheduler) nextByQueueDepths(depths [TotalCompactionLevels]int) (int, bool) {
	level1Running := int(atomic.LoadInt64(&s.activeCompactions.l1))
	level2Running := int(atomic.LoadInt64(&s.activeCompactions.l2))
	level3Running := int(atomic.LoadInt64(&s.activeCompactions.l3))
	level4Running := int(atomic.LoadInt64(&s.activeCompactions.full))
	level5Running := int(atomic.LoadInt64(&s.activeCompactions.optimize))

	if level1Running+level2Running+level3Running+level4Running+level5Running >= s.maxConcurrency {
		return 0, false
	}

	var (
		level    int
		runnable bool
	)

	loLimit, _ := s.limits()

	end := len(depths)
	if level3Running+level4Running+level5Running >= loLimit && s.maxConcurrency-(level1Running+level2Running) == 0 {
		end = 2
	}

	var weight float64
	for i := 0; i < end; i++ {
		if float64(depths[i])*s.weights[i] > weight {
			level, runnable = i+1, true
			weight = float64(depths[i]) * s.weights[i]
		}
	}
	return level, runnable
}

// SetActive sets the active compaction count for the given level (1-5).
// This is intended for testing.
func (s *Scheduler) SetActive(level int, count int64) {
	switch level {
	case 1:
		atomic.StoreInt64(&s.activeCompactions.l1, count)
	case 2:
		atomic.StoreInt64(&s.activeCompactions.l2, count)
	case 3:
		atomic.StoreInt64(&s.activeCompactions.l3, count)
	case 4:
		atomic.StoreInt64(&s.activeCompactions.full, count)
	case 5:
		atomic.StoreInt64(&s.activeCompactions.optimize, count)
	}
}

func (s *Scheduler) next() (int, bool) {
	return s.nextByQueueDepths(s.queues)
}

func (s *Scheduler) limits() (int, int) {
	hiLimit := s.maxConcurrency * 4 / 5
	loLimit := (s.maxConcurrency / 5) + 1
	if hiLimit == 0 {
		hiLimit = 1
	}

	if loLimit == 0 {
		loLimit = 1
	}

	return loLimit, hiLimit
}
