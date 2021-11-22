package tsm1

import (
	"sync/atomic"
)

var defaultWeights = [4]float64{0.4, 0.3, 0.2, 0.1}

type scheduler struct {
	maxConcurrency    int
	activeCompactions *compactionCounter

	// queues is the depth of work pending for each compaction level
	queues  [4]int
	weights [4]float64
}

func newScheduler(activeCompactions *compactionCounter, maxConcurrency int) *scheduler {
	return &scheduler{
		activeCompactions: activeCompactions,
		maxConcurrency:    maxConcurrency,
		weights:           defaultWeights,
	}
}

func (s *scheduler) setDepth(level, depth int) {
	level = level - 1
	if level < 0 || level > len(s.queues) {
		return
	}

	s.queues[level] = depth
}

func (s *scheduler) next() (int, bool) {
	level1Running := int(atomic.LoadInt64(&s.activeCompactions.l1))
	level2Running := int(atomic.LoadInt64(&s.activeCompactions.l2))
	level3Running := int(atomic.LoadInt64(&s.activeCompactions.l3))
	level4Running := int(atomic.LoadInt64(&s.activeCompactions.full) + atomic.LoadInt64(&s.activeCompactions.optimize))

	if level1Running+level2Running+level3Running+level4Running >= s.maxConcurrency {
		return 0, false
	}

	var (
		level    int
		runnable bool
	)

	loLimit, _ := s.limits()

	end := len(s.queues)
	if level3Running+level4Running >= loLimit && s.maxConcurrency-(level1Running+level2Running) == 0 {
		end = 2
	}

	var weight float64
	for i := 0; i < end; i++ {
		if float64(s.queues[i])*s.weights[i] > weight {
			level, runnable = i+1, true
			weight = float64(s.queues[i]) * s.weights[i]
		}
	}
	return level, runnable
}

func (s *scheduler) limits() (int, int) {
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
