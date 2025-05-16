package tsm1

import (
	"sync/atomic"
)

// Level 5 (optimize) is set so much lower because level 5 compactions take so much longer.
var defaultWeights = [TotalCompactionLevels]float64{0.4, 0.3, 0.2, 0.1, 0.01}

type scheduler struct {
	maxConcurrency int
	stats          *EngineStatistics

	// queues is the depth of work pending for each compaction level
	queues  [TotalCompactionLevels]int
	weights [TotalCompactionLevels]float64
}

func newScheduler(stats *EngineStatistics, maxConcurrency int) *scheduler {
	return &scheduler{
		stats:          stats,
		maxConcurrency: maxConcurrency,
		weights:        defaultWeights,
	}
}

func (s *scheduler) setDepth(level, depth int) {
	level = level - 1
	if level < 0 || level > len(s.queues) {
		return
	}

	s.queues[level] = depth
}

func (s *scheduler) nextByQueueDepths(depths [TotalCompactionLevels]int) (int, bool) {
	level1Running := int(atomic.LoadInt64(&s.stats.TSMCompactionsActive[0]))
	level2Running := int(atomic.LoadInt64(&s.stats.TSMCompactionsActive[1]))
	level3Running := int(atomic.LoadInt64(&s.stats.TSMCompactionsActive[2]))
	level4Running := int(atomic.LoadInt64(&s.stats.TSMFullCompactionsActive))
	level5Running := int(atomic.LoadInt64(&s.stats.TSMOptimizeCompactionsActive))

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

func (s *scheduler) next() (int, bool) {
	return s.nextByQueueDepths(s.queues)
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
