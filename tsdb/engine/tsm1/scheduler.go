package tsm1

import (
	"sync/atomic"

	"go.uber.org/zap"
)

// Level 5 (optimize) is set so much lower because level 5 compactions take so much longer.
var defaultWeights = [TotalCompactionLevels]float64{0.4, 0.3, 0.2, 0.1, 0.01}

type Scheduler struct {
	maxConcurrency int
	stats          *EngineStatistics

	// queues is the depth of work pending for each compaction level
	queues      [TotalCompactionLevels]int
	weights     [TotalCompactionLevels]float64
	traceLogger *zap.Logger
}

func newScheduler(stats *EngineStatistics, maxConcurrency int) *Scheduler {
	return &Scheduler{
		stats:          stats,
		maxConcurrency: maxConcurrency,
		weights:        defaultWeights,
		traceLogger:    zap.NewNop(),
	}
}

func (s *Scheduler) WithLogger(logger *zap.Logger) *Scheduler {
	if logger == nil {
		logger = zap.NewNop()
	}
	s.traceLogger = logger.With(zap.String("service", "compaction_scheduler"))
	return s
}

func (s *Scheduler) SetDepth(level, depth int) {
	level = level - 1
	if level < 0 || level > len(s.queues) {
		return
	}

	s.queues[level] = depth
}

func (s *Scheduler) nextByQueueDepths(depths [TotalCompactionLevels]int) (int, bool) {
	level1Running := int(atomic.LoadInt64(&s.stats.TSMCompactionsActive[0]))
	level2Running := int(atomic.LoadInt64(&s.stats.TSMCompactionsActive[1]))
	level3Running := int(atomic.LoadInt64(&s.stats.TSMCompactionsActive[2]))
	level4Running := int(atomic.LoadInt64(&s.stats.TSMFullCompactionsActive))
	level5Running := int(atomic.LoadInt64(&s.stats.TSMOptimizeCompactionsActive))

	if level1Running+level2Running+level3Running+level4Running+level5Running > 0 {
		s.traceLogger.Debug("running compactions",
			zap.Int("level1", level1Running),
			zap.Int("level2", level2Running),
			zap.Int("level3", level3Running),
			zap.Int("level4", level4Running),
			zap.Int("level5", level5Running))
	}
	if level1Running+level2Running+level3Running+level4Running+level5Running >= s.maxConcurrency {
		s.traceLogger.Debug("max compaction concurrency reached", zap.Int("maxConcurrency", s.maxConcurrency))
		return 0, false
	}

	var (
		level    int
		runnable bool
	)

	loLimit, _ := s.limits()

	end := len(depths)
	if level3Running+level4Running+level5Running >= loLimit && s.maxConcurrency-(level1Running+level2Running) == 0 {
		s.traceLogger.Debug("levels 3 through 5 compactions greater than low limit and levels 1 and 2 equal max concurrency, restricting to levels 1 and 2", zap.Int("loLimit", loLimit))
		end = 2
	}

	var weight float64
	for i := 0; i < end; i++ {
		if float64(depths[i])*s.weights[i] > weight {
			s.traceLogger.Debug("evaluating compaction level", zap.Int("level", i+1), zap.Int("depth", depths[i]), zap.Float64("weight", float64(depths[i])*s.weights[i]))
			level, runnable = i+1, true
			weight = float64(depths[i]) * s.weights[i]
		}
	}
	if runnable {
		s.traceLogger.Debug("selected runnable compaction level", zap.Int("level", level))
	}
	return level, runnable
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
