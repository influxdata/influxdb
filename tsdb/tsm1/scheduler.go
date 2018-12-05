package tsm1

var defaultWeights = [4]float64{0.4, 0.3, 0.2, 0.1}

type scheduler struct {
	maxConcurrency    int
	compactionTracker *compactionTracker

	// queues is the depth of work pending for each compaction level
	queues  [4]int
	weights [4]float64
}

func newScheduler(maxConcurrency int) *scheduler {
	return &scheduler{
		maxConcurrency:    maxConcurrency,
		weights:           defaultWeights,
		compactionTracker: newCompactionTracker(newCompactionMetrics(nil), nil),
	}
}

// setCompactionTracker sets the metrics on the scheduler. It must be called before next.
func (s *scheduler) setCompactionTracker(tracker *compactionTracker) {
	s.compactionTracker = tracker
}

func (s *scheduler) setDepth(level, depth int) {
	level = level - 1
	if level < 0 || level > len(s.queues) {
		return
	}

	s.queues[level] = depth
}

func (s *scheduler) next() (int, bool) {
	level1Running := int(s.compactionTracker.Active(1))
	level2Running := int(s.compactionTracker.Active(2))
	level3Running := int(s.compactionTracker.Active(3))
	level4Running := int(s.compactionTracker.ActiveFull() + s.compactionTracker.ActiveOptimise())

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
