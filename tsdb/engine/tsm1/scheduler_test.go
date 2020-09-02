package tsm1

import "testing"

func TestScheduler_Runnable_Empty(t *testing.T) {
	s := newScheduler(&EngineStatistics{}, 1)

	for i := 1; i < 5; i++ {
		s.setDepth(i, 1)
		level, runnable := s.next()
		if exp, got := true, runnable; exp != got {
			t.Fatalf("runnable(%d) mismatch: exp %v, got %v ", i, exp, got)
		}

		if exp, got := i, level; exp != got {
			t.Fatalf("runnable(%d) mismatch: exp %v, got %v ", i, exp, got)
		}
		s.setDepth(i, 0)
	}
}

func TestScheduler_Runnable_MaxConcurrency(t *testing.T) {
	s := newScheduler(&EngineStatistics{}, 1)

	// level 1
	s.stats = &EngineStatistics{}
	s.stats.TSMCompactionsActive[0] = 1
	for i := 0; i <= 4; i++ {
		_, runnable := s.next()
		if exp, got := false, runnable; exp != got {
			t.Fatalf("runnable mismatch: exp %v, got %v ", exp, got)
		}
	}

	// level 2
	s.stats = &EngineStatistics{}
	s.stats.TSMCompactionsActive[1] = 1
	for i := 0; i <= 4; i++ {
		_, runnable := s.next()
		if exp, got := false, runnable; exp != got {
			t.Fatalf("runnable mismatch: exp %v, got %v ", exp, got)
		}
	}

	// level 3
	s.stats = &EngineStatistics{}
	s.stats.TSMCompactionsActive[2] = 1
	for i := 0; i <= 4; i++ {
		_, runnable := s.next()
		if exp, got := false, runnable; exp != got {
			t.Fatalf("runnable mismatch: exp %v, got %v ", exp, got)
		}
	}

	// optimize
	s.stats = &EngineStatistics{}
	s.stats.TSMOptimizeCompactionsActive++
	for i := 0; i <= 4; i++ {
		_, runnable := s.next()
		if exp, got := false, runnable; exp != got {
			t.Fatalf("runnable mismatch: exp %v, got %v ", exp, got)
		}
	}

	// full
	s.stats = &EngineStatistics{}
	s.stats.TSMFullCompactionsActive++
	for i := 0; i <= 4; i++ {
		_, runnable := s.next()
		if exp, got := false, runnable; exp != got {
			t.Fatalf("runnable mismatch: exp %v, got %v ", exp, got)
		}
	}
}
