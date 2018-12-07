package tsm1

import "testing"

func TestScheduler_Runnable_Empty(t *testing.T) {
	s := newScheduler(1)

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
	s := newScheduler(1)

	// level 1
	s.compactionTracker.active[1] = 1
	for i := 0; i <= 4; i++ {
		_, runnable := s.next()
		if exp, got := false, runnable; exp != got {
			t.Fatalf("runnable mismatch: exp %v, got %v ", exp, got)
		}
	}

	// level 2
	s.compactionTracker.active[2] = 1
	for i := 0; i <= 4; i++ {
		_, runnable := s.next()
		if exp, got := false, runnable; exp != got {
			t.Fatalf("runnable mismatch: exp %v, got %v ", exp, got)
		}
	}

	// level 3
	s.compactionTracker.active[3] = 1
	for i := 0; i <= 4; i++ {
		_, runnable := s.next()
		if exp, got := false, runnable; exp != got {
			t.Fatalf("runnable mismatch: exp %v, got %v ", exp, got)
		}
	}

	// optimize
	s.compactionTracker.active[4] = 1
	for i := 0; i <= 4; i++ {
		_, runnable := s.next()
		if exp, got := false, runnable; exp != got {
			t.Fatalf("runnable mismatch: exp %v, got %v ", exp, got)
		}
	}

	// full
	s.compactionTracker.active[5] = 1
	for i := 0; i <= 4; i++ {
		_, runnable := s.next()
		if exp, got := false, runnable; exp != got {
			t.Fatalf("runnable mismatch: exp %v, got %v ", exp, got)
		}
	}
}
