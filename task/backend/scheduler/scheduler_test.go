package scheduler // can

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
)

type mockExecutor struct {
	sync.Mutex
	fn  func(l *sync.Mutex, ctx context.Context, id ID, scheduledAt time.Time)
	Err error
}

type mockSchedulable struct {
	id            ID
	schedule      Schedule
	offset        time.Duration
	lastScheduled time.Time
}

func (s mockSchedulable) ID() ID {
	return s.id
}

func (s mockSchedulable) Schedule() Schedule {
	return s.schedule
}
func (s mockSchedulable) Offset() time.Duration {
	return s.offset
}
func (s mockSchedulable) LastScheduled() time.Time {
	return s.lastScheduled
}

func (e *mockExecutor) Execute(ctx context.Context, id ID, scheduledAt time.Time) error {
	done := make(chan struct{}, 1)
	select {
	case <-ctx.Done():
	default:
		e.fn(&sync.Mutex{}, ctx, id, scheduledAt)
		done <- struct{}{}
	}
	return nil
}

type mockSchedulableService struct {
	fn func(ctx context.Context, id ID, t time.Time) error
}

func (m *mockSchedulableService) UpdateLastScheduled(ctx context.Context, id ID, t time.Time) error {

	return nil
}

func TestSchedule_Next(t *testing.T) {
	t.Run("fires properly with non-mocked time", func(t *testing.T) {
		now := time.Now()
		c := make(chan time.Time, 100)
		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledAt time.Time) {
			select {
			case <-ctx.Done():
				t.Log("ctx done")
			case c <- scheduledAt:
			default:
				t.Errorf("called the executor too many times")
			}
		}}
		sch, _, err := NewScheduler(
			exe,
			&mockSchedulableService{fn: func(ctx context.Context, id ID, t time.Time) error {
				return nil
			}},
			WithMaxConcurrentWorkers(2))
		if err != nil {
			t.Fatal(err)
		}
		defer sch.Stop()
		schedule, err := NewSchedule("* * * * * * *")
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: now.Add(-20 * time.Second)})
		if err != nil {
			t.Fatal(err)
		}

		select {
		case <-c:
		case <-time.After(10 * time.Second):
			t.Fatal("test timed out")
		}
	})
	t.Run("doesn't fire when the task isn't ready", func(t *testing.T) {
		mockTime := clock.NewMock()
		mockTime.Set(time.Now())
		c := make(chan time.Time, 100)
		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledAt time.Time) {
			select {
			case <-ctx.Done():
				t.Log("ctx done")
			case c <- scheduledAt:
			default:
				t.Errorf("called the executor too many times")
			}
		}}
		sch, _, err := NewScheduler(
			exe,
			&mockSchedulableService{fn: func(ctx context.Context, id ID, t time.Time) error {
				return nil
			}},
			WithTime(mockTime),
			WithMaxConcurrentWorkers(2))
		if err != nil {
			t.Fatal(err)
		}
		defer sch.Stop()
		schedule, err := NewSchedule("* * * * * * *")
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: mockTime.Now().UTC().Add(time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			sch.mu.Lock()
			mockTime.Set(mockTime.Now().Add(2 * time.Second))
			sch.mu.Unlock()
		}()

		select {
		case <-c:
			t.Fatal("test timed out")
		case <-time.After(2 * time.Second):
		}

	})

	t.Run("fires the correct number of times for the interval with a single schedulable", func(t *testing.T) {
		c := make(chan time.Time, 100)
		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledAt time.Time) {
			select {
			case <-ctx.Done():
				t.Log("ctx done")
			case c <- scheduledAt:
			}
		}}
		mockTime := clock.NewMock()
		mockTime.Set(time.Now())
		sch, _, err := NewScheduler(
			exe,
			&mockSchedulableService{fn: func(ctx context.Context, id ID, t time.Time) error {
				return nil
			}},
			WithTime(mockTime),
			WithMaxConcurrentWorkers(20))
		if err != nil {
			t.Fatal(err)
		}
		defer sch.Stop()
		schedule, err := NewSchedule("* * * * * * *")
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: mockTime.Now().UTC()})
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			sch.mu.Lock()
			mockTime.Set(mockTime.Now().UTC().Add(17 * time.Second))
			sch.mu.Unlock()
		}()

		after := time.After(6 * time.Second)
		for i := 0; i < 16; i++ {
			select {
			case <-c:
			case <-after:
				t.Fatalf("test timed out, only fired %d times but should have fired 16 times", i)
			}
		}
		go func() {
			sch.mu.Lock()
			mockTime.Set(mockTime.Now().UTC().Add(2 * time.Second))
			sch.mu.Unlock()
		}()

		after = time.After(6 * time.Second)

		for i := 0; i < 2; i++ {
			select {
			case <-c:
			case <-after:
				t.Fatalf("test timed out, only fired %d times but should have fired 2 times", i)
			}
		}

		select {
		case <-c:
			t.Fatalf("test scheduler fired too many times")
		case <-time.After(2 * time.Second):
		}
	})

	t.Run("fires the correct number of times for the interval with multiple schedulables", func(t *testing.T) {
		now := time.Date(2016, 0, 0, 0, 1, 1, 0, time.UTC)
		c := make(chan struct {
			ts time.Time
			id ID
		}, 100)
		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledAt time.Time) {
			select {
			case <-ctx.Done():
				t.Log("ctx done")
			case c <- struct {
				ts time.Time
				id ID
			}{
				ts: scheduledAt,
				id: id,
			}:
			}
		}}
		mockTime := clock.NewMock()
		mockTime.Set(now)
		sch, _, err := NewScheduler(
			exe,
			&mockSchedulableService{fn: func(ctx context.Context, id ID, t time.Time) error {
				return nil
			}},
			WithTime(mockTime),
			WithMaxConcurrentWorkers(20))
		if err != nil {
			t.Fatal(err)
		}
		defer sch.Stop()
		schedule, err := NewSchedule("* * * * * * *")
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: now})
		if err != nil {
			t.Fatal(err)
		}
		schedule2, err := NewSchedule("*/2 * * * * * *")
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 2, schedule: schedule2, offset: time.Second, lastScheduled: now})
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			sch.mu.Lock()
			mockTime.Set(mockTime.Now().Add(17 * time.Second))
			sch.mu.Unlock()
		}()

		after := time.After(6 * time.Second)
		for i := 0; i < 24; i++ {
			select {
			case <-c:
			case <-after:
				t.Fatalf("test timed out, only fired %d times but should have fired 24 times", i)
			}
		}

		go func() {
			sch.mu.Lock()
			mockTime.Set(mockTime.Now().Add(2 * time.Second))
			sch.mu.Unlock()
		}()

		after = time.After(6 * time.Second)

		for i := 0; i < 3; i++ {
			select {
			case <-c:
			case <-after:
				t.Fatalf("test timed out, only fired %d times but should have fired 3 times", i)
			}
		}

		select {
		case <-c:
			t.Fatalf("test scheduler fired too many times")
		case <-time.After(2 * time.Second):
		}
	})
}

func TestTreeScheduler_Stop(t *testing.T) {
	now := time.Now().Add(-20 * time.Second)
	mockTime := clock.NewMock()
	mockTime.Set(now)
	exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledAt time.Time) {}}
	sch, _, err := NewScheduler(exe, &mockSchedulableService{fn: func(ctx context.Context, id ID, t time.Time) error {
		return nil
	}},
		WithTime(mockTime))
	if err != nil {
		t.Fatal(err)
	}
	sch.Stop()
}

func TestSchedule_panic(t *testing.T) {
	// panics in the executor should be treated as errors
	now := time.Now().UTC()
	c := make(chan struct {
		ts  time.Time
		err error
	}, 1)

	exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledAt time.Time) {
		panic("yikes oh no!")
	}}

	sch, _, err := NewScheduler(
		exe,
		&mockSchedulableService{fn: func(ctx context.Context, id ID, t time.Time) error {
			return nil
		}},
		WithMaxConcurrentWorkers(1), // to make debugging easier
		WithOnErrorFn(func(_ context.Context, _ ID, ts time.Time, err error) {
			c <- struct {
				ts  time.Time
				err error
			}{
				ts:  ts,
				err: err,
			}
		}))
	if err != nil {
		t.Fatal(err)
	}

	schedule, err := NewSchedule("* * * * * * *")
	if err != nil {
		t.Fatal(err)
	}

	err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: now.Add(-20 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-c: // panic was caught and error handler used
	case <-time.After(10 * time.Second):
		t.Fatal("test timed out", now.UTC().Unix())
	}
}

func TestTreeScheduler_LongPanicTest(t *testing.T) {
	// This test is to catch one specifgic type of race condition that can occur and isn't caught by race test, but causes a panic
	// in the google btree library
	now := time.Date(2096, time.December, 30, 0, 0, 0, 0, time.UTC)

	mockTime := clock.NewMock()
	mockTime.Set(now)

	exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledAt time.Time) {
		select {
		case <-ctx.Done():
			t.Log("ctx done")
		default:
		}
	}}

	sch, _, err := NewScheduler(
		exe,
		&mockSchedulableService{fn: func(ctx context.Context, id ID, t time.Time) error {
			return nil
		}},
		WithTime(mockTime),
		WithMaxConcurrentWorkers(20))
	if err != nil {
		t.Fatal(err)
	}
	defer sch.Stop()

	// this tests for a race condition in the btree that isn't normally caught by the race detector
	schedule, err := NewSchedule("* * * * * * *")
	if err != nil {
		t.Fatal(err)
	}
	badSchedule, err := NewSchedule("0 0 1 12 *")
	if err != nil {
		t.Fatal(err)
	}

	for i := ID(1); i <= 2000; i++ { // since a valid ID probably shouldn't be zero
		if i%100 == 0 {
			err = sch.Schedule(mockSchedulable{id: i, schedule: badSchedule, offset: 0, lastScheduled: now.Add(-1 * time.Second)})
			if err != nil {
				t.Fatal(err)
			}
		} else {
			err = sch.Schedule(mockSchedulable{id: i, schedule: schedule, offset: 0, lastScheduled: now.Add(-1 * time.Second)})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	time.Sleep(2 * time.Second)
	sch.mu.Lock()
	mockTime.Set(mockTime.Now().UTC().Add(99 * time.Second))
	sch.mu.Unlock()
	time.Sleep(5 * time.Second)

}

func TestTreeScheduler_Release(t *testing.T) {
	c := make(chan time.Time, 100)
	exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledAt time.Time) {
		select {
		case <-ctx.Done():
			t.Log("ctx done")
		case c <- scheduledAt:
		}
	}}
	mockTime := clock.NewMock()
	mockTime.Set(time.Now())
	sch, _, err := NewScheduler(
		exe,
		&mockSchedulableService{fn: func(ctx context.Context, id ID, t time.Time) error {
			return nil
		}},
		WithTime(mockTime),
		WithMaxConcurrentWorkers(20))
	if err != nil {
		t.Fatal(err)
	}
	defer sch.Stop()
	schedule, err := NewSchedule("* * * * * * *")
	if err != nil {
		t.Fatal(err)
	}

	err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: mockTime.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		sch.mu.Lock()
		mockTime.Set(mockTime.Now().UTC().Add(2 * time.Second))
		sch.mu.Unlock()
	}()

	select {
	case <-c:
	case <-time.After(6 * time.Second):
		t.Fatalf("test timed out, it should have fired but didn't")
	}
	sch.Release(1)

	go func() {
		sch.mu.Lock()
		mockTime.Set(mockTime.Now().UTC().Add(6 * time.Second))
		sch.mu.Unlock()
	}()

	select {
	case <-c:
		t.Fatal("expected test not to fire here, because task was released, but it did anyway")
	case <-time.After(2 * time.Second):
	}
}
