package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/task/backend/scheduler"
)

type mockExecutor struct {
	sync.Mutex
	fn  func(l *sync.Mutex, ctx context.Context, id scheduler.ID, scheduledAt time.Time)
	Err error
}

type mockSchedulable struct {
	id            scheduler.ID
	schedule      scheduler.Schedule
	offset        time.Duration
	lastScheduled time.Time
}

func (s mockSchedulable) ID() scheduler.ID {
	return s.id
}

func (s mockSchedulable) Schedule() scheduler.Schedule {
	return s.schedule
}
func (s mockSchedulable) Offset() time.Duration {
	return s.offset
}
func (s mockSchedulable) LastScheduled() time.Time {
	return s.lastScheduled
}

func (e *mockExecutor) Execute(ctx context.Context, id scheduler.ID, scheduledAt time.Time) error {
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
	fn func(ctx context.Context, id scheduler.ID, t time.Time) error
}

func (m *mockSchedulableService) UpdateLastScheduled(ctx context.Context, id scheduler.ID, t time.Time) error {

	return nil
}

func TestSchedule_Next(t *testing.T) {
	t.Run("fires properly", func(t *testing.T) {
		now := time.Now()
		c := make(chan time.Time, 100)
		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id scheduler.ID, scheduledAt time.Time) {
			select {
			case <-ctx.Done():
				t.Log("ctx done")
			case c <- scheduledAt:
			default:
				t.Errorf("called the executor too many times")
			}
		}}
		sch, _, err := scheduler.NewScheduler(
			exe,
			&mockSchedulableService{fn: func(ctx context.Context, id scheduler.ID, t time.Time) error {
				return nil
			}},
			scheduler.WithMaxConcurrentWorkers(2))
		if err != nil {
			t.Fatal(err)
		}
		defer sch.Stop()
		schedule, err := scheduler.NewSchedule("* * * * * * *")
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
			t.Fatal("test timed out", sch.Now().Unix(), sch.When().Unix())
		}
	})
	t.Run("doesn't fire when the task isn't ready", func(t *testing.T) {
		now := time.Now()
		c := make(chan time.Time, 100)
		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id scheduler.ID, scheduledAt time.Time) {
			select {
			case <-ctx.Done():
				t.Log("ctx done")
			case c <- scheduledAt:
			default:
				t.Errorf("called the executor too many times")
			}
		}}
		mockTime := scheduler.NewMockTime(now)
		sch, _, err := scheduler.NewScheduler(
			exe,
			&mockSchedulableService{fn: func(ctx context.Context, id scheduler.ID, t time.Time) error {
				return nil
			}},
			scheduler.WithTime(mockTime),
			scheduler.WithMaxConcurrentWorkers(2))
		if err != nil {
			t.Fatal(err)
		}
		defer sch.Stop()
		schedule, err := scheduler.NewSchedule("* * * * * * *")
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: now.Add(time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			mockTime.Set(mockTime.T.Add(2 * time.Second))
		}()

		select {
		case <-c:
			t.Fatal("test timed out", sch.Now().Unix(), sch.When().Unix())
		case <-time.After(2 * time.Second):
		}

	})

	t.Run("fires the correct number of times for the interval with a single schedulable", func(t *testing.T) {
		now := time.Now().UTC()
		c := make(chan time.Time, 100)
		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id scheduler.ID, scheduledAt time.Time) {
			select {
			case <-ctx.Done():
				t.Log("ctx done")
			case c <- scheduledAt:
			}
		}}
		mockTime := scheduler.NewMockTime(now)
		sch, _, err := scheduler.NewScheduler(
			exe,
			&mockSchedulableService{fn: func(ctx context.Context, id scheduler.ID, t time.Time) error {
				return nil
			}},
			scheduler.WithTime(mockTime),
			scheduler.WithMaxConcurrentWorkers(20))
		if err != nil {
			t.Fatal(err)
		}
		defer sch.Stop()
		schedule, err := scheduler.NewSchedule("* * * * * * *")
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: now})
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			mockTime.Set(mockTime.T.Add(17 * time.Second))
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
			mockTime.Set(mockTime.T.Add(2 * time.Second))
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
			id scheduler.ID
		}, 100)
		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id scheduler.ID, scheduledAt time.Time) {
			select {
			case <-ctx.Done():
				t.Log("ctx done")
			case c <- struct {
				ts time.Time
				id scheduler.ID
			}{
				ts: scheduledAt,
				id: id,
			}:
			}
		}}
		mockTime := scheduler.NewMockTime(now)
		sch, _, err := scheduler.NewScheduler(
			exe,
			&mockSchedulableService{fn: func(ctx context.Context, id scheduler.ID, t time.Time) error {
				return nil
			}},
			scheduler.WithTime(mockTime),
			scheduler.WithMaxConcurrentWorkers(20))
		if err != nil {
			t.Fatal(err)
		}
		defer sch.Stop()
		schedule, err := scheduler.NewSchedule("* * * * * * *")
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: now})
		if err != nil {
			t.Fatal(err)
		}
		schedule2, err := scheduler.NewSchedule("*/2 * * * * * *")
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 2, schedule: schedule2, offset: time.Second, lastScheduled: now})
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			mockTime.Set(mockTime.T.Add(17 * time.Second))
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
			mockTime.Set(mockTime.T.Add(2 * time.Second))
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
	mockTime := scheduler.NewMockTime(now)
	exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id scheduler.ID, scheduledAt time.Time) {}}
	sch, _, err := scheduler.NewScheduler(exe, &mockSchedulableService{fn: func(ctx context.Context, id scheduler.ID, t time.Time) error {
		return nil
	}},
		scheduler.WithTime(mockTime))
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

	exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id scheduler.ID, scheduledAt time.Time) {
		panic("yikes oh no!")
	}}

	sch, _, err := scheduler.NewScheduler(
		exe,
		&mockSchedulableService{fn: func(ctx context.Context, id scheduler.ID, t time.Time) error {
			return nil
		}},
		scheduler.WithMaxConcurrentWorkers(1), // to make debugging easier
		scheduler.WithOnErrorFn(func(_ context.Context, _ scheduler.ID, ts time.Time, err error) {
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

	schedule, err := scheduler.NewSchedule("* * * * * * *")
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
		t.Fatal("test timed out", now.UTC().Unix(), sch.Now().Unix(), sch.When().Unix())
	}
}
