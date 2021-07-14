package scheduler

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/cron"

	"github.com/benbjohnson/clock"
)

type mockExecutor struct {
	sync.Mutex
	fn  func(l *sync.Mutex, ctx context.Context, id ID, scheduledFor time.Time)
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

func (e *mockExecutor) Execute(ctx context.Context, id ID, scheduledFor time.Time, runAt time.Time) error {
	done := make(chan struct{}, 1)
	select {
	case <-ctx.Done():
	default:
		e.fn(&sync.Mutex{}, ctx, id, scheduledFor)
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
	t.Run("@every fires on appropriate boundaries", func(t *testing.T) {
		// For these tests, the "timeElapsed" is the amount of time that is
		// simulated to pass for the purposes of verifying that the task fires the
		// correct amount of times. It is multiplied by a factor within the tests to
		// simulated firing multiple times.
		tests := []struct {
			name        string // also used as the cron time string
			timeElapsed time.Duration
		}{
			{
				name:        "@every 1m",
				timeElapsed: 1 * time.Minute,
			},
			{
				name:        "@every 1h",
				timeElapsed: 1 * time.Hour,
			},
			{
				name:        "@every 1w",        // regression test for https://github.com/influxdata/influxdb/issues/21842
				timeElapsed: 7 * 24 * time.Hour, // 1 week
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
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
				schedule, ts, err := NewSchedule(tt.name, mockTime.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}

				err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: ts})
				if err != nil {
					t.Fatal(err)
				}
				go func() {
					sch.mu.Lock()
					mockTime.Set(mockTime.Now().UTC().Add(17 * tt.timeElapsed))
					sch.mu.Unlock()
				}()

				after := time.After(6 * time.Second)
				oldCheckC := ts
				for i := 0; i < 16; i++ {
					select {
					case checkC := <-c:
						if checkC.Sub(oldCheckC) != tt.timeElapsed {
							t.Fatalf("task didn't fire on correct interval fired on %s interval", checkC.Sub(oldCheckC))
						}
						if !checkC.Truncate(tt.timeElapsed).Equal(checkC) {
							t.Fatalf("task didn't fire at the correct time boundary")
						}
						oldCheckC = checkC
					case <-after:
						t.Fatalf("test timed out, only fired %d times but should have fired 16 times", i)
					}
				}
			})
		}
	})
	t.Run("fires properly with non-mocked time", func(t *testing.T) {
		now := time.Now()
		c := make(chan time.Time, 100)
		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledFor time.Time) {
			select {
			case <-ctx.Done():
				t.Log("ctx done")
			case c <- scheduledFor:
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
		schedule, _, err := NewSchedule("* * * * * * *", time.Time{})
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
		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledFor time.Time) {
			select {
			case <-ctx.Done():
				t.Log("ctx done")
			case c <- scheduledFor:
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
		schedule, _, err := NewSchedule("* * * * * * *", time.Time{})
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
		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledFor time.Time) {
			select {
			case <-ctx.Done():
				t.Log("ctx done")
			case c <- scheduledFor:
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
		schedule, _, err := NewSchedule("* * * * * * *", time.Time{})
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
		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledFor time.Time) {
			select {
			case <-ctx.Done():
				t.Log("ctx done")
			case c <- struct {
				ts time.Time
				id ID
			}{
				ts: scheduledFor,
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
		schedule, _, err := NewSchedule("* * * * * * *", time.Time{})
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: now})
		if err != nil {
			t.Fatal(err)
		}

		schedule2, _, err := NewSchedule("*/2 * * * * * *", time.Time{})
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
	exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledFor time.Time) {}}
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

	exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledFor time.Time) {
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

	schedule, _, err := NewSchedule("* * * * * * *", time.Time{})
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

	exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledFor time.Time) {
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
	schedule, ts, err := NewSchedule("* * * * * * *", now.Add(-1*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	badSchedule, ts, err := NewSchedule("0 0 1 12 *", now.Add(-1*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	for i := ID(1); i <= 2000; i++ { // since a valid ID probably shouldn't be zero
		if i%100 == 0 {
			err = sch.Schedule(mockSchedulable{id: i, schedule: badSchedule, offset: 0, lastScheduled: ts})
			if err != nil {
				t.Fatal(err)
			}
		} else {
			err = sch.Schedule(mockSchedulable{id: i, schedule: schedule, offset: 0, lastScheduled: ts})
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
	exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledFor time.Time) {
		select {
		case <-ctx.Done():
			t.Log("ctx done")
		case c <- scheduledFor:
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
	schedule, ts, err := NewSchedule("* * * * * * *", mockTime.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}

	err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: ts.UTC()})
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
	if err := sch.Release(1); err != nil {
		t.Error(err)
	}

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

func mustCron(s string) Schedule {
	cr, err := cron.ParseUTC(s)
	if err != nil {
		panic(err)
	}
	return Schedule{cron: cr}
}

func TestNewSchedule(t *testing.T) {
	tests := []struct {
		name            string
		unparsed        string
		lastScheduledAt time.Time
		want            Schedule
		want1           time.Time
		wantErr         bool
	}{
		{
			name:            "bad cron",
			unparsed:        "this is not a cron string",
			lastScheduledAt: time.Now(),
			wantErr:         true,
		},
		{
			name:            "align to minute",
			unparsed:        "@every 1m",
			lastScheduledAt: time.Date(2016, 01, 01, 01, 10, 23, 1234567, time.UTC),
			want:            mustCron("@every 1m"),
			want1:           time.Date(2016, 01, 01, 01, 10, 0, 0, time.UTC),
		},
		{
			name:            "align to minute with @every 7m",
			unparsed:        "@every 7m",
			lastScheduledAt: time.Date(2016, 01, 01, 01, 10, 23, 1234567, time.UTC),
			want:            mustCron("@every 7m"),
			want1:           time.Date(2016, 01, 01, 01, 4, 0, 0, time.UTC),
		},

		{
			name:            "align to hour",
			unparsed:        "@every 1h",
			lastScheduledAt: time.Date(2016, 01, 01, 01, 10, 23, 1234567, time.UTC),
			want:            mustCron("@every 1h"),
			want1:           time.Date(2016, 01, 01, 01, 0, 0, 0, time.UTC),
		},
		{
			name:            "align to hour @every 3h",
			unparsed:        "@every 3h",
			lastScheduledAt: time.Date(2016, 01, 01, 01, 10, 23, 1234567, time.UTC),
			want:            mustCron("@every 3h"),
			want1:           time.Date(2016, 01, 01, 00, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := NewSchedule(tt.unparsed, tt.lastScheduledAt)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSchedule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSchedule() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("NewSchedule() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
