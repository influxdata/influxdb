package schedulerv2

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/influxdata/cron"
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
}

func (m *mockSchedulableService) UpdateLastScheduled(ctx context.Context, id ID, t time.Time) error {
	return nil
}

func TestSchedule(t *testing.T) {
	cases := []struct {
		name string
		fn   func(exe Executor, checkpointer SchedulableService, opts ...treeSchedulerOptFunc) (Scheduler, error)
	}{{
		name: "TreeScheduler",
		fn: func(exe Executor, checkpointer SchedulableService, opts ...treeSchedulerOptFunc) (Scheduler, error) {
			sch, _, err := NewTreeScheduler(exe, checkpointer, opts...)
			return sch, err
		},
	}, {
		name: "ShardedTreeScheduler",
		fn: func(exe Executor, checkpointer SchedulableService, opts ...treeSchedulerOptFunc) (Scheduler, error) {
			sch, _, err := NewShardedTreeScheduler(16, exe, checkpointer, opts...)
			return sch, err
		},
	},
	}
	for i := range cases {
		t.Run(cases[i].name, func(t *testing.T) {
			testSchedule(
				t,
				func(exe *mockExecutor, opts ...treeSchedulerOptFunc) (Scheduler, error) {
					return cases[i].fn(
						exe,
						&mockSchedulableService{},
						opts...,
					)
				},
			)
		})
	}
}

func testSchedule(t *testing.T, newSch func(exe *mockExecutor, opts ...treeSchedulerOptFunc) (Scheduler, error)) {
	t.Helper()
	t.Run("@every fires on appropriate boundaries", func(t *testing.T) {
		cases := []struct {
			cron      string
			boundary  time.Duration
			skipAhead time.Duration
		}{
			{
				cron:      "@every 1m",
				boundary:  time.Minute,
				skipAhead: 17 * time.Minute,
			}, {
				cron:      "@every 1h",
				boundary:  time.Hour,
				skipAhead: 17 * time.Hour,
			},
		}

		for _, testCase := range cases {
			t.Run(testCase.cron, func(t *testing.T) {
				mockTime := clock.NewMock()
				mockTime.Set(time.Now())

				c := make(chan time.Time, 100)
				exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledAt time.Time) {
					select {
					case <-ctx.Done():
						t.Log("ctx done")
					case c <- scheduledAt:
					}
				}}
				sch, err := newSch(exe, WithTime(mockTime))
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				wg := sync.WaitGroup{}
				wg.Add(1)
				go func() {
					sch.Process(ctx)
					wg.Done()
				}()
				defer func() {
					cancel()
					wg.Wait()
				}()

				schedule, ts, err := NewSchedule(testCase.cron, mockTime.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}

				err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: ts})
				if err != nil {
					t.Fatal(err)
				}

				mockTime.Set(mockTime.Now().UTC().Add(testCase.skipAhead))

				after := time.After(6 * time.Second)
				oldCheckC := ts
				for i := 0; i < 16; i++ {
					select {
					case checkC := <-c:
						if checkC.Sub(oldCheckC) != testCase.boundary {
							t.Fatalf("task didn't fire on correct interval fired on %s interval, expected on %s, with cron %s %s %s", checkC.Sub(oldCheckC), testCase.boundary, testCase.cron, checkC, oldCheckC)
						}
						if !checkC.Truncate(testCase.boundary).Equal(checkC) {
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
		theClock := clock.New()
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
		schedule, ts, err := NewSchedule("* * * * * * *", theClock.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}

		sch, err := newSch(exe, WithTime(theClock))
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: ts})
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			sch.Process(ctx)
			wg.Done()
		}()
		defer func() {
			cancel()
			wg.Wait()
		}()

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
		sch, err := newSch(exe, WithTime(mockTime))
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			sch.Process(ctx)
			wg.Done()
		}()
		defer func() {
			cancel()
			wg.Wait()
		}()
		schedule, _, err := NewSchedule("* * * * * * *", time.Time{})
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: mockTime.Now().UTC().Add(time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		mockTime.Set(mockTime.Now().Add(2 * time.Second))

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
		mockTime.Set(time.Now().UTC())
		sch, err := newSch(exe, WithTime(mockTime))
		if err != nil {
			t.Fatal(err)
		}
		schedule, _, err := NewSchedule("* * * * * * *", time.Time{})
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: mockTime.Now().UTC()})
		if err != nil {
			t.Fatal(err)
		}
		mockTime.Set(mockTime.Now().UTC().Add(17 * time.Second))

		ctx, cancel := context.WithCancel(context.Background())
		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			sch.Process(ctx)
			wg.Done()
		}()
		defer func() {
			cancel()
			wg.Wait()
		}()

		after := time.After(6 * time.Second)
		for i := 0; i < 16; i++ {
			select {
			case <-c:
			case <-after:
				t.Fatalf("test timed out, only fired %d times but should have fired 16 times", i)
			}
		}
		mockTime.Set(mockTime.Now().UTC().Add(2 * time.Second))

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
		sch, err := newSch(exe, WithTime(mockTime))
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			sch.Process(ctx)
			wg.Done()
		}()
		defer func() {
			cancel()
			wg.Wait()
		}()

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
		mockTime.Set(mockTime.Now().Add(17 * time.Second))

		after := time.After(6 * time.Second)
		for i := 0; i < 24; i++ {
			select {
			case <-c:
			case <-after:
				t.Fatalf("test timed out, only fired %d times but should have fired 24 times", i)
			}
		}

		mockTime.Set(mockTime.Now().Add(2 * time.Second))

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
	t.Run("stops properly when processing context is cancled", func(t *testing.T) {
		now := time.Now().Add(-20 * time.Second)
		mockTime := clock.NewMock()
		mockTime.Set(now)
		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledFor time.Time) {}}
		ctx, cancel := context.WithCancel(context.Background())
		sch, err := newSch(exe, WithTime(mockTime))
		if err != nil {
			t.Fatal(err)
		}
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			sch.Process(ctx)
			wg.Done()
		}()
		cancel()
	})

	t.Run("panics in the executor are treated as errors", func(t *testing.T) {
		// panics in the executor should be treated as errors
		mockTime := clock.NewMock()
		mockTime.Set(time.Now())
		c := make(chan struct {
			ts  time.Time
			err error
		}, 1000)

		exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledFor time.Time) {
			panic("yikes oh no!")
		}}

		sch, err := newSch(exe, WithTime(mockTime), WithOnErrorFn(
			func(ctx context.Context, taskID ID, scheduledFor time.Time, err error) {
				c <- struct {
					ts  time.Time
					err error
				}{
					ts:  scheduledFor,
					err: err,
				}
			}))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		wg := sync.WaitGroup{}
		defer func() {
			cancel()
			wg.Wait()
		}()
		wg.Add(1)
		go func() {
			sch.Process(ctx)
			wg.Done()
		}()

		schedule, ts, err := NewSchedule("* * * * * * *", time.Time{})
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: ts})
		if err != nil {
			t.Fatal(err)
		}

		mockTime.Set(mockTime.Now().UTC().Add(6 * time.Second))
		select {
		case <-c: // panic was caught and error handler used

		case <-time.After(4 * time.Second):
			t.Fatal("test timed out")
		}
	})
	t.Run("release should remove items from being scheduled", func(t *testing.T) {
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
		ctx, cancel := context.WithCancel(context.Background())
		sch, err := newSch(exe, WithTime(mockTime))
		if err != nil {
			t.Fatal(err)
		}
		wg := sync.WaitGroup{}
		defer func() {
			cancel()
			wg.Wait()
		}()
		wg.Add(1)
		go func() {
			sch.Process(ctx)
			wg.Done()
		}()

		schedule, ts, err := NewSchedule("* * * * * * *", mockTime.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}

		err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: ts.UTC()})
		if err != nil {
			t.Fatal(err)
		}
		mockTime.Set(mockTime.Now().UTC().Add(2 * time.Second))

		select {
		case <-c:
		case <-time.After(6 * time.Second):
			t.Fatalf("test timed out, it should have fired but didn't")
		}
		if err := sch.Release(1); err != nil {
			t.Error(err)
		}

		mockTime.Set(mockTime.Now().UTC().Add(6 * time.Second))

		select {
		case <-c:
			t.Fatal("expected test not to fire here, because task was released, but it did anyway")
		case <-time.After(2 * time.Second):
		}
	})
	t.Run("working with different local TZs", func(t *testing.T) {
		timeZ := []string{
			"America/Chicago",
			"America/New_York",
			"UTC",
			"America/Los_Angeles",
			"Asia/Kolkata",
		}
		for _, tzStr := range timeZ {
			loc, err := time.LoadLocation(tzStr)
			if err != nil {
				t.Fatalf("Cannot load %s location, the underlying system needs updated timezones, %s", loc, err)
			}
			oldLocal := time.Local
			time.Local = loc
			defer func() { time.Local = oldLocal }()
			mockTime := clock.NewMock()
			currentMockTime := time.Date(2020, time.December, 11, 11, 11, 3, 22, time.UTC)
			mockTime.Set(currentMockTime)

			c := make(chan time.Time, 100)
			exe := &mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id ID, scheduledAt time.Time) {
				select {
				case <-ctx.Done():
					t.Log("ctx done")
				case c <- scheduledAt:
				}
			}}
			sch, err := newSch(exe, WithTime(mockTime))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				sch.Process(ctx)
				wg.Done()
			}()
			defer func() {
				cancel()
				wg.Wait()
			}()

			schedule, ts, err := NewSchedule("0 * * * *", mockTime.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}

			err = sch.Schedule(mockSchedulable{id: 1, schedule: schedule, offset: time.Second, lastScheduled: ts})
			if err != nil {
				t.Fatal(err)
			}

			mockTime.Set(mockTime.Now().UTC().Add(time.Hour))

			after := time.After(6 * time.Second)
			select {
			case checkC := <-c:
				if !checkC.Equal(ts.UTC().Truncate(time.Hour).Add(time.Hour)) {
					t.Fatalf("scheduled at was incorrect expected %s, got %s", ts.UTC().Truncate(time.Hour).Add(time.Hour), checkC)
				}
			case <-after:
				t.Fatalf("test timed out")
			}
		}
	})

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
