package backend_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/influxdata/influxdb/kit/prom/promtest"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/mock"
	"go.uber.org/zap/zaptest"
)

func TestScheduler_Cancelation(t *testing.T) {
	t.Parallel()

	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	e.WithHanging(100 * time.Millisecond)
	rl := backend.NewInMemRunReaderWriter()

	o := backend.NewScheduler(d, e, rl, 5, backend.WithLogger(zaptest.NewLogger(t)))
	o.Start(context.Background())
	defer o.Stop()

	task := &backend.StoreTask{
		ID: platform.ID(1),
	}
	meta := &backend.StoreTaskMeta{
		MaxConcurrency:  1,
		EffectiveCron:   "@every 1s",
		LatestCompleted: 4,
	}
	d.SetTaskMeta(task.ID, *meta)
	if err := o.ClaimTask(task, meta); err != nil {
		t.Fatal(err)
	}
	runs, err := rl.ListRuns(context.Background(), platform.RunFilter{Task: &task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if err = o.CancelRun(context.Background(), task.ID, runs[0].ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond) // we have to do this because the storage system we are using for the logs is eventually consistent.
	runs, err = rl.ListRuns(context.Background(), platform.RunFilter{Task: &task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if runs[0].Status != "canceled" {
		t.Fatalf("Run not logged as canceled, but is %s", runs[0].Status)
	}
	// check to make sure it is really canceling, and that the status doesn't get changed to something else after it would have finished
	time.Sleep(500 * time.Millisecond)
	runs, err = rl.ListRuns(context.Background(), platform.RunFilter{Task: &task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if runs[0].Status != "canceled" {
		t.Fatalf("Run not actually canceled, but is %s", runs[0].Status)
	}
	// check for when we cancel something already canceled
	if err = o.CancelRun(context.Background(), task.ID, runs[0].ID); err != backend.ErrRunNotFound {
		t.Fatalf("expected ErrRunNotFound but got %s", err)
	}
}

func TestScheduler_StartScriptOnClaim(t *testing.T) {
	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	o := backend.NewScheduler(d, e, backend.NopLogWriter{}, 5, backend.WithLogger(zaptest.NewLogger(t)))
	o.Start(context.Background())
	defer o.Stop()

	task := &backend.StoreTask{
		ID: platform.ID(1),
	}
	meta := &backend.StoreTaskMeta{
		MaxConcurrency:  1,
		EffectiveCron:   "* * * * *",
		LatestCompleted: 3,
	}
	d.SetTaskMeta(task.ID, *meta)
	if err := o.ClaimTask(task, meta); err != nil {
		t.Fatal(err)
	}

	// No valid timestamps between 3 and 5 for every minute.
	if n := len(d.CreatedFor(task.ID)); n > 0 {
		t.Fatalf("expected no runs queued, but got %d", n)
	}

	// For every second, can queue for timestamps 4 and 5.
	task = &backend.StoreTask{
		ID: platform.ID(2),
	}
	meta = &backend.StoreTaskMeta{
		MaxConcurrency:  99,
		EffectiveCron:   "@every 1s",
		LatestCompleted: 3,
		CurrentlyRunning: []*backend.StoreTaskMetaRun{
			&backend.StoreTaskMetaRun{
				Now:   4,
				RunID: uint64(10),
			},
		},
	}
	d.SetTaskMeta(task.ID, *meta)
	if err := o.ClaimTask(task, meta); err != nil {
		t.Fatal(err)
	}

	if n := len(d.CreatedFor(task.ID)); n != 1 {
		t.Fatalf("expected 2 runs queued for 'every 1s' script, but got %d", n)
	}

	if x, err := d.PollForNumberCreated(task.ID, 1); err != nil {
		t.Fatalf("expected 1 runs queued, but got %d", len(x))
	}

	rps, err := e.PollForNumberRunning(task.ID, 2)
	if err != nil {
		t.Fatal(err)
	}

	for _, rp := range rps {
		if rp.Run().Now != 4 && rp.Run().Now != 5 {
			t.Fatalf("unexpected running task %+v", rp)
		}
		rp.Finish(mock.NewRunResult(nil, false), nil)
	}

	if x, err := d.PollForNumberCreated(task.ID, 0); err != nil {
		t.Fatalf("expected 1 runs queued, but got %d", len(x))
	}

	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}
}

func TestScheduler_CreateNextRunOnTick(t *testing.T) {
	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	o := backend.NewScheduler(d, e, backend.NopLogWriter{}, 5)
	o.Start(context.Background())
	defer o.Stop()

	task := &backend.StoreTask{
		ID: platform.ID(1),
	}
	meta := &backend.StoreTaskMeta{
		MaxConcurrency:  2,
		EffectiveCron:   "@every 1s",
		LatestCompleted: 5,
	}

	d.SetTaskMeta(task.ID, *meta)
	if err := o.ClaimTask(task, meta); err != nil {
		t.Fatal(err)
	}

	if x, err := d.PollForNumberCreated(task.ID, 0); err != nil {
		t.Fatalf("expected no runs queued, but got %d", len(x))
	}

	o.Tick(6)
	if x, err := d.PollForNumberCreated(task.ID, 1); err != nil {
		t.Fatalf("expected 1 run queued, but got %d", len(x))
	}
	running, err := e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	run6 := running[0]
	if run6.Run().Now != 6 {
		t.Fatalf("unexpected now for run 6: %d", run6.Run().Now)
	}

	o.Tick(7)
	if x, err := d.PollForNumberCreated(task.ID, 2); err != nil {
		t.Fatalf("expected 2 runs queued, but got %d", len(x))
	}
	running, err = e.PollForNumberRunning(task.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	var run7 *mock.RunPromise
	for _, r := range running {
		if r.Run().Now == 7 {
			run7 = r
			break
		}
	}
	if run7 == nil {
		t.Fatalf("did not detect run with now=7; got %#v", running)
	}

	o.Tick(8) // Can't exceed concurrency of 2.
	if x, err := d.PollForNumberCreated(task.ID, 2); err != nil {
		t.Fatalf("expected 2 runs queued, but got %d", len(x))
	}
	run6.Cancel() // 7 and 8 should be running.
	run7.Cancel()

	// Run 8 should be remaining.
	if _, err := e.PollForNumberRunning(task.ID, 1); err != nil {
		t.Fatal(err)
	}
}

func TestScheduler_Release(t *testing.T) {
	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	o := backend.NewScheduler(d, e, backend.NopLogWriter{}, 5)
	o.Start(context.Background())
	defer o.Stop()

	task := &backend.StoreTask{
		ID: platform.ID(1),
	}
	meta := &backend.StoreTaskMeta{
		MaxConcurrency:  99,
		EffectiveCron:   "@every 1s",
		LatestCompleted: 5,
	}

	d.SetTaskMeta(task.ID, *meta)
	if err := o.ClaimTask(task, meta); err != nil {
		t.Fatal(err)
	}

	o.Tick(6)
	o.Tick(7)
	if n := len(d.CreatedFor(task.ID)); n != 2 {
		t.Fatalf("expected 2 runs queued, but got %d", n)
	}

	if err := o.ReleaseTask(task.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := d.PollForNumberCreated(task.ID, 0); err != nil {
		t.Fatal(err)
	}
}

func TestScheduler_UpdateTask(t *testing.T) {
	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	s := backend.NewScheduler(d, e, backend.NopLogWriter{}, 3059, backend.WithLogger(zaptest.NewLogger(t)))
	s.Start(context.Background())
	defer s.Stop()

	task := &backend.StoreTask{
		ID: platform.ID(1),
	}
	meta := &backend.StoreTaskMeta{
		MaxConcurrency:  1,
		EffectiveCron:   "* * * * *", // Every minute.
		LatestCompleted: 3000,
	}

	d.SetTaskMeta(task.ID, *meta)
	if err := s.ClaimTask(task, meta); err != nil {
		t.Fatal(err)
	}

	s.Tick(3060)
	p, err := e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	p[0].Finish(mock.NewRunResult(nil, false), nil)

	meta.EffectiveCron = "0 * * * *"
	meta.MaxConcurrency = 30
	d.SetTaskMeta(task.ID, *meta)

	if err := s.UpdateTask(task, meta); err != nil {
		t.Fatal(err)
	}

	s.Tick(3061)
	_, err = e.PollForNumberRunning(task.ID, 0)
	if err != nil {
		t.Fatal(err)
	}

	s.Tick(3600)
	p, err = e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	p[0].Finish(mock.NewRunResult(nil, false), nil)
}

func TestScheduler_Queue(t *testing.T) {
	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	o := backend.NewScheduler(d, e, backend.NopLogWriter{}, 3059, backend.WithLogger(zaptest.NewLogger(t)))
	o.Start(context.Background())
	defer o.Stop()

	task := &backend.StoreTask{
		ID: platform.ID(1),
	}
	meta := &backend.StoreTaskMeta{
		MaxConcurrency:  1,
		EffectiveCron:   "* * * * *", // Every minute.
		LatestCompleted: 3000,
		ManualRuns: []*backend.StoreTaskMetaManualRun{
			{Start: 120, End: 240, LatestCompleted: 119, RequestedAt: 3001},
		},
	}

	d.SetTaskMeta(task.ID, *meta)
	if err := o.ClaimTask(task, meta); err != nil {
		t.Fatal(err)
	}

	cs, err := d.PollForNumberCreated(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	c := cs[0]
	if c.Now != 120 {
		t.Fatalf("expected run from queue at 120, got %d", c.Now)
	}

	// Finish that run. Next one should start.
	promises, err := e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	promises[0].Finish(mock.NewRunResult(nil, false), nil)

	// The scheduler will start the next queued run immediately, so we can't poll for 0 running then 1 running.
	// But we can poll for a specific run.Now.
	pollForRun := func(expNow int64) {
		const numAttempts = 50
		found := false
		for i := 0; i < numAttempts; i++ {
			rps := e.RunningFor(task.ID)
			if len(rps) > 0 && rps[0].Run().Now == expNow {
				found = true
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		if !found {
			var nows []string
			for _, rp := range e.RunningFor(task.ID) {
				nows = append(nows, fmt.Sprint(rp.Run().Now))
			}
			t.Fatalf("polled but could not find run with now = %d in time; got: %s", expNow, strings.Join(nows, ", "))
		}
	}
	pollForRun(180)

	// The manual run for 180 is still going.
	// Tick the scheduler so the next natural run will happen once 180 finishes.
	o.Tick(3062)

	// Cancel 180. Next run should be 3060, the next natural schedule.
	e.RunningFor(task.ID)[0].Cancel()
	pollForRun(3060)

	// Cancel the 3060 run; 240 should pick up.
	e.RunningFor(task.ID)[0].Cancel()
	pollForRun(240)

	// Cancel 240; jobs should be idle.
	e.RunningFor(task.ID)[0].Cancel()
	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}
}

// pollForRunStatus tries a few times to find runs matching supplied conditions, before failing.
func pollForRunStatus(t *testing.T, r backend.LogReader, taskID platform.ID, expCount, expIndex int, expStatus string) {
	t.Helper()

	var runs []*platform.Run
	var err error

	const maxAttempts = 50
	for i := 0; i < maxAttempts; i++ {
		if i != 0 {
			time.Sleep(10 * time.Millisecond)
		}

		runs, err = r.ListRuns(context.Background(), platform.RunFilter{Task: &taskID})
		if err != nil {
			t.Fatal(err)
		}

		if len(runs) != expCount {
			continue
		}

		if runs[expIndex].Status != expStatus {
			continue
		}

		// Everything checks out if we got here.
		return
	}

	t.Logf("failed to find run with status %s", expStatus)
	for i, r := range runs {
		t.Logf("run[%d]: %#v", i, r)
	}
	t.FailNow()
}

func TestScheduler_RunLog(t *testing.T) {
	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	rl := backend.NewInMemRunReaderWriter()
	s := backend.NewScheduler(d, e, rl, 5, backend.WithLogger(zaptest.NewLogger(t)))
	s.Start(context.Background())
	defer s.Stop()

	// Claim a task that starts later.
	task := &backend.StoreTask{
		ID: platform.ID(1),
	}
	meta := &backend.StoreTaskMeta{
		MaxConcurrency:  99,
		EffectiveCron:   "@every 1s",
		LatestCompleted: 5,
	}

	d.SetTaskMeta(task.ID, *meta)
	if err := s.ClaimTask(task, meta); err != nil {
		t.Fatal(err)
	}

	if _, err := rl.ListRuns(context.Background(), platform.RunFilter{Task: &task.ID}); err != backend.ErrRunNotFound {
		t.Fatal(err)
	}

	s.Tick(6)
	promises, err := e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	runs, err := rl.ListRuns(context.Background(), platform.RunFilter{Task: &task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(runs); got != 1 {
		t.Fatalf("expected 1 run, got %d", got)
	}

	if got := runs[0].Status; got != backend.RunStarted.String() {
		t.Fatalf("expected run to be started, got %s", got)
	}

	// Finish with success.
	promises[0].Finish(mock.NewRunResult(nil, false), nil)
	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}

	pollForRunStatus(t, rl, task.ID, 1, 0, backend.RunSuccess.String())

	// Create a new run, but fail this time.
	s.Tick(7)
	promises, err = e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	pollForRunStatus(t, rl, task.ID, 2, 1, backend.RunStarted.String())

	// Finish with failure.
	promises[0].Finish(nil, errors.New("forced failure"))
	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}

	pollForRunStatus(t, rl, task.ID, 2, 1, backend.RunFail.String())

	// One more run, but cancel this time.
	s.Tick(8)
	promises, err = e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	pollForRunStatus(t, rl, task.ID, 3, 2, backend.RunStarted.String())

	// Finish with failure.
	promises[0].Cancel()
	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}

	pollForRunStatus(t, rl, task.ID, 3, 2, backend.RunCanceled.String())
}

func TestScheduler_Metrics(t *testing.T) {
	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	s := backend.NewScheduler(d, e, backend.NopLogWriter{}, 5)
	s.Start(context.Background())
	defer s.Stop()

	reg := prom.NewRegistry()
	// PrometheusCollector isn't part of the Scheduler interface. Yet.
	// Still thinking about whether it should be.
	reg.MustRegister(s.PrometheusCollectors()...)

	// Claim a task that starts later.
	task := &backend.StoreTask{
		ID: platform.ID(1),
	}
	meta := &backend.StoreTaskMeta{
		MaxConcurrency:  99,
		EffectiveCron:   "@every 1s",
		LatestCompleted: 5,
	}

	d.SetTaskMeta(task.ID, *meta)
	if err := s.ClaimTask(task, meta); err != nil {
		t.Fatal(err)
	}

	// Claims active/complete increases with a claim.
	mfs := promtest.MustGather(t, reg)
	m := promtest.MustFindMetric(t, mfs, "task_scheduler_claims_active", nil)
	if got := *m.Gauge.Value; got != 1 {
		t.Fatalf("expected 1 active claimed, got %v", got)
	}
	m = promtest.MustFindMetric(t, mfs, "task_scheduler_claims_complete", map[string]string{"status": "success"})
	if got := *m.Counter.Value; got != 1 {
		t.Fatalf("expected 1 total claimed, got %v", got)
	}

	s.Tick(6)
	if _, err := e.PollForNumberRunning(task.ID, 1); err != nil {
		t.Fatal(err)
	}

	// Runs active increases as run starts.
	mfs = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mfs, "task_scheduler_total_runs_active", nil)
	if got := *m.Gauge.Value; got != 1 {
		t.Fatalf("expected 1 total run active, got %v", got)
	}
	m = promtest.MustFindMetric(t, mfs, "task_scheduler_runs_active", map[string]string{"task_id": task.ID.String()})
	if got := *m.Gauge.Value; got != 1 {
		t.Fatalf("expected 1 run active for task ID %s, got %v", task.ID.String(), got)
	}

	s.Tick(7)
	if _, err := e.PollForNumberRunning(task.ID, 2); err != nil {
		t.Fatal(err)
	}

	mfs = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mfs, "task_scheduler_total_runs_active", nil)
	if got := *m.Gauge.Value; got != 2 {
		t.Fatalf("expected 2 total runs active, got %v", got)
	}
	m = promtest.MustFindMetric(t, mfs, "task_scheduler_runs_active", map[string]string{"task_id": task.ID.String()})
	if got := *m.Gauge.Value; got != 2 {
		t.Fatalf("expected 2 runs active for task ID %s, got %v", task.ID.String(), got)
	}

	// Runs active decreases as run finishes.
	e.RunningFor(task.ID)[0].Finish(mock.NewRunResult(nil, false), nil)
	if _, err := e.PollForNumberRunning(task.ID, 1); err != nil {
		t.Fatal(err)
	}
	mfs = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mfs, "task_scheduler_total_runs_active", nil)
	if got := *m.Gauge.Value; got != 1 {
		t.Fatalf("expected 1 total run active, got %v", got)
	}
	m = promtest.MustFindMetric(t, mfs, "task_scheduler_runs_active", map[string]string{"task_id": task.ID.String()})
	if got := *m.Gauge.Value; got != 1 {
		t.Fatalf("expected 1 run active for task ID %s, got %v", task.ID.String(), got)
	}
	m = promtest.MustFindMetric(t, mfs, "task_scheduler_runs_complete", map[string]string{"task_id": task.ID.String(), "status": "success"})
	if got := *m.Counter.Value; got != 1 {
		t.Fatalf("expected 1 run succeeded for task ID %s, got %v", task.ID.String(), got)
	}

	e.RunningFor(task.ID)[0].Finish(mock.NewRunResult(nil, false), errors.New("failed to execute"))
	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}
	mfs = promtest.MustGather(t, reg)
	m = promtest.MustFindMetric(t, mfs, "task_scheduler_total_runs_active", nil)
	if got := *m.Gauge.Value; got != 0 {
		t.Fatalf("expected 0 total runs active, got %v", got)
	}
	m = promtest.MustFindMetric(t, mfs, "task_scheduler_runs_active", map[string]string{"task_id": task.ID.String()})
	if got := *m.Gauge.Value; got != 0 {
		t.Fatalf("expected 0 runs active for task ID %s, got %v", task.ID.String(), got)
	}
	m = promtest.MustFindMetric(t, mfs, "task_scheduler_runs_complete", map[string]string{"task_id": task.ID.String(), "status": "failure"})
	if got := *m.Counter.Value; got != 1 {
		t.Fatalf("expected 1 run failed for task ID %s, got %v", task.ID.String(), got)
	}

	// Runs label removed after task released.
	if err := s.ReleaseTask(task.ID); err != nil {
		t.Fatal(err)
	}
	mfs = promtest.MustGather(t, reg)
	if m := promtest.FindMetric(mfs, "task_scheduler_runs_active", map[string]string{"task_id": task.ID.String()}); m != nil {
		t.Fatalf("expected metric to be removed after releasing a task, got %v", m)
	}
	if m := promtest.FindMetric(mfs, "task_scheduler_runs_complete", map[string]string{"task_id": task.ID.String(), "status": "success"}); m != nil {
		t.Fatalf("expected metric to be removed after releasing a task, got %v", m)
	}
	if m := promtest.FindMetric(mfs, "task_scheduler_runs_complete", map[string]string{"task_id": task.ID.String(), "status": "failure"}); m != nil {
		t.Fatalf("expected metric to be removed after releasing a task, got %v", m)
	}

	m = promtest.MustFindMetric(t, mfs, "task_scheduler_claims_active", nil)
	if got := *m.Gauge.Value; got != 0 {
		t.Fatalf("expected 0 claims active, got %v", got)
	}
}

type fakeWaitExecutor struct {
	wait chan struct{}
}

func (e *fakeWaitExecutor) Execute(ctx context.Context, run backend.QueuedRun) (backend.RunPromise, error) {
	panic("fakeWaitExecutor cannot Execute")
}

func (e *fakeWaitExecutor) Wait() {
	<-e.wait
}

func TestScheduler_Stop(t *testing.T) {
	t.Parallel()

	e := &fakeWaitExecutor{wait: make(chan struct{})}
	o := backend.NewScheduler(mock.NewDesiredState(), e, backend.NopLogWriter{}, 4, backend.WithLogger(zaptest.NewLogger(t)))
	o.Start(context.Background())

	stopped := make(chan struct{})
	go func() {
		o.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatalf("scheduler stopped before in-flight executions finished")
	case <-time.After(50 * time.Millisecond):
		// Okay.
	}

	close(e.wait)

	select {
	case <-stopped:
		// Okay.
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("scheduler did not stop after executor Wait returned")
	}
}

func TestScheduler_WithTicker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tickFreq := 100 * time.Millisecond
	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	o := backend.NewScheduler(d, e, backend.NopLogWriter{}, 5, backend.WithLogger(zaptest.NewLogger(t)), backend.WithTicker(ctx, tickFreq))

	o.Start(ctx)
	defer o.Stop()

	task := &backend.StoreTask{
		ID: platform.ID(1),
	}
	createdAt := time.Now().Unix()
	meta := &backend.StoreTaskMeta{
		MaxConcurrency:  5,
		EffectiveCron:   "@every 1s",
		LatestCompleted: createdAt,
	}

	d.SetTaskMeta(task.ID, *meta)
	if err := o.ClaimTask(task, meta); err != nil {
		t.Fatal(err)
	}

	for time.Now().Unix() == createdAt {
		time.Sleep(tickFreq + 10*time.Millisecond)
	}

	if x, err := d.PollForNumberCreated(task.ID, 1); err != nil {
		t.Fatalf("expected 1 run queued, but got %d", len(x))
	}
}
