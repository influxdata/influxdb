package backend_test

import (
	"context"
	"errors"
	"testing"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/kit/prom"
	"github.com/influxdata/platform/kit/prom/promtest"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/task/backend"
	"github.com/influxdata/platform/task/mock"
	"go.uber.org/zap/zaptest"
)

func TestScheduler_StartScriptOnClaim(t *testing.T) {
	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	o := backend.NewScheduler(d, e, backend.NopLogWriter{}, 5, backend.WithLogger(zaptest.NewLogger(t)))

	task := &backend.StoreTask{
		ID: platform.ID{1},
	}
	meta := &backend.StoreTaskMeta{
		MaxConcurrency: 1,
		EffectiveCron:  "* * * * *",
		LastCompleted:  3,
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
		ID: platform.ID{2},
	}
	meta = &backend.StoreTaskMeta{
		MaxConcurrency: 99,
		EffectiveCron:  "@every 1s",
		LastCompleted:  3,
	}
	d.SetTaskMeta(task.ID, *meta)
	if err := o.ClaimTask(task, meta); err != nil {
		t.Fatal(err)
	}

	if n := len(d.CreatedFor(task.ID)); n != 2 {
		t.Fatalf("expected 2 runs queued for 'every 1s' script, but got %d", n)
	}
}

func TestScheduler_CreateNextRunOnTick(t *testing.T) {
	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	o := backend.NewScheduler(d, e, backend.NopLogWriter{}, 5)

	task := &backend.StoreTask{
		ID: platform.ID{1},
	}
	meta := &backend.StoreTaskMeta{
		MaxConcurrency: 2,
		EffectiveCron:  "@every 1s",
		LastCompleted:  5,
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

	o.Tick(7)
	if x, err := d.PollForNumberCreated(task.ID, 2); err != nil {
		t.Fatalf("expected 2 runs queued, but got %d", len(x))
	}
	o.Tick(8) // Can't exceed concurrency of 2.
	if x, err := d.PollForNumberCreated(task.ID, 2); err != nil {
		t.Fatalf("expected 2 runs queued, but got %d", len(x))
	}
	run6.Cancel()

	if x, err := d.PollForNumberCreated(task.ID, 1); err != nil {
		t.Fatal(err, x)
	}
}

func TestScheduler_Release(t *testing.T) {
	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	o := backend.NewScheduler(d, e, backend.NopLogWriter{}, 5)

	task := &backend.StoreTask{
		ID: platform.ID{1},
	}
	meta := &backend.StoreTaskMeta{
		MaxConcurrency: 99,
		EffectiveCron:  "@every 1s",
		LastCompleted:  5,
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

func TestScheduler_RunLog(t *testing.T) {
	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	rl := backend.NewInMemRunReaderWriter()
	s := backend.NewScheduler(d, e, rl, 5)

	// Claim a task that starts later.
	task := &backend.StoreTask{
		ID: platform.ID{1},
	}
	meta := &backend.StoreTaskMeta{
		MaxConcurrency: 99,
		EffectiveCron:  "@every 1s",
		LastCompleted:  5,
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

	runs, err = rl.ListRuns(context.Background(), platform.RunFilter{Task: &task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(runs); got != 1 {
		t.Fatalf("expected 1 run, got %d", got)
	}

	if got := runs[0].Status; got != backend.RunSuccess.String() {
		t.Fatalf("expected run to be success, got %s", got)
	}

	// Create a new run, but fail this time.
	s.Tick(7)
	promises, err = e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	runs, err = rl.ListRuns(context.Background(), platform.RunFilter{Task: &task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(runs); got != 2 {
		t.Fatalf("expected 2 runs, got %d", got)
	}

	if got := runs[1].Status; got != backend.RunStarted.String() {
		t.Fatalf("expected run to be started, got %s", got)
	}

	// Finish with failure.
	promises[0].Finish(nil, errors.New("forced failure"))
	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}

	runs, err = rl.ListRuns(context.Background(), platform.RunFilter{Task: &task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(runs); got != 2 {
		t.Fatalf("expected 2 runs, got %d", got)
	}

	if got := runs[1].Status; got != backend.RunFail.String() {
		t.Fatalf("expected run to be failure, got %s", got)
	}

	// One more run, but cancel this time.
	s.Tick(8)
	promises, err = e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	runs, err = rl.ListRuns(context.Background(), platform.RunFilter{Task: &task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(runs); got != 3 {
		t.Fatalf("expected 3 runs, got %d", got)
	}

	if got := runs[2].Status; got != backend.RunStarted.String() {
		t.Fatalf("expected run to be started, got %s", got)
	}

	// Finish with failure.
	promises[0].Cancel()
	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}

	runs, err = rl.ListRuns(context.Background(), platform.RunFilter{Task: &task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(runs); got != 3 {
		t.Fatalf("expected 3 runs, got %d", got)
	}

	if got := runs[2].Status; got != backend.RunCanceled.String() {
		t.Fatalf("expected run to be canceled, got %s", got)
	}
}

func TestScheduler_Metrics(t *testing.T) {
	d := mock.NewDesiredState()
	e := mock.NewExecutor()
	s := backend.NewScheduler(d, e, backend.NopLogWriter{}, 5)

	reg := prom.NewRegistry()
	// PrometheusCollector isn't part of the Scheduler interface. Yet.
	// Still thinking about whether it should be.
	reg.MustRegister(s.(prom.PrometheusCollector).PrometheusCollectors()...)

	// Claim a task that starts later.
	task := &backend.StoreTask{
		ID: platform.ID{1},
	}
	meta := &backend.StoreTaskMeta{
		MaxConcurrency: 99,
		EffectiveCron:  "@every 1s",
		LastCompleted:  5,
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
