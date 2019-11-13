package backend_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/flux"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/influxdata/influxdb/kit/prom/promtest"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/mock"
	"go.uber.org/zap/zaptest"
)

func TestScheduler_Cancelation(t *testing.T) {
	t.Skip("https://github.com/influxdata/influxdb/issues/13358")
	t.Parallel()

	tcs := mock.NewTaskControlService()
	e := mock.NewExecutor()
	e.WithHanging(100 * time.Millisecond)

	o := backend.NewScheduler(tcs, e, 5, backend.WithLogger(zaptest.NewLogger(t)))
	o.Start(context.Background())
	defer o.Stop()

	const orgID = 2
	latestCompleted, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:04Z")

	task := &platform.Task{
		ID:              platform.ID(1),
		OrganizationID:  orgID,
		Every:           "1s",
		LatestCompleted: latestCompleted,
		Flux:            `option task = {name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`,
	}
	tcs.SetTask(task)
	if err := o.ClaimTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	runs, err := tcs.CurrentlyRunning(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	run := runs[0]
	if err = o.CancelRun(context.Background(), task.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // we have to do this because the storage system we are using for the logs is eventually consistent.
	runs, err = tcs.CurrentlyRunning(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatal("canceled run still running")
	}

	// check for when we cancel something already canceled
	time.Sleep(500 * time.Millisecond)
	if err = o.CancelRun(context.Background(), task.ID, run.ID); err != platform.ErrRunNotFound {
		t.Fatalf("expected ErrRunNotFound but got %s", err)
	}
}

func TestScheduler_StartScriptOnClaim(t *testing.T) {
	t.Parallel()

	tcs := mock.NewTaskControlService()
	e := mock.NewExecutor()
	o := backend.NewScheduler(tcs, e, 5, backend.WithLogger(zaptest.NewLogger(t)))
	o.Start(context.Background())
	defer o.Stop()

	latestCompleted, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:03Z")
	task := &platform.Task{
		ID:              platform.ID(1),
		Cron:            "* * * * *",
		LatestCompleted: latestCompleted,
		Flux:            `option task = {name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`,
	}

	tcs.SetTask(task)
	if err := o.ClaimTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	// No valid timestamps between 3 and 5 for every minute.
	if n := len(tcs.CreatedFor(task.ID)); n > 0 {
		t.Fatalf("expected no runs queued, but got %d", n)
	}

	// For every second, can queue for timestamps 4 and 5.
	task = &platform.Task{
		ID:              platform.ID(2),
		Every:           "1s",
		LatestCompleted: latestCompleted,
		Flux:            `option task = {concurrency: 99, name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`,
	}

	tcs.SetTask(task)
	if err := o.ClaimTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	if n := len(tcs.CreatedFor(task.ID)); n != 2 {
		t.Fatalf("expected 2 runs queued for 'every 1s' script, but got %d", n)
	}

	if x, err := tcs.PollForNumberCreated(task.ID, 2); err != nil {
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

	if x, err := tcs.PollForNumberCreated(task.ID, 0); err != nil {
		t.Fatalf("expected 1 runs queued, but got %d", len(x))
	}

	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}
}

func TestScheduler_DontRunInactiveTasks(t *testing.T) {
	t.Parallel()

	tcs := mock.NewTaskControlService()
	e := mock.NewExecutor()
	o := backend.NewScheduler(tcs, e, 5)
	o.Start(context.Background())
	defer o.Stop()
	latestCompleted, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:05Z")

	task := &platform.Task{
		ID:              platform.ID(1),
		Every:           "1s",
		LatestCompleted: latestCompleted,
		Status:          "inactive",
		Flux:            `option task = {concurrency: 2, name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`,
	}

	tcs.SetTask(task)
	if err := o.ClaimTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	if x, err := tcs.PollForNumberCreated(task.ID, 0); err != nil {
		t.Fatalf("expected no runs queued, but got %d", len(x))
	}

	o.Tick(6)
	if x, err := tcs.PollForNumberCreated(task.ID, 0); err != nil {
		t.Fatalf("expected no runs on inactive task, got %d", len(x))
	}
}

func TestScheduler_CreateNextRunOnTick(t *testing.T) {
	t.Parallel()

	tcs := mock.NewTaskControlService()
	e := mock.NewExecutor()
	o := backend.NewScheduler(tcs, e, 5)
	o.Start(context.Background())
	defer o.Stop()
	latestCompleted, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:05Z")

	task := &platform.Task{
		ID:              platform.ID(1),
		Every:           "1s",
		LatestCompleted: latestCompleted,
		Flux:            `option task = {concurrency: 2, name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`,
	}

	tcs.SetTask(task)
	if err := o.ClaimTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	if x, err := tcs.PollForNumberCreated(task.ID, 0); err != nil {
		t.Fatalf("expected no runs queued, but got %d", len(x))
	}

	o.Tick(6)
	if x, err := tcs.PollForNumberCreated(task.ID, 1); err != nil {
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
	if x, err := tcs.PollForNumberCreated(task.ID, 2); err != nil {
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
	if x, err := tcs.PollForNumberCreated(task.ID, 2); err != nil {
		t.Fatalf("expected 2 runs queued, but got %d", len(x))
	}
	run6.Cancel() // 7 and 8 should be running.
	run7.Cancel()

	// Run 8 should be remaining.
	if _, err := e.PollForNumberRunning(task.ID, 1); err != nil {
		t.Fatal(err)
	}
}

func TestScheduler_LogStatisticsOnSuccess(t *testing.T) {
	t.Skip("flaky test: https://github.com/influxdata/influxdb/issues/15394")
	t.Parallel()

	tcs := mock.NewTaskControlService()
	e := mock.NewExecutor()

	o := backend.NewScheduler(tcs, e, 5, backend.WithLogger(zaptest.NewLogger(t)))
	o.Start(context.Background())
	defer o.Stop()

	const taskID = 0x12345
	const orgID = 0x54321
	latestCompleted, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:05Z")

	task := &platform.Task{
		ID:              taskID,
		OrganizationID:  orgID,
		Every:           "1s",
		LatestCompleted: latestCompleted,
		Flux:            `option task = {name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`,
	}

	tcs.SetTask(task)
	if err := o.ClaimTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	o.Tick(6)

	p, err := e.PollForNumberRunning(taskID, 1)
	if err != nil {
		t.Fatal(err)
	}

	rr := mock.NewRunResult(nil, false)
	rr.Stats = flux.Statistics{Metadata: flux.Metadata{"foo": []interface{}{"bar"}}}
	p[0].Finish(rr, nil)

	runID := p[0].Run().RunID

	if _, err := e.PollForNumberRunning(taskID, 0); err != nil {
		t.Fatal(err)
	}

	run := tcs.FinishedRun(runID)

	// For now, assume the stats line is the only line beginning with "{".
	var statJSON string
	for _, log := range run.Log {
		if len(log.Message) > 0 && log.Message[0] == '{' {
			statJSON = log.Message
			break
		}
	}

	if statJSON == "" {
		t.Fatal("could not find log message that looked like statistics")
	}
	var stats flux.Statistics
	if err := json.Unmarshal([]byte(statJSON), &stats); err != nil {
		t.Fatal(err)
	}
	foo := stats.Metadata["foo"]
	if !reflect.DeepEqual(foo, []interface{}{"bar"}) {
		t.Fatalf("query statistics were not encoded correctly into logs. expected metadata.foo=[bar], got: %#v", stats)
	}
}

func TestScheduler_Release(t *testing.T) {
	t.Parallel()

	tcs := mock.NewTaskControlService()
	e := mock.NewExecutor()
	o := backend.NewScheduler(tcs, e, 5)
	o.Start(context.Background())
	defer o.Stop()
	latestCompleted, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:05Z")

	task := &platform.Task{
		ID:              platform.ID(1),
		Every:           "1s",
		LatestCompleted: latestCompleted,
		Flux:            `option task = {concurrency: 99, name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`,
	}

	tcs.SetTask(task)
	if err := o.ClaimTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	o.Tick(6)
	o.Tick(7)
	if n := len(tcs.CreatedFor(task.ID)); n != 2 {
		t.Fatalf("expected 2 runs queued, but got %d", n)
	}

	if err := o.ReleaseTask(task.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := tcs.PollForNumberCreated(task.ID, 0); err != nil {
		t.Fatal(err)
	}
}

func TestScheduler_UpdateTask(t *testing.T) {
	t.Parallel()

	tcs := mock.NewTaskControlService()
	e := mock.NewExecutor()
	s := backend.NewScheduler(tcs, e, 3059, backend.WithLogger(zaptest.NewLogger(t)))
	s.Start(context.Background())
	defer s.Stop()
	latestCompleted, _ := time.Parse(time.RFC3339, "1970-01-01T00:50:00Z")

	task := &platform.Task{
		ID:              platform.ID(1),
		Cron:            "* * * * *",
		LatestCompleted: latestCompleted,
		Flux:            `option task = {name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`,
	}

	tcs.SetTask(task)
	if err := s.ClaimTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	s.Tick(3060)
	p, err := e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	p[0].Finish(mock.NewRunResult(nil, false), nil)

	task.Cron = "0 * * * *"
	task.Flux = `option task = {concurrency: 50, name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`
	tcs.SetTask(task)

	if err := s.UpdateTask(context.Background(), task); err != nil {
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
	t.Parallel()

	tcs := mock.NewTaskControlService()
	e := mock.NewExecutor()
	o := backend.NewScheduler(tcs, e, 3059, backend.WithLogger(zaptest.NewLogger(t)))
	o.Start(context.Background())
	defer o.Stop()
	latestCompleted, _ := time.Parse(time.RFC3339, "1970-01-01T00:50:00Z")

	task := &platform.Task{
		ID:              platform.ID(1),
		Cron:            "* * * * *",
		LatestCompleted: latestCompleted,
		Flux:            `option task = {name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`,
	}
	t1, _ := time.Parse(time.RFC3339, "1970-01-01T00:02:00Z")
	t2, _ := time.Parse(time.RFC3339, "1970-01-01T00:03:00Z")
	t3, _ := time.Parse(time.RFC3339, "1970-01-01T00:04:00Z")

	tcs.SetTask(task)
	tcs.SetManualRuns([]*platform.Run{
		&platform.Run{
			ID:           platform.ID(10),
			TaskID:       task.ID,
			ScheduledFor: t1,
		},
		&platform.Run{
			ID:           platform.ID(11),
			TaskID:       task.ID,
			ScheduledFor: t2,
		}, &platform.Run{
			ID:           platform.ID(12),
			TaskID:       task.ID,
			ScheduledFor: t3,
		},
	})
	if err := o.ClaimTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	cs, err := tcs.PollForNumberCreated(task.ID, 1)
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

	// Cancel 180. Next run should be 240, manual runs get priority.
	e.RunningFor(task.ID)[0].Cancel()
	pollForRun(240)

	// Cancel the 240 run; 3060 should pick up.
	e.RunningFor(task.ID)[0].Cancel()
	pollForRun(3060)

	// Cancel 3060; jobs should be idle.
	e.RunningFor(task.ID)[0].Cancel()
	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}
}

// LogListener allows us to act as a middleware and see if specific logs have been written
type logListener struct {
	mu sync.Mutex

	backend.TaskControlService

	logs map[string][]string
}

func newLogListener(tcs backend.TaskControlService) *logListener {
	return &logListener{
		TaskControlService: tcs,
		logs:               make(map[string][]string),
	}
}

func (l *logListener) AddRunLog(ctx context.Context, taskID, runID platform.ID, when time.Time, log string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	logs := l.logs[taskID.String()+runID.String()]
	logs = append(logs, log)
	l.logs[taskID.String()+runID.String()] = logs

	return l.TaskControlService.AddRunLog(ctx, taskID, runID, when, log)
}

func pollForRunLog(t *testing.T, ll *logListener, taskID, runID platform.ID, exp string) {
	t.Helper()

	var logs []string

	const maxAttempts = 50
	for i := 0; i < maxAttempts; i++ {
		if i != 0 {
			time.Sleep(10 * time.Millisecond)
		}
		ll.mu.Lock()
		logs = ll.logs[taskID.String()+runID.String()]
		ll.mu.Unlock()

		for _, log := range logs {
			if log == exp {
				return
			}
		}
	}

	t.Logf("Didn't find message %q in logs:", exp)
	for _, log := range logs {
		t.Logf("\t%s", log)
	}
	t.FailNow()
}

// runListener allows us to act as a middleware and see if specific states are updated
type runListener struct {
	mu sync.Mutex

	backend.TaskControlService

	rs map[platform.ID][]*platform.Run
}

func newRunListener(tcs backend.TaskControlService) *runListener {
	return &runListener{
		TaskControlService: tcs,
		rs:                 make(map[platform.ID][]*platform.Run),
	}
}

func (l *runListener) UpdateRunState(ctx context.Context, taskID, runID platform.ID, when time.Time, state backend.RunStatus) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	runs, ok := l.rs[taskID]
	if !ok {
		runs = []*platform.Run{}
	}
	found := false
	for _, run := range runs {
		if run.ID == runID {
			found = true
			run.Status = state.String()
		}
	}
	if !found {
		runs = append(runs, &platform.Run{ID: runID, Status: state.String()})
	}

	l.rs[taskID] = runs

	return l.TaskControlService.UpdateRunState(ctx, taskID, runID, when, state)
}

// pollForRunStatus tries a few times to find runs matching supplied conditions, before failing.
func pollForRunStatus(t *testing.T, r *runListener, taskID platform.ID, expCount, expIndex int, expStatus string) {
	t.Helper()

	var runs []*platform.Run
	const maxAttempts = 50
	for i := 0; i < maxAttempts; i++ {
		if i != 0 {
			time.Sleep(10 * time.Millisecond)
		}

		r.mu.Lock()
		runs = r.rs[taskID]
		runs := make([]*platform.Run, len(r.rs[taskID]))
		copy(runs, r.rs[taskID])
		r.mu.Unlock()

		if len(runs) != expCount {
			continue
		}

		// make sure we dont panic
		if len(runs) < expIndex {
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

func TestScheduler_RunStatus(t *testing.T) {
	t.Skip("https://github.com/influxdata/influxdb/issues/15273")

	t.Parallel()

	tcs := mock.NewTaskControlService()
	e := mock.NewExecutor()
	rl := newRunListener(tcs)
	s := backend.NewScheduler(rl, e, 5, backend.WithLogger(zaptest.NewLogger(t)))
	s.Start(context.Background())
	defer s.Stop()
	latestCompleted, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:05Z")

	// Claim a task that starts later.
	task := &platform.Task{
		ID:              platform.ID(1),
		OrganizationID:  platform.ID(2),
		Every:           "1s",
		LatestCompleted: latestCompleted,
		Flux:            `option task = {concurrency: 99, name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`,
	}

	tcs.SetTask(task)
	if err := s.ClaimTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	s.Tick(6)
	promises, err := e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	runs, err := tcs.CurrentlyRunning(context.Background(), task.ID)
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

	// Finish with failure to create the run.
	promises[0].Finish(nil, errors.New("forced failure"))
	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}

	pollForRunStatus(t, rl, task.ID, 2, 1, backend.RunFail.String())

	// Create a new run that starts but fails.
	s.Tick(8)
	promises, err = e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	pollForRunStatus(t, rl, task.ID, 3, 2, backend.RunStarted.String())
	promises[0].Finish(mock.NewRunResult(errors.New("started but failed to finish properly"), false), nil)
	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}
	pollForRunStatus(t, rl, task.ID, 3, 2, backend.RunFail.String())

	// One more run, but cancel this time.
	s.Tick(9)
	promises, err = e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	pollForRunStatus(t, rl, task.ID, 4, 3, backend.RunStarted.String())

	// Finish with failure.
	promises[0].Cancel()
	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}

	pollForRunStatus(t, rl, task.ID, 4, 3, backend.RunCanceled.String())
}

func TestScheduler_RunFailureCleanup(t *testing.T) {
	t.Skip("https://github.com/influxdata/influxdb/issues/13358")
	t.Parallel()

	tcs := mock.NewTaskControlService()
	e := mock.NewExecutor()
	ll := newLogListener(tcs)
	s := backend.NewScheduler(ll, e, 5, backend.WithLogger(zaptest.NewLogger(t)))
	s.Start(context.Background())
	defer s.Stop()
	latestCompleted, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:05Z")

	// Task with concurrency 1 should continue after one run fails.
	task := &platform.Task{
		ID:              platform.ID(1),
		Every:           "1s",
		LatestCompleted: latestCompleted,
		Flux:            `option task = {name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`,
	}

	tcs.SetTask(task)
	if err := s.ClaimTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	s.Tick(6)
	promises, err := e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Finish with failure to create the run.
	promises[0].Finish(nil, errors.New("forced failure"))
	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}
	pollForRunLog(t, ll, task.ID, promises[0].Run().RunID, "Waiting for execution result: forced failure")

	// Should continue even if max concurrency == 1.
	// This run will start and then fail.
	s.Tick(7)
	promises, err = e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}

	promises[0].Finish(mock.NewRunResult(errors.New("started but failed to finish properly"), false), nil)
	if _, err := e.PollForNumberRunning(task.ID, 0); err != nil {
		t.Fatal(err)
	}
	pollForRunLog(t, ll, task.ID, promises[0].Run().RunID, "Run failed to execute: started but failed to finish properly")

	// Fail to execute next run.
	if n := tcs.TotalRunsCreatedForTask(task.ID); n != 2 {
		t.Fatalf("should have created 2 runs so far, got %d", n)
	}
	e.FailNextCallToExecute(errors.New("forced failure on Execute"))
	s.Tick(8)
	// The execution happens in the background, so check a few times for 3 runs created.
	const attempts = 50
	for i := 0; i < attempts; i++ {
		time.Sleep(2 * time.Millisecond)
		n := tcs.TotalRunsCreatedForTask(task.ID)
		if n == 3 {
			break
		}
		if i == attempts-1 {
			// Fail if we haven't seen the right count by the last attempt.
			t.Fatalf("expected 3 runs created, got %d", n)
		}
	}
	// We don't have a good hook to get the run ID right now, so list the runs and assume the final one is ours.
	runs := tcs.FinishedRuns()
	if err != nil {
		t.Fatal(err)
	}
	pollForRunLog(t, ll, task.ID, runs[len(runs)-1].ID, "Run failed to begin execution: forced failure on Execute")

	// One more tick just to ensure that we can keep going after this type of failure too.
	s.Tick(9)
	_, err = e.PollForNumberRunning(task.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestScheduler_Metrics(t *testing.T) {
	t.Parallel()

	tcs := mock.NewTaskControlService()
	e := mock.NewExecutor()
	s := backend.NewScheduler(tcs, e, 5)
	s.Start(context.Background())
	defer s.Stop()

	reg := prom.NewRegistry()
	// PrometheusCollector isn't part of the Scheduler interface. Yet.
	// Still thinking about whether it should be.
	reg.MustRegister(s.PrometheusCollectors()...)

	// Claim a task that starts later.
	latestCompleted, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:05Z")
	task := &platform.Task{
		ID:              platform.ID(1),
		Every:           "1s",
		LatestCompleted: latestCompleted,
		Flux:            `option task = {concurrency: 99, name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`,
	}

	tcs.SetTask(task)
	if err := s.ClaimTask(context.Background(), task); err != nil {
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
	m = promtest.MustFindMetric(t, mfs, "task_scheduler_run_queue_delta", map[string]string{"taskID": "all"})
	if got := m.Summary.GetSampleCount(); got != 1.0 {
		t.Fatalf("expected 1 delta in summary: got: %v", got)
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
	m = promtest.MustFindMetric(t, mfs, "task_scheduler_run_queue_delta", map[string]string{"taskID": "all"})
	if got := m.Summary.GetSampleCount(); got != 2.0 {
		t.Fatalf("expected 2 delta in summary: got: %v", got)
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

	m = promtest.MustFindMetric(t, mfs, "task_scheduler_errors_counter", map[string]string{"error_type": "internal error"})
	if got := *m.Counter.Value; got != 1 {
		t.Fatalf("expected error type in metric to be internal error, got %v", got)
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
	o := backend.NewScheduler(mock.NewTaskControlService(), e, 4, backend.WithLogger(zaptest.NewLogger(t)))
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
	tcs := mock.NewTaskControlService()
	e := mock.NewExecutor()
	o := backend.NewScheduler(tcs, e, 5, backend.WithLogger(zaptest.NewLogger(t)), backend.WithTicker(ctx, tickFreq))

	o.Start(ctx)
	defer o.Stop()
	createdAt := time.Now().UTC()
	task := &platform.Task{
		ID:              platform.ID(1),
		Every:           "1s",
		Flux:            `option task = {concurrency: 5, name:"x", every:1m} from(bucket:"a") |> to(bucket:"b", org: "o")`,
		LatestCompleted: createdAt,
	}

	tcs.SetTask(task)
	if err := o.ClaimTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}

	for time.Now().Unix() == createdAt.Unix() {
		time.Sleep(tickFreq + 10*time.Millisecond)
	}

	if x, err := tcs.PollForNumberCreated(task.ID, 1); err != nil {
		t.Fatalf("expected 1 run queued, but got %d", len(x))
	}
}
