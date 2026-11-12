package launcher_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	nethttp "net/http"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	th "github.com/influxdata/influxdb/v2/pkg/testing/helper"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const restartTaskFlux = `option task = {name: "restart", every: 1h} from(bucket: "BUCKET") |> range(start: -1h)`

// taskRun is the subset of a run returned by the tasks API used by these tests.
type taskRun struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// restartLauncher shuts down l without removing its data directory, then starts
// a new server on the same data. The setup of l is copied to the new server.
func restartLauncher(t *testing.T, l *launcher.TestLauncher, setters ...launcher.OptSetter) *launcher.TestLauncher {
	t.Helper()
	path := l.Path
	t.Cleanup(func() { os.RemoveAll(path) })

	require.NoError(t, l.Launcher.Shutdown(ctx))

	l2 := launcher.NewTestLauncherServer()
	require.NoError(t, os.RemoveAll(l2.Path))
	l2.Path = path
	l2.User, l2.Org, l2.Bucket, l2.Auth = l.User, l.Org, l.Bucket, l.Auth
	l2.RunOrFail(t, ctx, setters...)
	return l2
}

func createTask(t *testing.T, l *launcher.TestLauncher, orgID platform.ID, flux string) *taskmodel.Task {
	t.Helper()
	task, err := l.TaskService(t).CreateTask(ctx, taskmodel.TaskCreate{
		OrganizationID: orgID,
		OwnerID:        l.User.ID,
		Flux:           flux,
	})
	require.NoError(t, err)
	return task
}

func listRuns(t *testing.T, l *launcher.TestLauncher, token string, taskID platform.ID) []taskRun {
	t.Helper()
	return listRunsBefore(t, l, token, taskID, time.Time{})
}

// listRunsBefore lists the task's runs scheduled before the given time, or
// before now if it is zero.
func listRunsBefore(t *testing.T, l *launcher.TestLauncher, token string, taskID platform.ID, before time.Time) []taskRun {
	t.Helper()
	path := fmt.Sprintf("/api/v2/tasks/%s/runs", taskID)
	if !before.IsZero() {
		path += "?beforeTime=" + before.UTC().Format(time.RFC3339)
	}
	req := l.NewHTTPRequestOrFail(t, "GET", path, token, "")
	var resp struct {
		Runs []taskRun `json:"runs"`
	}
	require.NoError(t, json.Unmarshal(mustDoRequest(t, req, nethttp.StatusOK), &resp))
	return resp.Runs
}

func getRun(t *testing.T, l *launcher.TestLauncher, token string, taskID platform.ID, runID string) taskRun {
	t.Helper()
	req := l.NewHTTPRequestOrFail(t, "GET", fmt.Sprintf("/api/v2/tasks/%s/runs/%s", taskID, runID), token, "")
	var run taskRun
	require.NoError(t, json.Unmarshal(mustDoRequest(t, req, nethttp.StatusOK), &run))
	return run
}

// runTaskNow starts a manual run of the task, waits for it to succeed, and
// returns its ID. A finished run is moved to the task system bucket, so reading
// it back goes through the analytical storage queries.
func runTaskNow(t *testing.T, l *launcher.TestLauncher, taskID platform.ID) string {
	t.Helper()
	runID := startRun(t, l, taskID)
	waitForRun(t, l, taskID, runID)
	return runID
}

// startRun starts a manual run of the task and returns its ID.
func startRun(t *testing.T, l *launcher.TestLauncher, taskID platform.ID) string {
	t.Helper()
	req := l.NewHTTPRequestOrFail(t, "POST", fmt.Sprintf("/api/v2/tasks/%s/runs", taskID), l.Auth.Token, "{}")
	var run taskRun
	require.NoError(t, json.Unmarshal(mustDoRequest(t, req, nethttp.StatusCreated), &run))
	return run.ID
}

// waitForRun waits for the run to finish and requires it to have succeeded.
func waitForRun(t *testing.T, l *launcher.TestLauncher, taskID platform.ID, runID string) {
	t.Helper()
	require.Equal(t, "success", waitForRunStatus(t, l, taskID, runID))
}

// waitForRunStatus waits for the run to finish and returns its final status.
//
// The executor sets a run's final status, then adds its last logs, and only
// then moves it from the KV store to the task system bucket. So the run counts
// as finished once it is no longer currently running and can be read back.
func waitForRunStatus(t *testing.T, l *launcher.TestLauncher, taskID platform.ID, runID string) string {
	t.Helper()
	kvRuns, ok := l.TaskServiceKV(t).(interface {
		CurrentlyRunning(ctx context.Context, taskID platform.ID) ([]*taskmodel.Run, error)
	})
	require.True(t, ok, "KV task service does not report currently running runs")

	// Poll without failing on errors: the condition runs on another goroutine,
	// and the run is briefly not found while it moves to the system bucket. On
	// timeout, the errors from the last poll are reported.
	var status string
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		running, err := kvRuns.CurrentlyRunning(ctx, taskID)
		if !assert.NoError(t, err) ||
			!assert.False(t, slices.ContainsFunc(running, func(r *taskmodel.Run) bool { return r.ID.String() == runID }), "run %s is still running", runID) {
			return
		}
		req, err := l.NewHTTPRequest("GET", fmt.Sprintf("/api/v2/tasks/%s/runs/%s", taskID, runID), l.Auth.Token, "")
		if !assert.NoError(t, err) {
			return
		}
		resp, err := nethttp.DefaultClient.Do(req)
		if !assert.NoError(t, err) {
			return
		}
		defer th.AssertCheckedClose(t, resp.Body)()
		body, err := io.ReadAll(resp.Body)
		if !assert.NoError(t, err) || !assert.Equal(t, nethttp.StatusOK, resp.StatusCode, string(body)) {
			return
		}
		var got taskRun
		if !assert.NoError(t, json.Unmarshal(body, &got), string(body)) {
			return
		}
		status = got.Status
		assert.Contains(t, []string{"success", "failed"}, status, "run %s has not finished", runID)
	}, 10*time.Second, 50*time.Millisecond)
	return status
}

// scheduleRun schedules a manual run of the task an hour from now and returns its ID.
func scheduleRun(t *testing.T, l *launcher.TestLauncher, taskID platform.ID) string {
	t.Helper()
	body := fmt.Sprintf(`{"scheduledFor": %q}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	req := l.NewHTTPRequestOrFail(t, "POST", fmt.Sprintf("/api/v2/tasks/%s/runs", taskID), l.Auth.Token, body)
	var run taskRun
	require.NoError(t, json.Unmarshal(mustDoRequest(t, req, nethttp.StatusCreated), &run))
	return run.ID
}

// TestLauncher_RestartWithTasks verifies that a server restarts with tasks in a
// variety of stored states. On startup the task executor loads the runs of
// every task, which queries each org's task system bucket by ID with no
// authorizer from a request.
func TestLauncher_RestartWithTasks(t *testing.T) {
	for _, tc := range []struct {
		name string
		// setup stores tasks and runs, and returns a check to run after the restart.
		setup   func(t *testing.T, l *launcher.TestLauncher) func(t *testing.T, l *launcher.TestLauncher)
		restart []launcher.OptSetter
	}{
		{
			name: "task without runs",
			setup: func(t *testing.T, l *launcher.TestLauncher) func(t *testing.T, l *launcher.TestLauncher) {
				task := createTask(t, l, l.Org.ID, restartTaskFlux)
				return func(t *testing.T, l *launcher.TestLauncher) {
					require.Empty(t, listRuns(t, l, l.Auth.Token, task.ID))
				}
			},
		},
		{
			name: "task with a completed run",
			setup: func(t *testing.T, l *launcher.TestLauncher) func(t *testing.T, l *launcher.TestLauncher) {
				task := createTask(t, l, l.Org.ID, restartTaskFlux)
				runID := runTaskNow(t, l, task.ID)
				return func(t *testing.T, l *launcher.TestLauncher) {
					require.Equal(t, []taskRun{{ID: runID, Status: "success"}}, listRuns(t, l, l.Auth.Token, task.ID))
				}
			},
		},
		{
			name: "task with a future scheduled run",
			setup: func(t *testing.T, l *launcher.TestLauncher) func(t *testing.T, l *launcher.TestLauncher) {
				task := createTask(t, l, l.Org.ID, restartTaskFlux)
				runID := scheduleRun(t, l, task.ID)
				return func(t *testing.T, l *launcher.TestLauncher) {
					// The executor marks a scheduled manual run as started while it
					// waits, so only check that the run is still there.
					runs := listRunsBefore(t, l, l.Auth.Token, task.ID, time.Now().Add(2*time.Hour))
					require.Len(t, runs, 1)
					require.Equal(t, runID, runs[0].ID)
				}
			},
		},
		{
			name: "inactive task",
			setup: func(t *testing.T, l *launcher.TestLauncher) func(t *testing.T, l *launcher.TestLauncher) {
				task := createTask(t, l, l.Org.ID, restartTaskFlux)
				runID := runTaskNow(t, l, task.ID)
				inactive := string(taskmodel.TaskInactive)
				_, err := l.TaskService(t).UpdateTask(ctx, task.ID, taskmodel.TaskUpdate{Status: &inactive})
				require.NoError(t, err)
				return func(t *testing.T, l *launcher.TestLauncher) {
					require.Equal(t, []taskRun{{ID: runID, Status: "success"}}, listRuns(t, l, l.Auth.Token, task.ID))
				}
			},
		},
		{
			// Each org has its own task system bucket.
			name: "tasks in multiple orgs",
			setup: func(t *testing.T, l *launcher.TestLauncher) func(t *testing.T, l *launcher.TestLauncher) {
				org := &influxdb.Organization{Name: "second-org"}
				require.NoError(t, l.OrgService(t).CreateOrganization(ctx, org))
				bucket := &influxdb.Bucket{OrgID: org.ID, Name: "BUCKET"}
				require.NoError(t, l.BucketService(t).CreateBucket(ctx, bucket))

				task1 := createTask(t, l, l.Org.ID, restartTaskFlux)
				run1 := runTaskNow(t, l, task1.ID)
				task2 := createTask(t, l, org.ID, restartTaskFlux)
				run2 := runTaskNow(t, l, task2.ID)
				return func(t *testing.T, l *launcher.TestLauncher) {
					require.Equal(t, []taskRun{{ID: run1, Status: "success"}}, listRuns(t, l, l.Auth.Token, task1.ID))
					require.Equal(t, []taskRun{{ID: run2, Status: "success"}}, listRuns(t, l, l.Auth.Token, task2.ID))
				}
			},
		},
		{
			// The task runs with its owner's permissions, which must be enough
			// to resolve buckets by ID for both reading and writing.
			name: "task reading and writing by bucket ID",
			setup: func(t *testing.T, l *launcher.TestLauncher) func(t *testing.T, l *launcher.TestLauncher) {
				target := &influxdb.Bucket{OrgID: l.Org.ID, Name: "target"}
				require.NoError(t, l.BucketService(t).CreateBucket(ctx, target))
				task := createTask(t, l, l.Org.ID, fmt.Sprintf(`option task = {name: "by-id", every: 1h}
from(bucketID: %q) |> range(start: -1h) |> to(bucketID: %q)`, l.Bucket.ID, target.ID))
				runID := runTaskNow(t, l, task.ID)
				return func(t *testing.T, l *launcher.TestLauncher) {
					require.Equal(t, []taskRun{{ID: runID, Status: "success"}}, listRuns(t, l, l.Auth.Token, task.ID))
				}
			},
		},
		{
			name: "task whose org was deleted",
			setup: func(t *testing.T, l *launcher.TestLauncher) func(t *testing.T, l *launcher.TestLauncher) {
				org := &influxdb.Organization{Name: "deleted-org"}
				require.NoError(t, l.OrgService(t).CreateOrganization(ctx, org))
				createTask(t, l, org.ID, restartTaskFlux)
				require.NoError(t, l.OrgService(t).DeleteOrganization(ctx, org.ID))
				return func(*testing.T, *launcher.TestLauncher) {}
			},
		},
		{
			// Loading a future run looks up the permissions of the task's owner.
			name: "future scheduled run of a task whose owner was deleted",
			setup: func(t *testing.T, l *launcher.TestLauncher) func(t *testing.T, l *launcher.TestLauncher) {
				var owner influxdb.User
				req := l.NewHTTPRequestOrFail(t, "POST", "/api/v2/users", l.Auth.Token, `{"name": "deleted-owner"}`)
				require.NoError(t, json.Unmarshal(mustDoRequest(t, req, nethttp.StatusCreated), &owner))

				// The tasks API makes the caller the owner, so create the task directly.
				kvCtx := icontext.SetAuthorizer(ctx, l.Auth)
				task, err := l.TaskServiceKV(t).CreateTask(kvCtx, taskmodel.TaskCreate{
					OrganizationID: l.Org.ID,
					OwnerID:        owner.ID,
					Flux:           restartTaskFlux,
				})
				require.NoError(t, err)
				_, err = l.TaskServiceKV(t).ForceRun(kvCtx, task.ID, time.Now().Add(time.Hour).Unix())
				require.NoError(t, err)

				req = l.NewHTTPRequestOrFail(t, "DELETE", "/api/v2/users/"+owner.ID.String(), l.Auth.Token, "")
				mustDoRequest(t, req, nethttp.StatusNoContent)
				return func(*testing.T, *launcher.TestLauncher) {}
			},
		},
		{
			// The executor loads existing runs even when the scheduler is disabled.
			name: "task with a completed run, restarted with tasks disabled",
			setup: func(t *testing.T, l *launcher.TestLauncher) func(t *testing.T, l *launcher.TestLauncher) {
				task := createTask(t, l, l.Org.ID, restartTaskFlux)
				runID := runTaskNow(t, l, task.ID)
				return func(t *testing.T, l *launcher.TestLauncher) {
					require.Equal(t, []taskRun{{ID: runID, Status: "success"}}, listRuns(t, l, l.Auth.Token, task.ID))
				}
			},
			restart: []launcher.OptSetter{func(o *launcher.InfluxdOpts) { o.NoTasks = true }},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := launcher.NewTestLauncherServer()
			l.RunOrFail(t, ctx)
			l.SetupOrFail(t)

			check := tc.setup(t, l)

			l = restartLauncher(t, l, tc.restart...)
			defer l.ShutdownOrFail(t, ctx)
			check(t, l)
		})
	}
}

// TestLauncher_Task_RunsWithTasksOnlyToken verifies that reading a task's runs
// and logs needs only read access to tasks, and retrying a run only write
// access. Finished runs are read from the task system bucket, which would
// otherwise require read access to the org.
func TestLauncher_Task_RunsWithTasksOnlyToken(t *testing.T) {
	l := launcher.NewTestLauncherServer()
	l.RunOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	task := createTask(t, l, l.Org.ID, restartTaskFlux)
	runID := runTaskNow(t, l, task.ID)

	mkToken := func(actions ...influxdb.Action) string {
		perms := make([]influxdb.Permission, 0, len(actions))
		for _, a := range actions {
			perms = append(perms, influxdb.Permission{
				Action:   a,
				Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &l.Org.ID},
			})
		}
		body, err := json.Marshal(map[string]interface{}{
			"status":      "active",
			"orgID":       l.Org.ID.String(),
			"permissions": perms,
		})
		require.NoError(t, err)
		var tok struct {
			Token string `json:"token"`
		}
		req := l.MustNewHTTPRequest("POST", "/api/v2/authorizations", string(body))
		require.NoError(t, json.Unmarshal(mustDoRequest(t, req, nethttp.StatusCreated), &tok))
		return tok.Token
	}
	readTasks := mkToken(influxdb.ReadAction)

	t.Run("list runs", func(t *testing.T) {
		require.Equal(t, []taskRun{{ID: runID, Status: "success"}}, listRuns(t, l, readTasks, task.ID))
	})
	t.Run("get run", func(t *testing.T) {
		require.Equal(t, taskRun{ID: runID, Status: "success"}, getRun(t, l, readTasks, task.ID, runID))
	})
	for _, path := range []string{
		fmt.Sprintf("/api/v2/tasks/%s/logs", task.ID),
		fmt.Sprintf("/api/v2/tasks/%s/runs/%s/logs", task.ID, runID),
	} {
		t.Run("get "+path, func(t *testing.T) {
			req := l.NewHTTPRequestOrFail(t, "GET", path, readTasks, "")
			var logs struct {
				Events []struct {
					RunID   string `json:"runID"`
					Message string `json:"message"`
				} `json:"events"`
			}
			require.NoError(t, json.Unmarshal(mustDoRequest(t, req, nethttp.StatusOK), &logs))
			msgs := make([]string, 0, len(logs.Events))
			for _, e := range logs.Events {
				require.Equal(t, runID, e.RunID, e.Message)
				msgs = append(msgs, e.Message)
			}
			// The executor logs the run's completion just before moving it to the
			// task system bucket, so this shows the full log was read from there.
			require.Contains(t, msgs, fmt.Sprintf("Started task from script: %q", restartTaskFlux))
			require.Contains(t, msgs, fmt.Sprintf("Completed(%s)", taskmodel.RunSuccess))
		})
	}
	t.Run("retry run", func(t *testing.T) {
		readWriteTasks := mkToken(influxdb.ReadAction, influxdb.WriteAction)
		req := l.NewHTTPRequestOrFail(t, "POST", fmt.Sprintf("/api/v2/tasks/%s/runs/%s/retry", task.ID, runID), readWriteTasks, "{}")
		var run taskRun
		require.NoError(t, json.Unmarshal(mustDoRequest(t, req, nethttp.StatusOK), &run))
		require.NotEqual(t, runID, run.ID)
		// Let the retried run finish before the server shuts down.
		waitForRun(t, l, task.ID, run.ID)
	})
}
