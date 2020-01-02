package launcher_test

import (
	"context"
	"encoding/json"
	"fmt"
	nethttp "net/http"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influxd/launcher"
	pctx "github.com/influxdata/influxdb/context"
)

func TestLauncher_Task(t *testing.T) {
	t.Skip("https://github.com/influxdata/influxdb/issues/13867")
	be := launcher.RunTestLauncherOrFail(t, ctx)
	be.SetupOrFail(t)
	defer be.ShutdownOrFail(t, ctx)

	now := time.Now().Unix() // Need to track now at the start of the test, for a query later.
	org := be.Org

	bIn := &influxdb.Bucket{OrgID: org.ID, Name: "my_bucket_in"}
	if err := be.BucketService(t).CreateBucket(context.Background(), bIn); err != nil {
		t.Fatal(err)
	}
	bOut := &influxdb.Bucket{OrgID: org.ID, Name: "my_bucket_out"}
	if err := be.BucketService(t).CreateBucket(context.Background(), bOut); err != nil {
		t.Fatal(err)
	}
	u := be.User

	writeBIn, err := influxdb.NewPermissionAtID(bIn.ID, influxdb.WriteAction, influxdb.BucketsResourceType, org.ID)
	if err != nil {
		t.Fatal(err)
	}

	writeBOut, err := influxdb.NewPermissionAtID(bOut.ID, influxdb.WriteAction, influxdb.BucketsResourceType, org.ID)
	if err != nil {
		t.Fatal(err)
	}

	writeT, err := influxdb.NewPermission(influxdb.WriteAction, influxdb.TasksResourceType, org.ID)
	if err != nil {
		t.Fatal(err)
	}

	readT, err := influxdb.NewPermission(influxdb.ReadAction, influxdb.TasksResourceType, org.ID)
	if err != nil {
		t.Fatal(err)
	}

	ctx = pctx.SetAuthorizer(context.Background(), be.Auth)

	a := &influxdb.Authorization{UserID: u.ID, OrgID: org.ID, Permissions: []influxdb.Permission{*writeBIn, *writeBOut, *writeT, *readT}}
	if err := be.AuthorizationService(t).CreateAuthorization(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if !be.Org.ID.Valid() {
		t.Fatal("invalid org id")
	}

	resp, err := nethttp.DefaultClient.Do(
		be.MustNewHTTPRequest(
			"POST",
			fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", be.Org.ID, bIn.ID), `
ctr,num=one n=1i
# Different series, same measurement.
ctr,num=two n=2i
# Other measurement, multiple values.
stuff f=-123.456,b=true,s="hello"
`))
	if err != nil {
		t.Fatal(err)
	}

	if err = resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != nethttp.StatusNoContent {
		t.Fatalf("exp status %d; got %d", nethttp.StatusNoContent, resp.StatusCode)
	}

	create := influxdb.TaskCreate{
		OrganizationID: org.ID,
		Flux: fmt.Sprintf(`option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"my_bucket_in") |> range(start:-5m) |> to(bucket:"%s", org:"%s")`, bOut.Name, be.Org.Name),
	}

	created, err := be.TaskService().CreateTask(ctx, create)
	if err != nil {
		t.Fatal(err)
	}

	// Poll for the task to have started and finished.
	deadline := time.Now().Add(10 * time.Second) // Arbitrary deadline; 10s seems safe for -race on a resource-constrained system.
	var targetRun influxdb.Run
	i := 0
	for {
		t.Logf("Looking for created run...")
		if time.Now().After(deadline) {
			t.Fatalf("didn't find completed run within deadline")
		}
		time.Sleep(5 * time.Millisecond)

		runs, _, err := be.TaskService().FindRuns(ctx, influxdb.RunFilter{Task: created.ID, Limit: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(runs) == 0 {
			// Run hasn't started yet.
			t.Log("No runs found yet...")
			continue
		}
		i++
		for _, r := range runs {
			if r.ScheduledFor.IsZero() {
				targetRun = *r
				break
			} else {
				t.Logf("Found run matching target schedule %s, but expected empty", r.ScheduledFor)
			}
		}

		if !targetRun.ScheduledFor.IsZero() {
			t.Logf("Didn't find scheduled run yet")
			continue
		}

		if targetRun.FinishedAt.IsZero() {
			// Run exists but hasn't completed yet.
			t.Logf("Found target run, but not finished yet: %#v", targetRun)
			continue
		}

		break
	}

	// Explicitly set the now option so want and got have the same _start and _end values.
	nowOpt := fmt.Sprintf("option now = () => %s\n", time.Unix(now, 0).UTC().Format(time.RFC3339))

	res := be.MustExecuteQuery(nowOpt + `from(bucket:"my_bucket_in") |> range(start:-5m)`)
	defer res.Done()
	if len(res.Results) < 1 {
		t.Fail()
	}

	want := make(map[string][]*executetest.Table) // Want the original.
	i = 0
	for _, r := range res.Results {
		i++
		if err := r.Tables().Do(func(tbl flux.Table) error {
			ct, err := executetest.ConvertTable(tbl)
			if err != nil {
				return err
			}
			want[r.Name()] = append(want[r.Name()], ct)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, w := range want {
		executetest.NormalizeTables(w)
	}
	res = be.MustExecuteQuery(nowOpt + `from(bucket:"my_bucket_out") |> range(start:-5m)`)
	defer res.Done()
	got := make(map[string][]*executetest.Table)
	for _, r := range res.Results {
		if err := r.Tables().Do(func(tbl flux.Table) error {
			ct, err := executetest.ConvertTable(tbl)
			if err != nil {
				return err
			}
			got[r.Name()] = append(got[r.Name()], ct)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, g := range got {
		executetest.NormalizeTables(g)
	}

	if !cmp.Equal(got, want) {
		t.Fatal("unexpected task result -want/+got", cmp.Diff(want, got))
	}

	t.Run("showrun", func(t *testing.T) {
		// do a show run!
		showRun, err := be.TaskService().FindRunByID(ctx, created.ID, targetRun.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !cmp.Equal(targetRun, *showRun) {
			t.Fatal("unexpected task result -want/+got\n", cmp.Diff(targetRun, showRun))
		}
	})

	// now lets see a logs
	logs, _, err := be.TaskService().FindLogs(ctx, influxdb.LogFilter{Task: created.ID, Run: &targetRun.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) < 1 {
		t.Fatalf("expected logs for run, got %d", len(logs))
	}

	// One of the log lines must be query statistics.
	// For now, assume it's the first line that begins with "{" (beginning of a JSON object).
	// That might change in the future.
	var statJSON string
	for _, log := range logs {
		if len(log.Message) > 0 && log.Message[0] == '{' {
			statJSON = log.Message
			break
		}
	}
	if statJSON == "" {
		t.Fatalf("no stats JSON found in run logs")
	}

	var stats flux.Statistics
	if err := json.Unmarshal([]byte(statJSON), &stats); err != nil {
		t.Fatal(err)
	}

	if reflect.DeepEqual(stats, flux.Statistics{}) {
		t.Fatalf("unmarshalled query statistics are zero; they should be non-zero. JSON: %s", statJSON)
	}
}
