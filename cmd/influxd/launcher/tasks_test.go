package launcher_test

import (
	"context"
	"fmt"
	nethttp "net/http"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute/executetest"
	platform "github.com/influxdata/influxdb"
	pctx "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/task/backend"
)

func TestLauncher_Task(t *testing.T) {
	be := RunLauncherOrFail(t, ctx)
	be.SetupOrFail(t)
	defer be.ShutdownOrFail(t, ctx)

	now := time.Now().Unix() // Need to track now at the start of the test, for a query later.
	org := be.Org

	bIn := &platform.Bucket{OrganizationID: org.ID, Organization: org.Name, Name: "my_bucket_in"}
	if err := be.BucketService().CreateBucket(context.Background(), bIn); err != nil {
		t.Fatal(err)
	}
	bOut := &platform.Bucket{OrganizationID: org.ID, Organization: org.Name, Name: "my_bucket_out"}
	if err := be.BucketService().CreateBucket(context.Background(), bOut); err != nil {
		t.Fatal(err)
	}
	u := be.User

	writeBIn, err := platform.NewPermissionAtID(bIn.ID, platform.WriteAction, platform.BucketsResourceType, org.ID)
	if err != nil {
		t.Fatal(err)
	}

	writeBOut, err := platform.NewPermissionAtID(bOut.ID, platform.WriteAction, platform.BucketsResourceType, org.ID)
	if err != nil {
		t.Fatal(err)
	}

	writeT, err := platform.NewPermission(platform.WriteAction, platform.TasksResourceType, org.ID)
	if err != nil {
		t.Fatal(err)
	}

	readT, err := platform.NewPermission(platform.ReadAction, platform.TasksResourceType, org.ID)
	if err != nil {
		t.Fatal(err)
	}

	ctx = pctx.SetAuthorizer(context.Background(), be.Auth)

	be.Auth = &platform.Authorization{UserID: u.ID, OrgID: org.ID, Permissions: []platform.Permission{*writeBIn, *writeBOut, *writeT, *readT}}
	if err := be.AuthorizationService().CreateAuthorization(context.Background(), be.Auth); err != nil {
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

	created := &platform.Task{
		OrganizationID: org.ID,
		Owner:          *be.User,
		Flux: fmt.Sprintf(`option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"my_bucket_in") |> range(start:-5m) |> to(bucket:"%s", org:"%s")`, bOut.Name, be.Org.Name),
	}
	if err := be.TaskService().CreateTask(ctx, created); err != nil {
		t.Fatal(err)
	}

	// Find the next due run of the task we just created, so that we can accurately tick the scheduler to it.
	m, err := be.TaskStore().FindTaskMetaByID(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	ndr, err := m.NextDueRun()
	if err != nil {
		t.Fatal(err)
	}

	be.TaskScheduler().(*backend.TickScheduler).Tick(ndr + 1)

	// Poll for the task to have started and finished.
	deadline := time.Now().Add(10 * time.Second) // Arbitrary deadline; 10s seems safe for -race on a resource-constrained system.
	ndrString := time.Unix(ndr, 0).UTC().Format(time.RFC3339)
	var targetRun platform.Run
	i := 0
	for {
		t.Logf("Looking for created run...")
		if time.Now().After(deadline) {
			t.Fatalf("didn't find completed run within deadline")
		}
		time.Sleep(5 * time.Millisecond)

		runs, _, err := be.TaskService().FindRuns(ctx, platform.RunFilter{Org: &org.ID, Task: &created.ID, Limit: 1})
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
			if r.ScheduledFor == ndrString {
				targetRun = *r
				break
			} else {
				t.Logf("Found run matching target schedule %s, but looking for %s", r.ScheduledFor, ndrString)
			}
		}

		if targetRun.ScheduledFor != ndrString {
			t.Logf("Didn't find scheduled run yet")
			continue
		}

		if targetRun.FinishedAt == "" {
			// Run exists but hasn't completed yet.
			t.Logf("Found target run, but not finished yet: %#v", targetRun)
			continue
		}

		break
	}

	// Explicitly set the now option so want and got have the same _start and _end values.
	nowOpt := fmt.Sprintf("option now = () => %s\n", time.Unix(now, 0).UTC().Format(time.RFC3339))

	res := be.MustExecuteQuery(org.ID, nowOpt+`from(bucket:"my_bucket_in") |> range(start:-5m)`, be.Auth)
	defer res.Done()
	if len(res.Results) < 1 {
		t.Fail()
	}

	want := make(map[string][]*executetest.Table) // Want the original.
	i = 0
	for k, v := range res.Results {
		i++
		if err := v.Tables().Do(func(tbl flux.Table) error {
			ct, err := executetest.ConvertTable(tbl)
			if err != nil {
				return err
			}
			want[k] = append(want[k], ct)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, w := range want {
		executetest.NormalizeTables(w)
	}
	res = be.MustExecuteQuery(org.ID, nowOpt+`from(bucket:"my_bucket_out") |> range(start:-5m)`, be.Auth)
	defer res.Done()
	got := make(map[string][]*executetest.Table)
	for k, v := range res.Results {
		if err := v.Tables().Do(func(tbl flux.Table) error {
			ct, err := executetest.ConvertTable(tbl)
			if err != nil {
				return err
			}
			got[k] = append(got[k], ct)
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
		t.Skip("FindRunByID isn't returning the log")
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
	logs, _, err := be.TaskService().FindLogs(ctx, platform.LogFilter{Org: &org.ID, Task: &created.ID, Run: &targetRun.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected 1 log for run, got %d", len(logs))
	}
}
