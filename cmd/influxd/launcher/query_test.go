package launcher_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	nethttp "net/http"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/influxdb/cmd/influxd/launcher"
	phttp "github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/query"
)

func TestPipeline_Write_Query_FieldKey(t *testing.T) {
	be := launcher.RunTestLauncherOrFail(t, ctx)
	be.SetupOrFail(t)
	defer be.ShutdownOrFail(t, ctx)

	resp, err := nethttp.DefaultClient.Do(
		be.MustNewHTTPRequest(
			"POST",
			fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", be.Org.ID, be.Bucket.ID),
			`cpu,region=west,server=a v0=1.2
cpu,region=west,server=b v0=33.2
cpu,region=east,server=b,area=z v1=100.0
disk,regions=north,server=b v1=101.2
mem,server=b value=45.2`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Error(err)
		}
	}()
	if resp.StatusCode != 204 {
		t.Fatal("failed call to write points")
	}

	rawQ := fmt.Sprintf(`from(bucket:"%s")
	|> filter(fn: (r) => r._measurement ==  "cpu" and (r._field == "v1" or r._field == "v0"))
	|> range(start:-1m)
	`, be.Bucket.Name)

	// Expected keys:
	//
	// _measurement=cpu,region=east,server=b,area=z,_field=v1
	// _measurement=cpu,region=west,server=a,_field=v0
	// _measurement=cpu,region=west,server=b,_field=v0
	//
	results := be.MustExecuteQuery(rawQ)
	defer results.Done()
	results.First(t).HasTablesWithCols([]int{5, 4, 4})
}

// This test initialises a default launcher writes some data,
// and checks that the queried results contain the expected number of tables
// and expected number of columns.
func TestPipeline_WriteV2_Query(t *testing.T) {
	be := launcher.RunTestLauncherOrFail(t, ctx)
	be.SetupOrFail(t)
	defer be.ShutdownOrFail(t, ctx)

	// The default gateway instance inserts some values directly such that ID lookups seem to break,
	// so go the roundabout way to insert things correctly.
	req := be.MustNewHTTPRequest(
		"POST",
		fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", be.Org.ID, be.Bucket.ID),
		fmt.Sprintf("ctr n=1i %d", time.Now().UnixNano()),
	)
	phttp.SetToken(be.Auth.Token, req)

	resp, err := nethttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Error(err)
		}
	}()

	if resp.StatusCode != nethttp.StatusNoContent {
		buf := new(bytes.Buffer)
		if _, err := io.Copy(buf, resp.Body); err != nil {
			t.Fatalf("Could not read body: %s", err)
		}
		t.Fatalf("exp status %d; got %d, body: %s", nethttp.StatusNoContent, resp.StatusCode, buf.String())
	}

	res := be.MustExecuteQuery(fmt.Sprintf(`from(bucket:"%s") |> range(start:-5m)`, be.Bucket.Name))
	defer res.Done()
	res.HasTableCount(t, 1)
}

// This test initializes a default launcher; writes some data; queries the data (success);
// sets memory limits to the same read query; checks that the query fails because limits are exceeded.
func TestPipeline_QueryMemoryLimits(t *testing.T) {
	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	// write some points
	for i := 0; i < 100; i++ {
		l.WritePointsOrFail(t, fmt.Sprintf(`m,k=v1 f=%di %d`, i*100, time.Now().UnixNano()))
	}

	// compile a from query and get the spec
	spec, err := flux.Compile(context.Background(), fmt.Sprintf(`from(bucket:"%s") |> range(start:-5m)`, l.Bucket.Name), time.Now())
	if err != nil {
		t.Fatal(err)
	}

	// we expect this request to succeed
	req := &query.Request{
		Authorization:  l.Auth,
		OrganizationID: l.Org.ID,
		Compiler: lang.SpecCompiler{
			Spec: spec,
		},
	}
	if err := l.QueryAndNopConsume(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	// ok, the first request went well, let's add memory limits:
	// this query should error.
	spec.Resources = flux.ResourceManagement{
		MemoryBytesQuota: 100,
	}

	if err := l.QueryAndNopConsume(context.Background(), req); err != nil {
		if !strings.Contains(err.Error(), "allocation limit reached") {
			t.Fatalf("query errored with unexpected error: %v", err)
		}
	} else {
		t.Fatal("expected error, got successful query execution")
	}
}
