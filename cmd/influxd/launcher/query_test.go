package launcher_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb"
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
	|> range(start:-1m)
	|> filter(fn: (r) => r._measurement ==  "cpu" and (r._field == "v1" or r._field == "v0"))
	|> group(columns:["_time", "_value"], mode:"except")
	`, be.Bucket.Name)

	// Expected keys:
	//
	// _measurement=cpu,region=west,server=a,_field=v0
	// _measurement=cpu,region=west,server=b,_field=v0
	// _measurement=cpu,region=east,server=b,area=z,_field=v1
	//
	results := be.MustExecuteQuery(rawQ)
	defer results.Done()
	results.First(t).HasTablesWithCols([]int{4, 4, 5})
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
	t.Skip("setting memory limits in the client is not implemented yet")

	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	// write some points
	for i := 0; i < 100; i++ {
		l.WritePointsOrFail(t, fmt.Sprintf(`m,k=v1 f=%di %d`, i*100, time.Now().UnixNano()))
	}

	// compile a from query and get the spec
	qs := fmt.Sprintf(`from(bucket:"%s") |> range(start:-5m)`, l.Bucket.Name)
	pkg, err := flux.Parse(qs)
	if err != nil {
		t.Fatal(err)
	}

	// we expect this request to succeed
	req := &query.Request{
		Authorization:  l.Auth,
		OrganizationID: l.Org.ID,
		Compiler: lang.ASTCompiler{
			AST: pkg,
		},
	}
	if err := l.QueryAndNopConsume(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	// ok, the first request went well, let's add memory limits:
	// this query should error.
	// spec.Resources = flux.ResourceManagement{
	// 	MemoryBytesQuota: 100,
	// }

	if err := l.QueryAndNopConsume(context.Background(), req); err != nil {
		if !strings.Contains(err.Error(), "allocation limit reached") {
			t.Fatalf("query errored with unexpected error: %v", err)
		}
	} else {
		t.Fatal("expected error, got successful query execution")
	}
}

func TestPipeline_Query_LoadSecret_Success(t *testing.T) {
	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	const key, value = "mytoken", "secrettoken"
	if err := l.SecretService().PutSecret(ctx, l.Org.ID, key, value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// write one point so we can use it
	l.WritePointsOrFail(t, fmt.Sprintf(`m,k=v1 f=%di %d`, 0, time.Now().UnixNano()))

	// we expect this request to succeed
	req := &query.Request{
		Authorization:  l.Auth,
		OrganizationID: l.Org.ID,
		Compiler: lang.FluxCompiler{
			Query: fmt.Sprintf(`
import "influxdata/influxdb/secrets"

token = secrets.get(key: "mytoken")
from(bucket: "%s")
	|> range(start: -5m)
	|> set(key: "token", value: token)
`, l.Bucket.Name),
		},
	}
	if err := l.QueryAndConsume(ctx, req, func(r flux.Result) error {
		return r.Tables().Do(func(tbl flux.Table) error {
			return tbl.Do(func(cr flux.ColReader) error {
				j := execute.ColIdx("token", cr.Cols())
				if j == -1 {
					return errors.New("cannot find table column \"token\"")
				}

				for i := 0; i < cr.Len(); i++ {
					v := execute.ValueForRow(cr, i, j)
					if got, want := v, values.NewString("secrettoken"); !got.Equal(want) {
						t.Errorf("unexpected value at row %d -want/+got:\n\t- %v\n\t+ %v", i, got, want)
					}
				}
				return nil
			})
		})
	}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestPipeline_Query_LoadSecret_Forbidden(t *testing.T) {
	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	const key, value = "mytoken", "secrettoken"
	if err := l.SecretService().PutSecret(ctx, l.Org.ID, key, value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// write one point so we can use it
	l.WritePointsOrFail(t, fmt.Sprintf(`m,k=v1 f=%di %d`, 0, time.Now().UnixNano()))

	auth := &influxdb.Authorization{
		OrgID:  l.Org.ID,
		UserID: l.User.ID,
		Permissions: []influxdb.Permission{
			{
				Action: influxdb.ReadAction,
				Resource: influxdb.Resource{
					Type:  influxdb.BucketsResourceType,
					ID:    &l.Bucket.ID,
					OrgID: &l.Org.ID,
				},
			},
		},
	}
	if err := l.AuthorizationService(t).CreateAuthorization(ctx, auth); err != nil {
		t.Fatalf("unexpected error creating authorization: %s", err)
	}
	l.Auth = auth

	// we expect this request to succeed
	req := &query.Request{
		Authorization:  l.Auth,
		OrganizationID: l.Org.ID,
		Compiler: lang.FluxCompiler{
			Query: fmt.Sprintf(`
import "influxdata/influxdb/secrets"

token = secrets.get(key: "mytoken")
from(bucket: "%s")
	|> range(start: -5m)
	|> set(key: "token", value: token)
`, l.Bucket.Name),
		},
	}
	if err := l.QueryAndNopConsume(ctx, req); err == nil {
		t.Error("expected error")
	} else if got, want := influxdb.ErrorCode(err), influxdb.EUnauthorized; got != want {
		t.Errorf("unexpected error code -want/+got:\n\t- %v\n\t+ %v", got, want)
	}
}

// We need a separate test for dynamic queries because our Flux e2e tests cannot test them now.
// Indeed, tableFind would fail while initializing the data in the input bucket, because the data is not
// written, and tableFind would complain not finding the tables.
// This will change once we make side effects drive execution and remove from/to concurrency in our e2e tests.
// See https://github.com/influxdata/flux/issues/1799.
func TestPipeline_DynamicQuery(t *testing.T) {
	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	l.WritePointsOrFail(t, `
m0,k=k0 f=0i 0
m0,k=k0 f=1i 1
m0,k=k0 f=2i 2
m0,k=k0 f=3i 3
m0,k=k0 f=4i 4
m0,k=k1 f=5i 5
m0,k=k1 f=6i 6
m1,k=k0 f=5i 7
m1,k=k2 f=0i 8
m1,k=k0 f=6i 9
m1,k=k1 f=6i 10
m1,k=k0 f=7i 11
m1,k=k0 f=5i 12
m1,k=k1 f=8i 13
m1,k=k2 f=9i 14
m1,k=k3 f=5i 15`)

	// How many points do we have in stream2 with the same values of the ones in the table with key k0 in stream1?
	// The only point matching the description is `m1,k=k2 f=0i 8`, because its value is in the set [0, 1, 2, 3, 4].
	dq := fmt.Sprintf(`
stream1 = from(bucket: "%s") |> range(start: 0) |> filter(fn: (r) => r._measurement == "m0" and r._field == "f")
stream2 = from(bucket: "%s") |> range(start: 0) |> filter(fn: (r) => r._measurement == "m1" and r._field == "f")
col = stream1 |> tableFind(fn: (key) => key.k == "k0") |> getColumn(column: "_value")
// Here is where dynamicity kicks in.
stream2 |> filter(fn: (r) => contains(value: r._value, set: col)) |> group() |> count() |> yield(name: "dynamic")`,
		l.Bucket.Name, l.Bucket.Name)
	req := &query.Request{
		Authorization:  l.Auth,
		OrganizationID: l.Org.ID,
		Compiler:       lang.FluxCompiler{Query: dq},
	}
	noRes := 0
	if err := l.QueryAndConsume(ctx, req, func(r flux.Result) error {
		noRes++
		if n := r.Name(); n != "dynamic" {
			t.Fatalf("got unexpected result: %s", n)
		}
		noTables := 0
		if err := r.Tables().Do(func(tbl flux.Table) error {
			return tbl.Do(func(cr flux.ColReader) error {
				noTables++
				j := execute.ColIdx("_value", cr.Cols())
				if j == -1 {
					return errors.New("cannot find table column \"_value\"")
				}
				if want := 1; cr.Len() != want {
					t.Fatalf("wrong number of rows in table: -want/+got:\n\t- %d\n\t+ %d", want, cr.Len())
				}
				v := execute.ValueForRow(cr, 0, j)
				if got, want := v, values.NewInt(1); !got.Equal(want) {
					t.Errorf("unexpected value at row %d -want/+got:\n\t- %v\n\t+ %v", 0, want, got)
				}
				return nil
			})
		}); err != nil {
			return err
		}
		if want := 1; noTables != want {
			t.Fatalf("wrong number of tables in result: -want/+got:\n\t- %d\n\t+ %d", want, noRes)
		}
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if want := 1; noRes != want {
		t.Fatalf("wrong number of results: -want/+got:\n\t- %d\n\t+ %d", want, noRes)
	}
}

func TestPipeline_Query_ExperimentalTo(t *testing.T) {
	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	// Last row of data tests nil field value
	data := `
#datatype,string,long,dateTime:RFC3339,double,string,string,string,string
#group,false,false,false,false,true,true,true,true
#default,_result,,,,,,,
,result,table,_time,_value,_field,_measurement,cpu,host
,,0,2018-05-22T19:53:26Z,1.0,usage_guest,cpu,cpu-total,host.local
,,0,2018-05-22T19:53:36Z,1.1,usage_guest,cpu,cpu-total,host.local
,,1,2018-05-22T19:53:26Z,2.0,usage_guest_nice,cpu,cpu-total,host.local
,,1,2018-05-22T19:53:36Z,2.1,usage_guest_nice,cpu,cpu-total,host.local
,,2,2018-05-22T19:53:26Z,91.7364670583823,usage_idle,cpu,cpu-total,host.local
,,2,2018-05-22T19:53:36Z,89.51118889861233,usage_idle,cpu,cpu-total,host.local
,,3,2018-05-22T19:53:26Z,3.0,usage_iowait,cpu,cpu-total,host.local
,,3,2018-05-22T19:53:36Z,,usage_iowait,cpu,cpu-total,host.local
`
	pivotQuery := fmt.Sprintf(`
import "csv"
import "experimental"
import "influxdata/influxdb/v1"
csv.from(csv: "%s")
    |> range(start: 2018-05-21T00:00:00Z, stop: 2018-05-23T00:00:00Z)
    |> v1.fieldsAsCols()
`, data)
	res := l.MustExecuteQuery(pivotQuery)
	defer res.Done()
	pivotedResultIterator := flux.NewSliceResultIterator(res.Results)

	toQuery := pivotQuery + fmt.Sprintf(`|> experimental.to(bucket: "%s", org: "%s") |> yield(name: "_result")`,
		l.Bucket.Name, l.Org.Name)
	res = l.MustExecuteQuery(toQuery)
	defer res.Done()
	toOutputResultIterator := flux.NewSliceResultIterator(res.Results)

	// Make sure that experimental.to() echoes its input to its output
	if err := executetest.EqualResultIterators(pivotedResultIterator, toOutputResultIterator); err != nil {
		t.Fatal(err)
	}

	csvQuery := fmt.Sprintf(`
import "csv"
csv.from(csv: "%s")
  |> filter(fn: (r) => exists r._value)
`,
		data)
	res = l.MustExecuteQuery(csvQuery)
	defer res.Done()
	csvResultIterator := flux.NewSliceResultIterator(res.Results)

	fromQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: 2018-05-15T00:00:00Z, stop: 2018-06-01T00:00:00Z)
  |> drop(columns: ["_start", "_stop"])
`,
		l.Bucket.Name)
	res = l.MustExecuteQuery(fromQuery)
	defer res.Done()
	fromResultIterator := flux.NewSliceResultIterator(res.Results)

	// Make sure that the data we stored matches the CSV
	if err := executetest.EqualResultIterators(csvResultIterator, fromResultIterator); err != nil {
		t.Fatal(err)
	}
}
