package launcher_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"math/rand"
	nethttp "net/http"
	"strings"
	"sync"
	"testing"
	"time"

	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/execute/table"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	phttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/query"
	"go.uber.org/zap"
)

func TestLauncher_Write_Query_FieldKey(t *testing.T) {
	be := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
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
func TestLauncher_WriteV2_Query(t *testing.T) {
	be := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
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

func getMemoryUnused(t *testing.T, reg *prom.Registry) int64 {
	t.Helper()

	ms, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range ms {
		if m.GetName() == "qc_memory_unused_bytes" {
			return int64(*m.GetMetric()[0].Gauge.Value)
		}
	}
	t.Errorf("query metric for unused memory not found")
	return 0
}

//lint:ignore U1000 erroneously flagged by staticcheck since it is used in skipped tests
func checkMemoryUsed(t *testing.T, l *launcher.TestLauncher, concurrency, initial int) {
	t.Helper()

	got := l.QueryController().GetUsedMemoryBytes()
	// base memory used is equal to initial memory bytes * concurrency.
	if want := int64(concurrency * initial); want != got {
		t.Errorf("expected used memory %d, got %d", want, got)
	}
}

func writeBytes(t *testing.T, l *launcher.TestLauncher, tagValue string, bs int) int {
	// When represented in Flux, every point is:
	//    1 byte _measurement ("m")
	//	+ 1 byte _field ("f")
	//  + 8 bytes _value
	//  + len(tagValue) bytes
	//  + 8 bytes _time
	//  + 8 bytes _start
	//  + 8 bytes _stop
	//  ---------------------------
	//  = 34 + len(tag) bytes
	pointSize := 34 + len(tagValue)
	if bs < pointSize {
		bs = pointSize
	}
	n := bs / pointSize
	if n*pointSize < bs {
		n++
	}
	sb := strings.Builder{}
	for i := 0; i < n; i++ {
		sb.WriteString(fmt.Sprintf(`m,t=%s f=%di %d`, tagValue, i*100, time.Now().UnixNano()))
		sb.WriteRune('\n')
	}
	l.WritePointsOrFail(t, sb.String())
	return n * pointSize
}

type data struct {
	Bucket   string
	TagValue string
	Sleep    string
	verbose  bool
}

type queryOption func(d *data)

func withTagValue(tv string) queryOption {
	return func(d *data) {
		d.TagValue = tv
	}
}

func withSleep(s time.Duration) queryOption {
	return func(d *data) {
		d.Sleep = flux.ConvertDuration(s).String()
	}
}

func queryPoints(ctx context.Context, t *testing.T, l *launcher.TestLauncher, opts ...queryOption) error {
	d := &data{
		Bucket: l.Bucket.Name,
	}
	for _, opt := range opts {
		opt(d)
	}
	tmpls := `from(bucket: "{{ .Bucket }}")
	|> range(start:-5m)
	{{- if .TagValue }}
	// this must be pushed down to avoid unnecessary memory allocations.
	|> filter(fn: (r) => r.t == "{{ .TagValue }}")
	{{- end}}
	// ensure we load everything into memory.
	|> sort(columns: ["_time"])
	{{- if .Sleep }}
	// now that you have everything in memory, you can sleep.
	|> sleep(duration: {{ .Sleep }})
	{{- end}}`
	tmpl, err := template.New("test-query").Parse(tmpls)
	if err != nil {
		return err
	}
	bs := new(bytes.Buffer)
	if err := tmpl.Execute(bs, d); err != nil {
		return err
	}
	qs := bs.String()
	if d.verbose {
		t.Logf("query:\n%s", qs)
	}
	pkg, err := runtime.ParseToJSON(qs)
	if err != nil {
		t.Fatal(err)
	}
	req := &query.Request{
		Authorization:  l.Auth,
		OrganizationID: l.Org.ID,
		Compiler: lang.ASTCompiler{
			AST: pkg,
		},
	}
	return l.QueryAndNopConsume(ctx, req)
}

// This test:
//  - initializes a default launcher and sets memory limits;
//  - writes some data;
//  - queries the data;
//  - verifies that the query fails (or not) and that the memory was de-allocated.
func TestLauncher_QueryMemoryLimits(t *testing.T) {
	tcs := []struct {
		name           string
		setOpts        launcher.OptSetter
		err            bool
		querySizeBytes int
		// max_memory - per_query_memory * concurrency
		unusedMemoryBytes int
	}{
		{
			name: "ok - initial memory bytes, memory bytes, and max memory set",
			setOpts: func(o *launcher.InfluxdOpts) {
				o.ConcurrencyQuota = 1
				o.QueueSize = 1
				o.InitialMemoryBytesQuotaPerQuery = 100
				o.MaxMemoryBytes = 1048576 // 1MB
			},
			querySizeBytes:    30000,
			err:               false,
			unusedMemoryBytes: 1048476,
		},
		{
			name: "error - memory bytes and max memory set",
			setOpts: func(o *launcher.InfluxdOpts) {
				o.ConcurrencyQuota = 1
				o.QueueSize = 1
				o.MemoryBytesQuotaPerQuery = 1
				o.MaxMemoryBytes = 100
			},
			querySizeBytes:    2,
			err:               true,
			unusedMemoryBytes: 99,
		},
		{
			name: "error - initial memory bytes and max memory set",
			setOpts: func(o *launcher.InfluxdOpts) {
				o.ConcurrencyQuota = 1
				o.QueueSize = 1
				o.InitialMemoryBytesQuotaPerQuery = 1
				o.MaxMemoryBytes = 100
			},
			querySizeBytes:    101,
			err:               true,
			unusedMemoryBytes: 99,
		},
		{
			name: "error - initial memory bytes, memory bytes, and max memory set",
			setOpts: func(o *launcher.InfluxdOpts) {
				o.ConcurrencyQuota = 1
				o.QueueSize = 1
				o.InitialMemoryBytesQuotaPerQuery = 1
				o.MemoryBytesQuotaPerQuery = 50
				o.MaxMemoryBytes = 100
			},
			querySizeBytes:    51,
			err:               true,
			unusedMemoryBytes: 99,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			l := launcher.RunAndSetupNewLauncherOrFail(ctx, t, tc.setOpts)
			defer l.ShutdownOrFail(t, ctx)

			const tagValue = "t0"
			writeBytes(t, l, tagValue, tc.querySizeBytes)
			if err := queryPoints(context.Background(), t, l, withTagValue(tagValue)); err != nil {
				if tc.err {
					if !strings.Contains(err.Error(), "allocation limit reached") {
						t.Errorf("query errored with unexpected error: %v", err)
					}
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			} else if tc.err {
				t.Errorf("expected error, got successful query execution")
			}

			reg := l.Registry()
			got := getMemoryUnused(t, reg)
			want := int64(tc.unusedMemoryBytes)
			if want != got {
				t.Errorf("expected unused memory %d, got %d", want, got)
			}
		})
	}
}

// This test:
//  - initializes a default launcher and sets memory limits;
//  - writes some data;
//  - launches a query that does not error;
//  - launches a query that gets canceled while executing;
//  - launches a query that does not error;
//  - verifies after each query run the used memory.
func TestLauncher_QueryMemoryManager_ExceedMemory(t *testing.T) {
	t.Skip("this test is flaky, occasionally get error: \"memory allocation limit reached\" on OK query")

	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t, func(o *launcher.InfluxdOpts) {
		o.LogLevel = zap.ErrorLevel
		o.ConcurrencyQuota = 1
		o.InitialMemoryBytesQuotaPerQuery = 100
		o.MemoryBytesQuotaPerQuery = 50000
		o.MaxMemoryBytes = 200000
	})
	defer l.ShutdownOrFail(t, ctx)

	// One tag does not exceed memory.
	const tOK = "t0"
	writeBytes(t, l, tOK, 10000)
	// The other does.
	const tKO = "t1"
	writeBytes(t, l, tKO, 50001)

	if err := queryPoints(context.Background(), t, l, withTagValue(tOK)); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	checkMemoryUsed(t, l, 1, 100)
	if err := queryPoints(context.Background(), t, l, withTagValue(tKO)); err != nil {
		if !strings.Contains(err.Error(), "allocation limit reached") {
			t.Errorf("query errored with unexpected error: %v", err)
		}
	} else {
		t.Errorf("unexpected error: %v", err)
	}
	checkMemoryUsed(t, l, 1, 100)
	if err := queryPoints(context.Background(), t, l, withTagValue(tOK)); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	checkMemoryUsed(t, l, 1, 100)
}

// This test:
//  - initializes a default launcher and sets memory limits;
//  - writes some data;
//  - launches a query that does not error;
//  - launches a query and cancels its context;
//  - launches a query that does not error;
//  - verifies after each query run the used memory.
func TestLauncher_QueryMemoryManager_ContextCanceled(t *testing.T) {
	t.Skip("this test is flaky, occasionally get error: \"memory allocation limit reached\"")

	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t, func(o *launcher.InfluxdOpts) {
		o.LogLevel = zap.ErrorLevel
		o.ConcurrencyQuota = 1
		o.InitialMemoryBytesQuotaPerQuery = 100
		o.MemoryBytesQuotaPerQuery = 50000
		o.MaxMemoryBytes = 200000
	})
	defer l.ShutdownOrFail(t, ctx)

	const tag = "t0"
	writeBytes(t, l, tag, 10000)

	if err := queryPoints(context.Background(), t, l, withTagValue(tag)); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	checkMemoryUsed(t, l, 1, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := queryPoints(ctx, t, l, withSleep(4*time.Second)); err == nil {
		t.Errorf("expected error got none")
	}
	checkMemoryUsed(t, l, 1, 100)
	if err := queryPoints(context.Background(), t, l, withTagValue(tag)); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	checkMemoryUsed(t, l, 1, 100)
}

// This test:
//  - initializes a default launcher and sets memory limits;
//  - writes some data;
//  - launches (concurrently) a mixture of
//    - OK queries;
//    - queries that exceed the memory limit;
//    - queries that get canceled;
//  - verifies the used memory.
// Concurrency limit is set to 1, so only 1 query runs at a time and the others are queued.
// OK queries do not overcome the soft limit, so that they can run concurrently with the ones that exceed limits.
// The aim of this test is to verify that memory tracking works properly in the controller,
// even in the case of concurrent/queued queries.
func TestLauncher_QueryMemoryManager_ConcurrentQueries(t *testing.T) {
	t.Skip("this test is flaky, occasionally get error: \"dial tcp 127.0.0.1:59654: connect: connection reset by peer\"")

	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t, func(o *launcher.InfluxdOpts) {
		o.LogLevel = zap.ErrorLevel
		o.QueueSize = 1024
		o.ConcurrencyQuota = 1
		o.InitialMemoryBytesQuotaPerQuery = 10000
		o.MemoryBytesQuotaPerQuery = 50000
		o.MaxMemoryBytes = 200000
	})
	defer l.ShutdownOrFail(t, ctx)

	// One tag does not exceed memory.
	// The size is below the soft limit, so that querying this bucket never fail.
	const tSmall = "t0"
	writeBytes(t, l, tSmall, 9000)
	// The other exceeds memory per query.
	const tBig = "t1"
	writeBytes(t, l, tBig, 100000)

	const nOK = 100
	const nMemExceeded = 100
	const nContextCanceled = 100
	nTotalQueries := nOK + nMemExceeded + nContextCanceled

	// In order to increase the variety of the load, store and shuffle queries.
	qs := make([]func(), 0, nTotalQueries)
	// Flock of OK queries.
	for i := 0; i < nOK; i++ {
		qs = append(qs, func() {
			if err := queryPoints(context.Background(), t, l, withTagValue(tSmall)); err != nil {
				t.Errorf("unexpected error (ok-query %d): %v", i, err)
			}
		})
	}
	// Flock of big queries.
	for i := 0; i < nMemExceeded; i++ {
		qs = append(qs, func() {
			if err := queryPoints(context.Background(), t, l, withTagValue(tBig)); err == nil {
				t.Errorf("expected error got none (high-memory-query %d)", i)
			} else if !strings.Contains(err.Error(), "allocation limit reached") {
				t.Errorf("got wrong error (high-memory-query %d): %v", i, err)
			}
		})
	}
	// Flock of context canceled queries.
	for i := 0; i < nContextCanceled; i++ {
		qs = append(qs, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := queryPoints(ctx, t, l, withTagValue(tSmall), withSleep(4*time.Second)); err == nil {
				t.Errorf("expected error got none (context-canceled-query %d)", i)
			} else if !strings.Contains(err.Error(), "context") {
				t.Errorf("got wrong error (context-canceled-query %d): %v", i, err)
			}
		})
	}
	rand.Shuffle(len(qs), func(i, j int) { qs[i], qs[j] = qs[j], qs[i] })

	wg := sync.WaitGroup{}
	wg.Add(nTotalQueries)
	for i, q := range qs {
		qs[i] = func() {
			defer wg.Done()
			q()
		}
	}
	for _, q := range qs {
		go q()
	}
	wg.Wait()
	checkMemoryUsed(t, l, 1, 10000)
}

func TestLauncher_Query_LoadSecret_Success(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
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

func TestLauncher_Query_LoadSecret_Forbidden(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
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
	} else if got, want := errors2.ErrorCode(err), errors2.EUnauthorized; got != want {
		t.Errorf("unexpected error code -want/+got:\n\t- %v\n\t+ %v", got, want)
	}
}

// We need a separate test for dynamic queries because our Flux e2e tests cannot test them now.
// Indeed, tableFind would fail while initializing the data in the input bucket, because the data is not
// written, and tableFind would complain not finding the tables.
// This will change once we make side effects drive execution and remove from/to concurrency in our e2e tests.
// See https://github.com/influxdata/flux/issues/1799.
func TestLauncher_DynamicQuery(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
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

func TestLauncher_Query_ExperimentalTo(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
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

type TestQueryProfiler struct {
	start int64
}

func (s TestQueryProfiler) Name() string {
	return fmt.Sprintf("query%d", s.start)
}

func (s TestQueryProfiler) GetSortedResult(q flux.Query, alloc *memory.Allocator, desc bool, sortKeys ...string) (flux.Table, error) {
	return nil, nil
}

func (s TestQueryProfiler) GetResult(q flux.Query, alloc *memory.Allocator) (flux.Table, error) {
	groupKey := execute.NewGroupKey(
		[]flux.ColMeta{
			{
				Label: "_measurement",
				Type:  flux.TString,
			},
		},
		[]values.Value{
			values.NewString(fmt.Sprintf("profiler/query%d", s.start)),
		},
	)
	b := execute.NewColListTableBuilder(groupKey, alloc)
	colMeta := []flux.ColMeta{
		{
			Label: "_measurement",
			Type:  flux.TString,
		},
		{
			Label: "TotalDuration",
			Type:  flux.TInt,
		},
		{
			Label: "CompileDuration",
			Type:  flux.TInt,
		},
		{
			Label: "QueueDuration",
			Type:  flux.TInt,
		},
		{
			Label: "PlanDuration",
			Type:  flux.TInt,
		},
		{
			Label: "RequeueDuration",
			Type:  flux.TInt,
		},
		{
			Label: "ExecuteDuration",
			Type:  flux.TInt,
		},
		{
			Label: "Concurrency",
			Type:  flux.TInt,
		},
		{
			Label: "MaxAllocated",
			Type:  flux.TInt,
		},
		{
			Label: "TotalAllocated",
			Type:  flux.TInt,
		},
		{
			Label: "RuntimeErrors",
			Type:  flux.TString,
		},
		{
			Label: "influxdb/scanned-bytes",
			Type:  flux.TInt,
		},
		{
			Label: "influxdb/scanned-values",
			Type:  flux.TInt,
		},
		{
			Label: "flux/query-plan",
			Type:  flux.TString,
		},
	}
	colData := []interface{}{
		fmt.Sprintf("profiler/query%d", s.start),
		s.start,
		s.start + 1,
		s.start + 2,
		s.start + 3,
		s.start + 4,
		s.start + 5,
		s.start + 6,
		s.start + 7,
		s.start + 8,
		"error1\nerror2",
		s.start + 9,
		s.start + 10,
		"query plan",
	}
	for _, col := range colMeta {
		if _, err := b.AddCol(col); err != nil {
			return nil, err
		}
	}
	for i := 0; i < len(colData); i++ {
		if intValue, ok := colData[i].(int64); ok {
			b.AppendInt(i, intValue)
		} else {
			b.AppendString(i, colData[i].(string))
		}
	}
	tbl, err := b.Table()
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

func NewTestQueryProfiler0() execute.Profiler {
	return &TestQueryProfiler{start: 0}
}

func NewTestQueryProfiler100() execute.Profiler {
	return &TestQueryProfiler{start: 100}
}

func TestFluxProfiler(t *testing.T) {
	testcases := []struct {
		name  string
		data  []string
		query string
		want  string
	}{
		{
			name: "range last single point start time",
			data: []string{
				"m,tag=a f=1i 1",
			},
			query: `
option profiler.enabledProfilers = ["query0", "query100", "query100", "NonExistentProfiler"]
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00.000000001Z, stop: 1970-01-01T01:00:00Z)
	|> last()
`,
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,tag
,,0,1970-01-01T00:00:00.000000001Z,1970-01-01T01:00:00Z,1970-01-01T00:00:00.000000001Z,1,f,m,a

#datatype,string,long,string,long,long,long,long,long,long,long,long,long,string,string,long,long
#group,false,false,true,false,false,false,false,false,false,false,false,false,false,false,false,false
#default,_profiler,,,,,,,,,,,,,,,
,result,table,_measurement,TotalDuration,CompileDuration,QueueDuration,PlanDuration,RequeueDuration,ExecuteDuration,Concurrency,MaxAllocated,TotalAllocated,RuntimeErrors,flux/query-plan,influxdb/scanned-bytes,influxdb/scanned-values
,,0,profiler/query0,0,1,2,3,4,5,6,7,8,"error1
error2","query plan",9,10
,,1,profiler/query100,100,101,102,103,104,105,106,107,108,"error1
error2","query plan",109,110
`,
		},
	}
	execute.RegisterProfilerFactories(NewTestQueryProfiler0, NewTestQueryProfiler100)
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
			defer l.ShutdownOrFail(t, ctx)

			l.WritePointsOrFail(t, strings.Join(tc.data, "\n"))

			queryStr := "import \"profiler\"\nv = {bucket: " + "\"" + l.Bucket.Name + "\"" + "}\n" + tc.query
			req := &query.Request{
				Authorization:  l.Auth,
				OrganizationID: l.Org.ID,
				Compiler: lang.FluxCompiler{
					Query: queryStr,
				},
			}
			if got, err := l.FluxQueryService().Query(ctx, req); err != nil {
				t.Error(err)
			} else {
				dec := csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
				want, err := dec.Decode(ioutil.NopCloser(strings.NewReader(tc.want)))
				if err != nil {
					t.Fatal(err)
				}
				defer want.Release()

				if err := executetest.EqualResultIterators(want, got); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestQueryPushDowns(t *testing.T) {
	testcases := []struct {
		name  string
		data  []string
		query string
		op    string
		want  string
		skip  string
	}{
		{
			name: "range last single point start time",
			data: []string{
				"m,tag=a f=1i 1",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00.000000001Z, stop: 1970-01-01T01:00:00Z)
	|> last()
`,
			op: "readWindow(last)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,tag
,,0,1970-01-01T00:00:00.000000001Z,1970-01-01T01:00:00Z,1970-01-01T00:00:00.000000001Z,1,f,m,a
`,
		},
		{
			name: "window last",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:05Z, stop: 1970-01-01T00:00:20Z)
	|> window(every: 3s)
	|> last()
`,
			op: "readWindow(last)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:05Z,1970-01-01T00:00:06Z,1970-01-01T00:00:05Z,5,f,m0,k0
,,1,1970-01-01T00:00:06Z,1970-01-01T00:00:09Z,1970-01-01T00:00:08Z,0,f,m0,k0
,,2,1970-01-01T00:00:09Z,1970-01-01T00:00:12Z,1970-01-01T00:00:11Z,7,f,m0,k0
,,3,1970-01-01T00:00:12Z,1970-01-01T00:00:15Z,1970-01-01T00:00:14Z,9,f,m0,k0
,,4,1970-01-01T00:00:15Z,1970-01-01T00:00:18Z,1970-01-01T00:00:15Z,5,f,m0,k0
`,
		},
		{
			name: "window offset last",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:05Z, stop: 1970-01-01T00:00:20Z)
	|> window(every: 3s, offset: 2s)
	|> last()
`,
			op: "readWindow(last)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:05Z,1970-01-01T00:00:08Z,1970-01-01T00:00:07Z,5,f,m0,k0
,,1,1970-01-01T00:00:08Z,1970-01-01T00:00:11Z,1970-01-01T00:00:10Z,6,f,m0,k0
,,2,1970-01-01T00:00:11Z,1970-01-01T00:00:14Z,1970-01-01T00:00:13Z,8,f,m0,k0
,,3,1970-01-01T00:00:14Z,1970-01-01T00:00:17Z,1970-01-01T00:00:15Z,5,f,m0,k0
`,
		},
		{
			name: "bare last",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:05Z, stop: 1970-01-01T00:00:20Z)
	|> last()
`,
			op: "readWindow(last)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:05Z,1970-01-01T00:00:20Z,1970-01-01T00:00:15Z,5,f,m0,k0
`,
		},
		{
			name: "window empty last",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1969-12-31T23:00:00Z, stop: 1970-01-01T02:00:00Z)
	|> window(every: 1h, createEmpty: true)
	|> last()
`,
			op: "readWindow(last)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,0,1969-12-31T23:00:00Z,1970-01-01T00:00:00Z,,,f,m0,k0
,result,table,_start,_stop,_time,_value,_field,_measurement,k

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,1,1970-01-01T00:00:00Z,1970-01-01T01:00:00Z,1970-01-01T00:00:15Z,5,f,m0,k0

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,2,1970-01-01T01:00:00Z,1970-01-01T02:00:00Z,,,f,m0,k0
,result,table,_start,_stop,_time,_value,_field,_measurement,k
`,
		},
		{
			name: "window empty offset last",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1969-12-31T23:00:00Z, stop: 1970-01-01T02:00:00Z)
	|> window(every: 1h, offset: 1h, createEmpty: true)
	|> last()
`,
			op: "readWindow(last)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,0,1969-12-31T23:00:00Z,1970-01-01T00:00:00Z,,,f,m0,k0
,result,table,_start,_stop,_time,_value,_field,_measurement,k

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,1,1970-01-01T00:00:00Z,1970-01-01T01:00:00Z,1970-01-01T00:00:15Z,5,f,m0,k0

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,2,1970-01-01T01:00:00Z,1970-01-01T02:00:00Z,,,f,m0,k0
,result,table,_start,_stop,_time,_value,_field,_measurement,k
`,
		},
		{
			name: "window aggregate last",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1969-12-31T23:59:59Z, stop: 1970-01-01T00:00:33Z)
	|> aggregateWindow(every: 10s, fn: last)
`,
			op: "readWindow(last)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,0,1969-12-31T23:59:59Z,1970-01-01T00:00:33Z,1970-01-01T00:00:10Z,6,f,m0,k0
,,0,1969-12-31T23:59:59Z,1970-01-01T00:00:33Z,1970-01-01T00:00:20Z,5,f,m0,k0
`,
		},
		{
			name: "window first",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:05Z, stop: 1970-01-01T00:00:20Z)
	|> window(every: 3s)
	|> first()
`,
			op: "readWindow(first)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:05Z,1970-01-01T00:00:06Z,1970-01-01T00:00:05Z,5,f,m0,k0
,,1,1970-01-01T00:00:06Z,1970-01-01T00:00:09Z,1970-01-01T00:00:06Z,6,f,m0,k0
,,2,1970-01-01T00:00:09Z,1970-01-01T00:00:12Z,1970-01-01T00:00:09Z,6,f,m0,k0
,,3,1970-01-01T00:00:12Z,1970-01-01T00:00:15Z,1970-01-01T00:00:12Z,5,f,m0,k0
,,4,1970-01-01T00:00:15Z,1970-01-01T00:00:18Z,1970-01-01T00:00:15Z,5,f,m0,k0
`,
		},
		{
			name: "window first string",
			data: []string{
				"m,tag=a f=\"c\" 2000000000",
				"m,tag=a f=\"d\" 3000000000",
				"m,tag=a f=\"h\" 7000000000",
				"m,tag=a f=\"i\" 8000000000",
				"m,tag=a f=\"j\" 9000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:10Z)
	|> window(every: 5s)
	|> first()
`,
			op: "readWindow(first)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,tag
,,0,1970-01-01T00:00:00Z,1970-01-01T00:00:05Z,1970-01-01T00:00:02Z,c,f,m,a
,,1,1970-01-01T00:00:05Z,1970-01-01T00:00:10Z,1970-01-01T00:00:07Z,h,f,m,a
`,
		},
		{
			name: "bare first",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:05Z, stop: 1970-01-01T00:00:20Z)
	|> first()
`,
			op: "readWindow(first)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:05Z,1970-01-01T00:00:20Z,1970-01-01T00:00:05Z,5,f,m0,k0
`,
		},
		{
			name: "window empty first",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:02Z)
	|> window(every: 500ms, createEmpty: true)
	|> first()
`,
			op: "readWindow(first)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,_result,table,_start,_stop,_time,_value,_field,_measurement,k
,_result,0,1970-01-01T00:00:00Z,1970-01-01T00:00:00.5Z,1970-01-01T00:00:00Z,0,f,m0,k0

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,1,1970-01-01T00:00:00.5Z,1970-01-01T00:00:01Z,,,f,m0,k0
,_result,table,_start,_stop,_time,_value,_field,_measurement,k

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,_result,table,_start,_stop,_time,_value,_field,_measurement,k
,_result,2,1970-01-01T00:00:01Z,1970-01-01T00:00:01.5Z,1970-01-01T00:00:01Z,1,f,m0,k0

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,3,1970-01-01T00:00:01.5Z,1970-01-01T00:00:02Z,,,f,m0,k0
,_result,table,_start,_stop,_time,_value,_field,_measurement,k
`,
		},
		{
			name: "window aggregate first",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:02Z)
	|> aggregateWindow(every: 500ms, fn: first)
`,
			op: "readWindow(first)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:00Z,1970-01-01T00:00:02Z,1970-01-01T00:00:00.5Z,0,f,m0,k0
,,0,1970-01-01T00:00:00Z,1970-01-01T00:00:02Z,1970-01-01T00:00:01.5Z,1,f,m0,k0
`,
		},
		{
			name: "window min",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:05Z, stop: 1970-01-01T00:00:20Z)
	|> window(every: 3s)
	|> min()
`,
			op: "readWindow(min)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:05Z,1970-01-01T00:00:06Z,1970-01-01T00:00:05Z,5,f,m0,k0
,,1,1970-01-01T00:00:06Z,1970-01-01T00:00:09Z,1970-01-01T00:00:08Z,0,f,m0,k0
,,2,1970-01-01T00:00:09Z,1970-01-01T00:00:12Z,1970-01-01T00:00:09Z,6,f,m0,k0
,,3,1970-01-01T00:00:12Z,1970-01-01T00:00:15Z,1970-01-01T00:00:12Z,5,f,m0,k0
,,4,1970-01-01T00:00:15Z,1970-01-01T00:00:18Z,1970-01-01T00:00:15Z,5,f,m0,k0
`,
		},
		{
			name: "bare min",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:05Z, stop: 1970-01-01T00:00:20Z)
	|> min()
`,
			op: "readWindow(min)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:05Z,1970-01-01T00:00:20Z,1970-01-01T00:00:08Z,0,f,m0,k0
`,
		},
		{
			name: "window empty min",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:12Z)
	|> window(every: 3s, createEmpty: true)
	|> min()
`,
			op: "readWindow(min)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,_result,table,_start,_stop,_time,_value,_field,_measurement,k
,_result,0,1970-01-01T00:00:00Z,1970-01-01T00:00:03Z,1970-01-01T00:00:00Z,0,f,m0,k0

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,1,1970-01-01T00:00:03Z,1970-01-01T00:00:06Z,,,f,m0,k0
,_result,table,_start,_stop,_time,_value,_field,_measurement,k

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,_result,table,_start,_stop,_time,_value,_field,_measurement,k
,_result,2,1970-01-01T00:00:06Z,1970-01-01T00:00:09Z,1970-01-01T00:00:08Z,0,f,m0,k0

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,3,1970-01-01T00:00:09Z,1970-01-01T00:00:12Z,,,f,m0,k0
,_result,table,_start,_stop,_time,_value,_field,_measurement,k
`,
		},
		{
			name: "window aggregate min",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:12Z)
	|> aggregateWindow(every: 3s, fn: min)
`,
			op: "readWindow(min)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:00Z,1970-01-01T00:00:12Z,1970-01-01T00:00:03Z,0,f,m0,k0
,,0,1970-01-01T00:00:00Z,1970-01-01T00:00:12Z,1970-01-01T00:00:09Z,0,f,m0,k0
`,
		},
		{
			name: "window max",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:05Z, stop: 1970-01-01T00:00:20Z)
	|> window(every: 3s)
	|> max()
`,
			op: "readWindow(max)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:05Z,1970-01-01T00:00:06Z,1970-01-01T00:00:05Z,5,f,m0,k0
,,1,1970-01-01T00:00:06Z,1970-01-01T00:00:09Z,1970-01-01T00:00:06Z,6,f,m0,k0
,,2,1970-01-01T00:00:09Z,1970-01-01T00:00:12Z,1970-01-01T00:00:11Z,7,f,m0,k0
,,3,1970-01-01T00:00:12Z,1970-01-01T00:00:15Z,1970-01-01T00:00:14Z,9,f,m0,k0
,,4,1970-01-01T00:00:15Z,1970-01-01T00:00:18Z,1970-01-01T00:00:15Z,5,f,m0,k0
`,
		},
		{
			name: "bare max",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:05Z, stop: 1970-01-01T00:00:20Z)
	|> max()
`,
			op: "readWindow(max)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:05Z,1970-01-01T00:00:20Z,1970-01-01T00:00:14Z,9,f,m0,k0
`,
		},
		{
			name: "window empty max",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:12Z)
	|> window(every: 3s, createEmpty: true)
	|> max()
`,
			op: "readWindow(max)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,_result,table,_start,_stop,_time,_value,_field,_measurement,k
,_result,0,1970-01-01T00:00:00Z,1970-01-01T00:00:03Z,1970-01-01T00:00:02Z,2,f,m0,k0

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,1,1970-01-01T00:00:03Z,1970-01-01T00:00:06Z,,,f,m0,k0
,_result,table,_start,_stop,_time,_value,_field,_measurement,k

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,_result,table,_start,_stop,_time,_value,_field,_measurement,k
,_result,2,1970-01-01T00:00:06Z,1970-01-01T00:00:09Z,1970-01-01T00:00:06Z,6,f,m0,k0

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,3,1970-01-01T00:00:09Z,1970-01-01T00:00:12Z,,,f,m0,k0
,_result,table,_start,_stop,_time,_value,_field,_measurement,k
`,
		},
		{
			name: "window aggregate max",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:12Z)
	|> aggregateWindow(every: 3s, fn: max)
`,
			op: "readWindow(max)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:00Z,1970-01-01T00:00:12Z,1970-01-01T00:00:03Z,2,f,m0,k0
,,0,1970-01-01T00:00:00Z,1970-01-01T00:00:12Z,1970-01-01T00:00:09Z,6,f,m0,k0
`,
		},
		{
			name: "window count removes empty series",
			data: []string{
				"m,tag=a f=0i 1500000000",
				"m,tag=b f=1i 2500000000",
				"m,tag=c f=2i 3500000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:01Z, stop: 1970-01-01T00:00:02Z)
	|> window(every: 500ms, createEmpty: true)
	|> count()
`,
			op: "readWindow(count)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,true,true,true
#default,_result,,,,,,,
,result,table,_start,_stop,_value,_field,_measurement,tag
,_result,0,1970-01-01T00:00:01Z,1970-01-01T00:00:01.5Z,0,f,m,a
,_result,1,1970-01-01T00:00:01.5Z,1970-01-01T00:00:02Z,1,f,m,a
`,
		},
		{
			name: "count",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> aggregateWindow(every: 5s, fn: count)
	|> drop(columns: ["_start", "_stop"])
`,
			op: "readWindow(count)",
			want: `
#datatype,string,long,dateTime:RFC3339,long,string,string,string
#group,false,false,false,false,true,true,true
#default,_result,,,,,,
,result,table,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:05Z,5,f,m0,k0
,,0,1970-01-01T00:00:10Z,5,f,m0,k0
,,0,1970-01-01T00:00:15Z,5,f,m0,k0
`,
		},
		{
			name: "window offset count",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> window(every: 5s, offset: 2s)
	|> count()
`,
			op: "readWindow(count)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,true,true,true
#default,_result,,,,,,,
,result,table,_start,_stop,_value,_field,_measurement,k
,,0,1970-01-01T00:00:00Z,1970-01-01T00:00:02Z,2,f,m0,k0
,,1,1970-01-01T00:00:02Z,1970-01-01T00:00:07Z,5,f,m0,k0
,,2,1970-01-01T00:00:07Z,1970-01-01T00:00:12Z,5,f,m0,k0
,,3,1970-01-01T00:00:12Z,1970-01-01T00:00:15Z,3,f,m0,k0
`,
		},
		{
			name: "count with nulls",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> aggregateWindow(every: 5s, fn: count)
	|> drop(columns: ["_start", "_stop"])
`,
			op: "readWindow(count)",
			want: `
#datatype,string,long,dateTime:RFC3339,long,string,string,string
#group,false,false,false,false,true,true,true
#default,_result,,,,,,
,result,table,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:05Z,5,f,m0,k0
,,0,1970-01-01T00:00:10Z,0,f,m0,k0
,,0,1970-01-01T00:00:15Z,5,f,m0,k0
`,
		},
		{
			name: "bare count",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> count()
	|> drop(columns: ["_start", "_stop"])
`,
			op: "readWindow(count)",
			want: `
#group,false,false,false,true,true,true
#datatype,string,long,long,string,string,string
#default,_result,,,,,
,result,table,_value,_field,_measurement,k
,,0,15,f,m0,k0
`,
		},
		{
			name: "window sum removes empty series",
			data: []string{
				"m,tag=a f=1i 1500000000",
				"m,tag=a f=2i 1600000000",
				"m,tag=b f=3i 2500000000",
				"m,tag=c f=4i 3500000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:01Z, stop: 1970-01-01T00:00:02Z)
	|> window(every: 500ms, createEmpty: true)
	|> sum()
`,
			op: "readWindow(sum)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,true,true,true
#default,_result,,,,,,,
,result,table,_start,_stop,_value,_field,_measurement,tag
,_result,0,1970-01-01T00:00:01Z,1970-01-01T00:00:01.5Z,,f,m,a
,_result,1,1970-01-01T00:00:01.5Z,1970-01-01T00:00:02Z,3,f,m,a
`,
		},
		{
			name: "sum",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> aggregateWindow(every: 5s, fn: sum)
	|> drop(columns: ["_start", "_stop"])
`,
			op: "readWindow(sum)",
			want: `
#datatype,string,long,dateTime:RFC3339,long,string,string,string
#group,false,false,false,false,true,true,true
#default,_result,,,,,,
,result,table,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:05Z,10,f,m0,k0
,,0,1970-01-01T00:00:10Z,22,f,m0,k0
,,0,1970-01-01T00:00:15Z,35,f,m0,k0
`,
		},
		{
			name: "window offset sum",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> window(every: 5s, offset: 2s)
	|> sum()
`,
			op: "readWindow(sum)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,true,true,true
#default,_result,,,,,,,
,result,table,_start,_stop,_value,_field,_measurement,k
,,0,1970-01-01T00:00:00Z,1970-01-01T00:00:02Z,1,f,m0,k0
,,1,1970-01-01T00:00:02Z,1970-01-01T00:00:07Z,20,f,m0,k0
,,2,1970-01-01T00:00:07Z,1970-01-01T00:00:12Z,24,f,m0,k0
,,3,1970-01-01T00:00:12Z,1970-01-01T00:00:15Z,22,f,m0,k0
`,
		},
		{
			name: "sum with nulls",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> aggregateWindow(every: 5s, fn: sum)
	|> drop(columns: ["_start", "_stop"])
`,
			op: "readWindow(sum)",
			want: `
#datatype,string,long,dateTime:RFC3339,long,string,string,string
#group,false,false,false,false,true,true,true
#default,_result,,,,,,
,result,table,_time,_value,_field,_measurement,k
,,0,1970-01-01T00:00:05Z,10,f,m0,k0
,,0,1970-01-01T00:00:10Z,,f,m0,k0
,,0,1970-01-01T00:00:15Z,35,f,m0,k0
`,
		},
		{
			name: "bare sum",
			data: []string{
				"m0,k=k0 f=0i 0",
				"m0,k=k0 f=1i 1000000000",
				"m0,k=k0 f=2i 2000000000",
				"m0,k=k0 f=3i 3000000000",
				"m0,k=k0 f=4i 4000000000",
				"m0,k=k0 f=5i 5000000000",
				"m0,k=k0 f=6i 6000000000",
				"m0,k=k0 f=5i 7000000000",
				"m0,k=k0 f=0i 8000000000",
				"m0,k=k0 f=6i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=5i 12000000000",
				"m0,k=k0 f=8i 13000000000",
				"m0,k=k0 f=9i 14000000000",
				"m0,k=k0 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> sum()
	|> drop(columns: ["_start", "_stop"])
`,
			op: "readWindow(sum)",
			want: `
#group,false,false,false,true,true,true
#datatype,string,long,long,string,string,string
#default,_result,,,,,
,result,table,_value,_field,_measurement,k
,,0,67,f,m0,k0
`,
		},
		{
			name: "bare mean",
			data: []string{
				"m0,k=k0,kk=kk0 f=5 0",
				"m0,k=k0,kk=kk0 f=6 5000000000",
				"m0,k=k0,kk=kk0 f=7 10000000000",
				"m0,k=k0,kk=kk0 f=9 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 0)
	|> mean()
	|> keep(columns: ["_value"])
`,
			op: "readWindow(mean)",
			want: `
#datatype,string,long,double
#group,false,false,false
#default,_result,,
,result,table,_value
,,0,6.75
`,
		},
		{
			name: "window mean",
			data: []string{
				"m0,k=k0 f=1i 5000000000",
				"m0,k=k0 f=2i 6000000000",
				"m0,k=k0 f=3i 7000000000",
				"m0,k=k0 f=4i 8000000000",
				"m0,k=k0 f=5i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=8i 12000000000",
				"m0,k=k0 f=9i 13000000000",
				"m0,k=k0 f=10i 14000000000",
				"m0,k=k0 f=11i 15000000000",
				"m0,k=k0 f=12i 16000000000",
				"m0,k=k0 f=13i 17000000000",
				"m0,k=k0 f=14i 18000000000",
				"m0,k=k0 f=16i 19000000000",
				"m0,k=k0 f=17i 20000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:05Z, stop: 1970-01-01T00:00:20Z)
	|> aggregateWindow(fn: mean, every: 5s)
	|> keep(columns: ["_time", "_value"])
`,
			op: "readWindow(mean)",
			want: `
#datatype,string,long,dateTime:RFC3339,double
#group,false,false,false,false
#default,_result,,,
,result,table,_time,_value
,,0,1970-01-01T00:00:10Z,3
,,0,1970-01-01T00:00:15Z,8
,,0,1970-01-01T00:00:20Z,13.2
`,
		},
		{
			name: "window mean offset",
			data: []string{
				"m0,k=k0 f=1i 5000000000",
				"m0,k=k0 f=2i 6000000000",
				"m0,k=k0 f=3i 7000000000",
				"m0,k=k0 f=4i 8000000000",
				"m0,k=k0 f=5i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=8i 12000000000",
				"m0,k=k0 f=9i 13000000000",
				"m0,k=k0 f=10i 14000000000",
				"m0,k=k0 f=11i 15000000000",
				"m0,k=k0 f=12i 16000000000",
				"m0,k=k0 f=13i 17000000000",
				"m0,k=k0 f=14i 18000000000",
				"m0,k=k0 f=16i 19000000000",
				"m0,k=k0 f=17i 20000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:05Z, stop: 1970-01-01T00:00:20Z)
	|> window(every: 5s, offset: 1s)
	|> mean()
`,
			op: "readWindow(mean)",
			want: `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string,string,double
#group,false,false,true,true,true,true,true,false
#default,_result,,,,,,,
,result,table,_start,_stop,_field,_measurement,k,_value
,,0,1970-01-01T00:00:05Z,1970-01-01T00:00:06Z,f,m0,k0,1
,,1,1970-01-01T00:00:06Z,1970-01-01T00:00:11Z,f,m0,k0,4
,,2,1970-01-01T00:00:11Z,1970-01-01T00:00:16Z,f,m0,k0,9
,,3,1970-01-01T00:00:16Z,1970-01-01T00:00:20Z,f,m0,k0,13.75
`,
		},
		{
			name: "window mean offset with duplicate and unwindow",
			data: []string{
				"m0,k=k0 f=1i 5000000000",
				"m0,k=k0 f=2i 6000000000",
				"m0,k=k0 f=3i 7000000000",
				"m0,k=k0 f=4i 8000000000",
				"m0,k=k0 f=5i 9000000000",
				"m0,k=k0 f=6i 10000000000",
				"m0,k=k0 f=7i 11000000000",
				"m0,k=k0 f=8i 12000000000",
				"m0,k=k0 f=9i 13000000000",
				"m0,k=k0 f=10i 14000000000",
				"m0,k=k0 f=11i 15000000000",
				"m0,k=k0 f=12i 16000000000",
				"m0,k=k0 f=13i 17000000000",
				"m0,k=k0 f=14i 18000000000",
				"m0,k=k0 f=16i 19000000000",
				"m0,k=k0 f=17i 20000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:05Z, stop: 1970-01-01T00:00:20Z)
	|> window(every: 5s, offset: 1s)
	|> mean()
	|> duplicate(column: "_stop", as: "_time")
	|> window(every: inf)
	|> keep(columns: ["_time", "_value"])
`,
			op: "readWindow(mean)",
			want: `
#datatype,string,long,dateTime:RFC3339,double
#group,false,false,false,false
#default,_result,,,
,result,table,_time,_value
,,0,1970-01-01T00:00:06Z,1
,,0,1970-01-01T00:00:11Z,4
,,0,1970-01-01T00:00:16Z,9
,,0,1970-01-01T00:00:20Z,13.75
`,
		},
		{
			name: "group first",
			data: []string{
				"m0,k=k0,kk=kk0 f=0i 0",
				"m0,k=k0,kk=kk1 f=1i 1000000000",
				"m0,k=k0,kk=kk0 f=2i 2000000000",
				"m0,k=k0,kk=kk1 f=3i 3000000000",
				"m0,k=k0,kk=kk0 f=4i 4000000000",
				"m0,k=k0,kk=kk1 f=5i 5000000000",
				"m0,k=k0,kk=kk0 f=6i 6000000000",
				"m0,k=k0,kk=kk1 f=5i 7000000000",
				"m0,k=k0,kk=kk0 f=0i 8000000000",
				"m0,k=k0,kk=kk1 f=6i 9000000000",
				"m0,k=k0,kk=kk0 f=6i 10000000000",
				"m0,k=k0,kk=kk1 f=7i 11000000000",
				"m0,k=k0,kk=kk0 f=5i 12000000000",
				"m0,k=k0,kk=kk1 f=8i 13000000000",
				"m0,k=k0,kk=kk0 f=9i 14000000000",
				"m0,k=k0,kk=kk1 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 0)
	|> group(columns: ["k"])
	|> first()
	|> keep(columns: ["_time", "_value"])
`,
			op: "readGroup(first)",
			want: `
#datatype,string,long,dateTime:RFC3339,long
#group,false,false,false,false
#default,_result,,,
,result,table,_time,_value
,,0,1970-01-01T00:00:00.00Z,0
`,
			skip: "https://github.com/influxdata/idpe/issues/8828",
		},
		{
			name: "group none first",
			data: []string{
				"m0,k=k0,kk=kk0 f=0i 0",
				"m0,k=k0,kk=kk1 f=1i 1000000000",
				"m0,k=k0,kk=kk0 f=2i 2000000000",
				"m0,k=k0,kk=kk1 f=3i 3000000000",
				"m0,k=k0,kk=kk0 f=4i 4000000000",
				"m0,k=k0,kk=kk1 f=5i 5000000000",
				"m0,k=k0,kk=kk0 f=6i 6000000000",
				"m0,k=k0,kk=kk1 f=5i 7000000000",
				"m0,k=k0,kk=kk0 f=0i 8000000000",
				"m0,k=k0,kk=kk1 f=6i 9000000000",
				"m0,k=k0,kk=kk0 f=6i 10000000000",
				"m0,k=k0,kk=kk1 f=7i 11000000000",
				"m0,k=k0,kk=kk0 f=5i 12000000000",
				"m0,k=k0,kk=kk1 f=8i 13000000000",
				"m0,k=k0,kk=kk0 f=9i 14000000000",
				"m0,k=k0,kk=kk1 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 0)
	|> group()
	|> first()
	|> keep(columns: ["_time", "_value"])
`,
			op: "readGroup(first)",
			want: `
#datatype,string,long,dateTime:RFC3339,long
#group,false,false,false,false
#default,_result,,,
,result,table,_time,_value
,,0,1970-01-01T00:00:00.00Z,0
`,
			skip: "https://github.com/influxdata/idpe/issues/8828",
		},
		{
			name: "group last",
			data: []string{
				"m0,k=k0,kk=kk0 f=0i 0",
				"m0,k=k0,kk=kk1 f=1i 1000000000",
				"m0,k=k0,kk=kk0 f=2i 2000000000",
				"m0,k=k0,kk=kk1 f=3i 3000000000",
				"m0,k=k0,kk=kk0 f=4i 4000000000",
				"m0,k=k0,kk=kk1 f=5i 5000000000",
				"m0,k=k0,kk=kk0 f=6i 6000000000",
				"m0,k=k0,kk=kk1 f=5i 7000000000",
				"m0,k=k0,kk=kk0 f=0i 8000000000",
				"m0,k=k0,kk=kk1 f=6i 9000000000",
				"m0,k=k0,kk=kk0 f=6i 10000000000",
				"m0,k=k0,kk=kk1 f=7i 11000000000",
				"m0,k=k0,kk=kk0 f=5i 12000000000",
				"m0,k=k0,kk=kk1 f=8i 13000000000",
				"m0,k=k0,kk=kk0 f=9i 14000000000",
				"m0,k=k0,kk=kk1 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 0)
	|> group(columns: ["k"])
	|> last()
	|> keep(columns: ["_time", "_value"])
`,
			op: "readGroup(last)",
			want: `
#datatype,string,long,dateTime:RFC3339,long
#group,false,false,false,false
#default,_result,,,
,result,table,_time,_value
,,0,1970-01-01T00:00:15.00Z,5
`,
			skip: "https://github.com/influxdata/idpe/issues/8828",
		},
		{
			name: "group none last",
			data: []string{
				"m0,k=k0,kk=kk0 f=0i 0",
				"m0,k=k0,kk=kk1 f=1i 1000000000",
				"m0,k=k0,kk=kk0 f=2i 2000000000",
				"m0,k=k0,kk=kk1 f=3i 3000000000",
				"m0,k=k0,kk=kk0 f=4i 4000000000",
				"m0,k=k0,kk=kk1 f=5i 5000000000",
				"m0,k=k0,kk=kk0 f=6i 6000000000",
				"m0,k=k0,kk=kk1 f=5i 7000000000",
				"m0,k=k0,kk=kk0 f=0i 8000000000",
				"m0,k=k0,kk=kk1 f=6i 9000000000",
				"m0,k=k0,kk=kk0 f=6i 10000000000",
				"m0,k=k0,kk=kk1 f=7i 11000000000",
				"m0,k=k0,kk=kk0 f=5i 12000000000",
				"m0,k=k0,kk=kk1 f=8i 13000000000",
				"m0,k=k0,kk=kk0 f=9i 14000000000",
				"m0,k=k0,kk=kk1 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 0)
	|> group()
	|> last()
	|> keep(columns: ["_time", "_value"])
`,
			op: "readGroup(last)",
			want: `
#datatype,string,long,dateTime:RFC3339,long
#group,false,false,false,false
#default,_result,,,
,result,table,_time,_value
,,0,1970-01-01T00:00:15.00Z,5
`,
			skip: "https://github.com/influxdata/idpe/issues/8828",
		},
		{
			name: "count group none",
			data: []string{
				"m0,k=k0,kk=kk0 f=0i 0",
				"m0,k=k0,kk=kk1 f=1i 1000000000",
				"m0,k=k0,kk=kk0 f=2i 2000000000",
				"m0,k=k0,kk=kk1 f=3i 3000000000",
				"m0,k=k0,kk=kk0 f=4i 4000000000",
				"m0,k=k0,kk=kk1 f=5i 5000000000",
				"m0,k=k0,kk=kk0 f=6i 6000000000",
				"m0,k=k0,kk=kk1 f=5i 7000000000",
				"m0,k=k0,kk=kk0 f=0i 8000000000",
				"m0,k=k0,kk=kk1 f=6i 9000000000",
				"m0,k=k0,kk=kk0 f=6i 10000000000",
				"m0,k=k0,kk=kk1 f=7i 11000000000",
				"m0,k=k0,kk=kk0 f=5i 12000000000",
				"m0,k=k0,kk=kk1 f=8i 13000000000",
				"m0,k=k0,kk=kk0 f=9i 14000000000",
				"m0,k=k0,kk=kk1 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> group()
	|> count()
	|> drop(columns: ["_start", "_stop"])
`,
			op: "readGroup(count)",
			want: `
#datatype,string,long,long
#group,false,false,false
#default,_result,,
,result,table,_value
,,0,15
`,
			skip: "https://github.com/influxdata/idpe/issues/8828",
		},
		{
			name: "count group",
			data: []string{
				"m0,k=k0,kk=kk0 f=0i 0",
				"m0,k=k0,kk=kk1 f=1i 1000000000",
				"m0,k=k0,kk=kk0 f=2i 2000000000",
				"m0,k=k0,kk=kk1 f=3i 3000000000",
				"m0,k=k0,kk=kk0 f=4i 4000000000",
				"m0,k=k0,kk=kk1 f=5i 5000000000",
				"m0,k=k0,kk=kk0 f=6i 6000000000",
				"m0,k=k0,kk=kk1 f=5i 7000000000",
				"m0,k=k0,kk=kk0 f=0i 8000000000",
				"m0,k=k0,kk=kk1 f=6i 9000000000",
				"m0,k=k0,kk=kk0 f=6i 10000000000",
				"m0,k=k0,kk=kk1 f=7i 11000000000",
				"m0,k=k0,kk=kk0 f=5i 12000000000",
				"m0,k=k0,kk=kk1 f=8i 13000000000",
				"m0,k=k0,kk=kk0 f=9i 14000000000",
				"m0,k=k0,kk=kk1 f=5i 15000000000",
			},
			op: "readGroup(count)",
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> group(columns: ["kk"])
	|> count()
	|> drop(columns: ["_start", "_stop"])
`,
			want: `
#datatype,string,long,string,long
#group,false,false,true,false
#default,_result,,,
,result,table,kk,_value
,,0,kk0,8
,,1,kk1,7
`,
			skip: "https://github.com/influxdata/idpe/issues/8828",
		},
		{
			name: "sum group none",
			data: []string{
				"m0,k=k0,kk=kk0 f=0i 0",
				"m0,k=k0,kk=kk1 f=1i 1000000000",
				"m0,k=k0,kk=kk0 f=2i 2000000000",
				"m0,k=k0,kk=kk1 f=3i 3000000000",
				"m0,k=k0,kk=kk0 f=4i 4000000000",
				"m0,k=k0,kk=kk1 f=5i 5000000000",
				"m0,k=k0,kk=kk0 f=6i 6000000000",
				"m0,k=k0,kk=kk1 f=5i 7000000000",
				"m0,k=k0,kk=kk0 f=0i 8000000000",
				"m0,k=k0,kk=kk1 f=6i 9000000000",
				"m0,k=k0,kk=kk0 f=6i 10000000000",
				"m0,k=k0,kk=kk1 f=7i 11000000000",
				"m0,k=k0,kk=kk0 f=5i 12000000000",
				"m0,k=k0,kk=kk1 f=8i 13000000000",
				"m0,k=k0,kk=kk0 f=9i 14000000000",
				"m0,k=k0,kk=kk1 f=5i 15000000000",
			},
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> group()
	|> sum()
	|> drop(columns: ["_start", "_stop"])
`,
			op: "readGroup(sum)",
			want: `
#datatype,string,long,long
#group,false,false,false
#default,_result,,
,result,table,_value
,,0,67
`,
			skip: "https://github.com/influxdata/idpe/issues/8828",
		},
		{
			name: "sum group",
			data: []string{
				"m0,k=k0,kk=kk0 f=0i 0",
				"m0,k=k0,kk=kk1 f=1i 1000000000",
				"m0,k=k0,kk=kk0 f=2i 2000000000",
				"m0,k=k0,kk=kk1 f=3i 3000000000",
				"m0,k=k0,kk=kk0 f=4i 4000000000",
				"m0,k=k0,kk=kk1 f=5i 5000000000",
				"m0,k=k0,kk=kk0 f=6i 6000000000",
				"m0,k=k0,kk=kk1 f=5i 7000000000",
				"m0,k=k0,kk=kk0 f=0i 8000000000",
				"m0,k=k0,kk=kk1 f=6i 9000000000",
				"m0,k=k0,kk=kk0 f=6i 10000000000",
				"m0,k=k0,kk=kk1 f=7i 11000000000",
				"m0,k=k0,kk=kk0 f=5i 12000000000",
				"m0,k=k0,kk=kk1 f=8i 13000000000",
				"m0,k=k0,kk=kk0 f=9i 14000000000",
				"m0,k=k0,kk=kk1 f=5i 15000000000",
			},
			op: "readGroup(sum)",
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> group(columns: ["kk"])
	|> sum()
	|> drop(columns: ["_start", "_stop"])
`,
			want: `
#datatype,string,long,string,long
#group,false,false,true,false
#default,_result,,,
,result,table,kk,_value
,,0,kk0,32
,,1,kk1,35
`,
			skip: "https://github.com/influxdata/idpe/issues/8828",
		},
		{
			name: "min group",
			data: []string{
				"m0,k=k0,kk=kk0 f=0i 0",
				"m0,k=k0,kk=kk1 f=1i 1000000000",
				"m0,k=k0,kk=kk0 f=2i 2000000000",
				"m0,k=k0,kk=kk1 f=3i 3000000000",
				"m0,k=k0,kk=kk0 f=4i 4000000000",
				"m0,k=k0,kk=kk1 f=5i 5000000000",
				"m0,k=k0,kk=kk0 f=6i 6000000000",
				"m0,k=k0,kk=kk1 f=5i 7000000000",
				"m0,k=k0,kk=kk0 f=0i 8000000000",
				"m0,k=k0,kk=kk1 f=6i 9000000000",
				"m0,k=k0,kk=kk0 f=6i 10000000000",
				"m0,k=k0,kk=kk1 f=7i 11000000000",
				"m0,k=k0,kk=kk0 f=5i 12000000000",
				"m0,k=k0,kk=kk1 f=8i 13000000000",
				"m0,k=k0,kk=kk0 f=9i 14000000000",
				"m0,k=k0,kk=kk1 f=5i 15000000000",
			},
			op: "readGroup(min)",
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> group(columns: ["kk"])
	|> min()
	|> keep(columns: ["kk", "_value"])
`,
			want: `
#datatype,string,long,string,long
#group,false,false,true,false
#default,_result,,,
,result,table,kk,_value
,,0,kk0,0
,,1,kk1,1
`,
			skip: "https://github.com/influxdata/idpe/issues/8828",
		},
		{
			name: "max group",
			data: []string{
				"m0,k=k0,kk=kk0 f=0i 0",
				"m0,k=k0,kk=kk1 f=1i 1000000000",
				"m0,k=k0,kk=kk0 f=2i 2000000000",
				"m0,k=k0,kk=kk1 f=3i 3000000000",
				"m0,k=k0,kk=kk0 f=4i 4000000000",
				"m0,k=k0,kk=kk1 f=5i 5000000000",
				"m0,k=k0,kk=kk0 f=6i 6000000000",
				"m0,k=k0,kk=kk1 f=5i 7000000000",
				"m0,k=k0,kk=kk0 f=0i 8000000000",
				"m0,k=k0,kk=kk1 f=6i 9000000000",
				"m0,k=k0,kk=kk0 f=6i 10000000000",
				"m0,k=k0,kk=kk1 f=7i 11000000000",
				"m0,k=k0,kk=kk0 f=5i 12000000000",
				"m0,k=k0,kk=kk1 f=8i 13000000000",
				"m0,k=k0,kk=kk0 f=9i 14000000000",
				"m0,k=k0,kk=kk1 f=5i 15000000000",
			},
			op: "readGroup(max)",
			query: `
from(bucket: v.bucket)
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:15Z)
	|> group(columns: ["kk"])
	|> max()
	|> keep(columns: ["kk", "_value"])
`,
			want: `
#datatype,string,long,string,long
#group,false,false,true,false
#default,_result,,,
,result,table,kk,_value
,,0,kk0,9
,,1,kk1,8
`,
			skip: "https://github.com/influxdata/idpe/issues/8828",
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip != "" {
				t.Skip(tc.skip)
			}

			l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
			defer l.ShutdownOrFail(t, ctx)

			l.WritePointsOrFail(t, strings.Join(tc.data, "\n"))

			queryStr := "v = {bucket: " + "\"" + l.Bucket.Name + "\"" + "}\n" + tc.query

			res := l.MustExecuteQuery(queryStr)
			defer res.Done()
			got := flux.NewSliceResultIterator(res.Results)
			defer got.Release()

			dec := csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
			want, err := dec.Decode(ioutil.NopCloser(strings.NewReader(tc.want)))
			if err != nil {
				t.Fatal(err)
			}
			defer want.Release()

			if err := executetest.EqualResultIterators(want, got); err != nil {
				t.Fatal(err)
			}
			if want, got := uint64(1), l.NumReads(t, tc.op); want != got {
				t.Fatalf("unexpected sample count -want/+got:\n\t- %d\n\t+ %d", want, got)
			}
		})
	}
}

func TestLauncher_Query_Buckets_MultiplePages(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l.ShutdownOrFail(t, ctx)

	// Create a large number of buckets. This is above the default
	// page size of 20.
	for i := 0; i < 50; i++ {
		b := &influxdb.Bucket{
			OrgID: l.Org.ID,
			Name:  fmt.Sprintf("b%02d", i),
		}
		if err := l.BucketService(t).CreateBucket(ctx, b); err != nil {
			t.Fatal(err)
		}
	}

	var sb strings.Builder
	sb.WriteString(`
#datatype,string,long,string
#group,false,false,false
#default,_result,,
,result,table,name
,,0,BUCKET
,,0,_monitoring
,,0,_tasks
`)
	for i := 0; i < 50; i++ {
		_, _ = fmt.Fprintf(&sb, ",,0,b%02d\n", i)
	}
	data := sb.String()

	bucketsQuery := `
buckets()
	|> keep(columns: ["name"])
	|> sort(columns: ["name"])
`
	res := l.MustExecuteQuery(bucketsQuery)
	defer res.Done()

	firstResult := func(ri flux.ResultIterator) flux.Result {
		ri.More()
		return ri.Next()
	}
	got := firstResult(flux.NewSliceResultIterator(res.Results))

	want, err := csv.NewResultDecoder(csv.ResultDecoderConfig{}).Decode(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	if diff := table.Diff(want.Tables(), got.Tables()); diff != "" {
		t.Fatalf("unexpected output -want/+got:\n%s", diff)
	}
}
