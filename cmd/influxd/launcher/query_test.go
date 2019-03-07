package launcher_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	nethttp "net/http"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	platform "github.com/influxdata/influxdb"
	phttp "github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/query"
)

func TestPipeline_Write_Query_FieldKey(t *testing.T) {
	be := RunLauncherOrFail(t, ctx)
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
	results := be.MustExecuteQuery(be.Org.ID, rawQ, be.Auth)
	defer results.Done()
	results.First(t).HasTablesWithCols([]int{5, 4, 4})
}

// This test initialises a default launcher writes some data,
// and checks that the queried results contain the expected number of tables
// and expected number of columns.
func TestPipeline_WriteV2_Query(t *testing.T) {
	t.Parallel()

	be := RunLauncherOrFail(t, ctx)
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

	res := be.MustExecuteQuery(
		be.Org.ID,
		fmt.Sprintf(`from(bucket:"%s") |> range(start:-5m)`, be.Bucket.Name),
		be.Auth)
	defer res.Done()
	res.HasTableCount(t, 1)
}

// QueryResult wraps a single flux.Result with some helper methods.
type QueryResult struct {
	t *testing.T
	q flux.Result
}

// HasTableWithCols checks if the desired number of tables and columns exist,
// ignoring any system columns.
//
// If the result is not as expected then the testing.T fails.
func (r *QueryResult) HasTablesWithCols(want []int) {
	r.t.Helper()

	// _start, _stop, _time, _f
	systemCols := 4
	got := []int{}
	if err := r.q.Tables().Do(func(b flux.Table) error {
		got = append(got, len(b.Cols())-systemCols)
		b.Do(func(c flux.ColReader) error { return nil })
		return nil
	}); err != nil {
		r.t.Fatal(err)
	}

	if !reflect.DeepEqual(got, want) {
		r.t.Fatalf("got %v, expected %v", got, want)
	}
}

// TablesN returns the number of tables for the result.
func (r *QueryResult) TablesN() int {
	var total int
	r.q.Tables().Do(func(b flux.Table) error {
		total++
		b.Do(func(c flux.ColReader) error { return nil })
		return nil
	})
	return total
}

// MustExecuteQuery executes the provided query panicking if an error is encountered.
// Callers of MustExecuteQuery must call Done on the returned QueryResults.
func (p *Launcher) MustExecuteQuery(orgID platform.ID, query string, auth *platform.Authorization) *QueryResults {
	results, err := p.ExecuteQuery(orgID, query, auth)
	if err != nil {
		panic(err)
	}
	return results
}

// ExecuteQuery executes the provided query against the ith query node.
// Callers of ExecuteQuery must call Done on the returned QueryResults.
func (p *Launcher) ExecuteQuery(orgID platform.ID, q string, auth *platform.Authorization) (*QueryResults, error) {
	fq, err := p.QueryController().Query(context.Background(), &query.Request{
		Authorization:  auth,
		OrganizationID: orgID,
		Compiler: lang.FluxCompiler{
			Query: q,
		}})
	if err != nil {
		return nil, err
	}
	if err = fq.Err(); err != nil {
		return nil, fq.Err()
	}
	return &QueryResults{
		Results: <-fq.Ready(),
		Query:   fq,
	}, nil
}

// QueryResults wraps a set of query results with some helper methods.
type QueryResults struct {
	Results map[string]flux.Result
	Query   flux.Query
}

func (r *QueryResults) Done() {
	r.Query.Done()
}

// First returns the first QueryResult. When there are not exactly 1 table First
// will fail.
func (r *QueryResults) First(t *testing.T) *QueryResult {
	r.HasTableCount(t, 1)
	for _, result := range r.Results {
		return &QueryResult{t: t, q: result}
	}
	return nil
}

// HasTableCount asserts that there are n tables in the result.
func (r *QueryResults) HasTableCount(t *testing.T, n int) {
	if got, exp := len(r.Results), n; got != exp {
		t.Fatalf("result has %d tables, expected %d. Tables: %s", got, exp, r.Names())
	}
}

// Names returns the sorted set of table names for the query results.
func (r *QueryResults) Names() []string {
	if len(r.Results) == 0 {
		return nil
	}
	names := make([]string, len(r.Results), 0)
	for k := range r.Results {
		names = append(names, k)
	}
	return names
}

// SortedNames returns the sorted set of table names for the query results.
func (r *QueryResults) SortedNames() []string {
	names := r.Names()
	sort.Strings(names)
	return names
}
