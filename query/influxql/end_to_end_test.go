package influxql_test

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	ifql "github.com/influxdata/flux/influxql"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/querytest"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/query"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/query/influxql"
	platformtesting "github.com/influxdata/influxdb/testing"
)

const generatedInfluxQLDataDir = "testdata"

var dbrpMappingSvcE2E = mock.NewDBRPMappingService()

func init() {
	mapping := platform.DBRPMapping{
		Cluster:         "cluster",
		Database:        "db0",
		RetentionPolicy: "autogen",
		Default:         true,
		OrganizationID:  platformtesting.MustIDBase16("cadecadecadecade"),
		BucketID:        platformtesting.MustIDBase16("da7aba5e5eedca5e"),
	}
	dbrpMappingSvcE2E.FindByFn = func(ctx context.Context, cluster string, db string, rp string) (*platform.DBRPMapping, error) {
		return &mapping, nil
	}
	dbrpMappingSvcE2E.FindFn = func(ctx context.Context, filter platform.DBRPMappingFilter) (*platform.DBRPMapping, error) {
		return &mapping, nil
	}
	dbrpMappingSvcE2E.FindManyFn = func(ctx context.Context, filter platform.DBRPMappingFilter, opt ...platform.FindOptions) ([]*platform.DBRPMapping, int, error) {
		return []*platform.DBRPMapping{&mapping}, 1, nil
	}
}

var skipTests = map[string]string{
	"hardcoded_literal_1":      "transpiler count query is off by 1 (https://github.com/influxdata/platform/issues/1278)",
	"hardcoded_literal_3":      "transpiler count query is off by 1 (https://github.com/influxdata/platform/issues/1278)",
	"fuzz_join_within_cursor":  "transpiler does not implement joining fields within a cursor (https://github.com/influxdata/platform/issues/1340)",
	"derivative_count":         "add derivative support to the transpiler (https://github.com/influxdata/platform/issues/93)",
	"derivative_first":         "add derivative support to the transpiler (https://github.com/influxdata/platform/issues/93)",
	"derivative_last":          "add derivative support to the transpiler (https://github.com/influxdata/platform/issues/93)",
	"derivative_max":           "add derivative support to the transpiler (https://github.com/influxdata/platform/issues/93)",
	"derivative_mean":          "add derivative support to the transpiler (https://github.com/influxdata/platform/issues/93)",
	"derivative_median":        "add derivative support to the transpiler (https://github.com/influxdata/platform/issues/93)",
	"derivative_min":           "add derivative support to the transpiler (https://github.com/influxdata/platform/issues/93)",
	"derivative_mode":          "add derivative support to the transpiler (https://github.com/influxdata/platform/issues/93)",
	"derivative_percentile_10": "add derivative support to the transpiler (https://github.com/influxdata/platform/issues/93)",
	"derivative_percentile_50": "add derivative support to the transpiler (https://github.com/influxdata/platform/issues/93)",
	"derivative_percentile_90": "add derivative support to the transpiler (https://github.com/influxdata/platform/issues/93)",
	"derivative_sum":           "add derivative support to the transpiler (https://github.com/influxdata/platform/issues/93)",
	"regex_measurement_0":      "Transpiler: regex on measurements not evaluated (https://github.com/influxdata/platform/issues/1592)",
	"regex_measurement_1":      "Transpiler: regex on measurements not evaluated (https://github.com/influxdata/platform/issues/1592)",
	"regex_measurement_2":      "Transpiler: regex on measurements not evaluated (https://github.com/influxdata/platform/issues/1592)",
	"regex_measurement_3":      "Transpiler: regex on measurements not evaluated (https://github.com/influxdata/platform/issues/1592)",
	"regex_measurement_4":      "Transpiler: regex on measurements not evaluated (https://github.com/influxdata/platform/issues/1592)",
	"regex_measurement_5":      "Transpiler: regex on measurements not evaluated (https://github.com/influxdata/platform/issues/1592)",
	"regex_tag_0":              "Transpiler: Returns results in wrong sort order for regex filter on tags (https://github.com/influxdata/platform/issues/1596)",
	"regex_tag_1":              "Transpiler: Returns results in wrong sort order for regex filter on tags (https://github.com/influxdata/platform/issues/1596)",
	"regex_tag_2":              "Transpiler: Returns results in wrong sort order for regex filter on tags (https://github.com/influxdata/platform/issues/1596)",
	"regex_tag_3":              "Transpiler: Returns results in wrong sort order for regex filter on tags (https://github.com/influxdata/platform/issues/1596)",
	"explicit_type_0":          "Transpiler should remove _start column (https://github.com/influxdata/platform/issues/1360)",
	"explicit_type_1":          "Transpiler should remove _start column (https://github.com/influxdata/platform/issues/1360)",
	"fills_0":                  "need fill/Interpolate function (https://github.com/influxdata/platform/issues/272)",
	"random_math_0":            "transpiler does not implement joining fields within a cursor (https://github.com/influxdata/platform/issues/1340)",
	"selector_0":               "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"selector_1":               "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"selector_2":               "Transpiler: first function uses different series than influxQL (https://github.com/influxdata/platform/issues/1605)",
	"selector_6":               "Transpiler: first function uses different series than influxQL (https://github.com/influxdata/platform/issues/1605)",
	"selector_7":               "Transpiler: first function uses different series than influxQL (https://github.com/influxdata/platform/issues/1605)",
	"selector_8":               "Transpiler: selectors with group by produce different time values than influxQL (https://github.com/influxdata/platform/issues/1606)",
	"selector_9":               "Transpiler: selectors with group by produce different time values than influxQL (https://github.com/influxdata/platform/issues/1606)",
	"series_agg_0":             "Transpiler: Implement difference (https://github.com/influxdata/platform/issues/1609)",
	"series_agg_1":             "Transpiler: Implement stddev (https://github.com/influxdata/platform/issues/1610)",
	"series_agg_2":             "Transpiler: Implement spread (https://github.com/influxdata/platform/issues/1611)",
	"series_agg_3":             "Transpiler: Implement elapsed (https://github.com/influxdata/platform/issues/1612)",
	"series_agg_4":             "Transpiler: Implement cumulative_sum (https://github.com/influxdata/platform/issues/1613)",
	"series_agg_5":             "add derivative support to the transpiler (https://github.com/influxdata/platform/issues/93)",
	"series_agg_6":             "Transpiler: Implement non_negative_derivative (https://github.com/influxdata/platform/issues/1614)",
	"series_agg_7":             "Transpiler should remove _start column (https://github.com/influxdata/platform/issues/1360)",
	"series_agg_8":             "Transpiler should remove _start column (https://github.com/influxdata/platform/issues/1360)",
	"series_agg_9":             "Transpiler should remove _start column (https://github.com/influxdata/platform/issues/1360)",
	"Subquery_0":               "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"Subquery_1":               "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"Subquery_2":               "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"Subquery_3":               "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"Subquery_4":               "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"Subquery_5":               "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"NestedSubquery_0":         "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"NestedSubquery_1":         "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"NestedSubquery_2":         "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"NestedSubquery_3":         "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"SimulatedHTTP_0":          "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"SimulatedHTTP_1":          "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"SimulatedHTTP_2":          "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"SimulatedHTTP_3":          "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"SimulatedHTTP_4":          "Implement subqueries in the transpiler (https://github.com/influxdata/platform/issues/194)",
	"SelectorMath_0":           "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_1":           "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_2":           "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_3":           "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_4":           "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_5":           "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_6":           "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_7":           "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_8":           "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_9":           "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_10":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_11":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_12":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_13":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_14":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_15":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_16":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_17":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_18":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_19":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_20":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_21":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_22":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_23":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_24":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_25":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_26":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_27":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_28":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_29":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_30":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
	"SelectorMath_31":          "Transpiler: unimplemented functions: top and bottom (https://github.com/influxdata/platform/issues/1601)",
}

var querier = querytest.NewQuerier()

func withEachInfluxQLFile(t testing.TB, fn func(prefix, caseName string)) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, generatedInfluxQLDataDir)

	influxqlFiles, err := filepath.Glob(filepath.Join(path, "*.influxql"))
	if err != nil {
		t.Fatalf("error searching for influxQL files: %s", err)
	}

	for _, influxqlFile := range influxqlFiles {
		ext := filepath.Ext(influxqlFile)
		prefix := influxqlFile[0 : len(influxqlFile)-len(ext)]
		_, caseName := filepath.Split(prefix)
		fn(prefix, caseName)
	}
}

func Test_GeneratedInfluxQLQueries(t *testing.T) {
	withEachInfluxQLFile(t, func(prefix, caseName string) {
		reason, skip := skipTests[caseName]
		influxqlName := caseName + ".influxql"
		t.Run(influxqlName, func(t *testing.T) {
			if skip {
				t.Skip(reason)
			}
			testGeneratedInfluxQL(t, prefix, ".influxql")
		})
	})
}

func testGeneratedInfluxQL(t testing.TB, prefix, queryExt string) {
	q, err := ioutil.ReadFile(prefix + queryExt)
	if err != nil {
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		t.Skip("influxql query is missing")
	}

	inFile := prefix + ".in.json"
	outFile := prefix + ".out.json"

	out, err := jsonToResultIterator(outFile)
	if err != nil {
		t.Fatalf("failed to read expected JSON results: %v", err)
	}
	defer out.Release()

	var exp []flux.Result
	for out.More() {
		exp = append(exp, out.Next())
	}

	res, err := resultsFromQuerier(querier, influxQLCompiler(string(q), inFile))
	if err != nil {
		t.Fatalf("failed to run query: %v", err)
	}
	defer res.Release()

	var got []flux.Result
	for res.More() {
		got = append(got, res.Next())
	}

	if ok, err := executetest.EqualResults(exp, got); !ok {
		t.Errorf("result not as expected: %v", err)

		expBuffer := new(bytes.Buffer)
		for _, e := range exp {
			e.Tables().Do(func(tbl flux.Table) error {
				_, err := execute.NewFormatter(tbl, nil).WriteTo(expBuffer)
				return err
			})
		}

		gotBuffer := new(bytes.Buffer)
		for _, e := range got {
			e.Tables().Do(func(tbl flux.Table) error {
				_, err := execute.NewFormatter(tbl, nil).WriteTo(gotBuffer)
				return err
			})
		}
		t.Logf("\nExpected Tables:\n%s\nActualTables:\n%s\n", expBuffer.String(), gotBuffer.String())
	}
}

func resultsFromQuerier(querier *querytest.Querier, compiler flux.Compiler) (flux.ResultIterator, error) {
	req := &query.ProxyRequest{
		Request: query.Request{
			Compiler: compiler,
		},
		Dialect: new(influxql.Dialect),
	}
	jsonBuf, err := queryToJSON(querier, req)
	if err != nil {
		return nil, err
	}
	decoder := ifql.NewResultDecoder(new(memory.Allocator))
	return decoder.Decode(ioutil.NopCloser(jsonBuf))
}

func influxQLCompiler(query, filename string) querytest.FromInfluxJSONCompiler {
	compiler := influxql.NewCompiler(dbrpMappingSvcE2E)
	compiler.Cluster = "cluster"
	compiler.DB = "db0"
	compiler.Query = query
	return querytest.FromInfluxJSONCompiler{
		Compiler:  compiler,
		InputFile: filename,
	}
}

func queryToJSON(querier *querytest.Querier, req *query.ProxyRequest) (io.ReadCloser, error) {
	var buf bytes.Buffer
	_, err := querier.Query(context.Background(), &buf, req.Request.Compiler, req.Dialect)
	if err != nil {
		return nil, err
	}
	return ioutil.NopCloser(&buf), nil
}

func jsonToResultIterator(file string) (flux.ResultIterator, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	// Reader for influxql json file
	jsonReader := bufio.NewReaderSize(f, 8196)

	// InfluxQL json -> Flux tables decoder
	decoder := ifql.NewResultDecoder(new(memory.Allocator))

	// Decode json into Flux tables
	results, err := decoder.Decode(ioutil.NopCloser(jsonReader))
	if err != nil {
		return nil, err
	}
	return results, nil
}
