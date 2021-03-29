package influxql_test

import (
	"bufio"
	"bytes"
	"context"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
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
	fluxquerytest "github.com/influxdata/flux/querytest"
	platform "github.com/influxdata/influxdb/v2"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/influxql"
	"github.com/influxdata/influxdb/v2/query/querytest"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
)

const generatedInfluxQLDataDir = "testdata"

var dbrpMappingSvcE2E = &mock.DBRPMappingServiceV2{}

func init() {
	mapping := platform.DBRPMappingV2{
		Database:        "db0",
		RetentionPolicy: "autogen",
		Default:         true,
		OrganizationID:  platformtesting.MustIDBase16("cadecadecadecade"),
		BucketID:        platformtesting.MustIDBase16("da7aba5e5eedca5e"),
	}
	dbrpMappingSvcE2E.FindByIDFn = func(ctx context.Context, orgID, id platform2.ID) (*platform.DBRPMappingV2, error) {
		return &mapping, nil
	}
	dbrpMappingSvcE2E.FindManyFn = func(ctx context.Context, filter platform.DBRPMappingFilterV2, opt ...platform.FindOptions) ([]*platform.DBRPMappingV2, int, error) {
		return []*platform.DBRPMappingV2{&mapping}, 1, nil
	}
}

var skipTests = map[string]string{
	"hardcoded_literal_1":      "transpiler count query is off by 1 https://github.com/influxdata/influxdb/issues/10744",
	"hardcoded_literal_3":      "transpiler count query is off by 1 https://github.com/influxdata/influxdb/issues/10744",
	"fuzz_join_within_cursor":  "transpiler does not implement joining fields within a cursor https://github.com/influxdata/influxdb/issues/10743",
	"derivative_count":         "add derivative support to the transpiler https://github.com/influxdata/influxdb/issues/10759",
	"derivative_first":         "add derivative support to the transpiler https://github.com/influxdata/influxdb/issues/10759",
	"derivative_last":          "add derivative support to the transpiler https://github.com/influxdata/influxdb/issues/10759",
	"derivative_max":           "add derivative support to the transpiler https://github.com/influxdata/influxdb/issues/10759",
	"derivative_mean":          "add derivative support to the transpiler https://github.com/influxdata/influxdb/issues/10759",
	"derivative_median":        "add derivative support to the transpiler https://github.com/influxdata/influxdb/issues/10759",
	"derivative_min":           "add derivative support to the transpiler https://github.com/influxdata/influxdb/issues/10759",
	"derivative_mode":          "add derivative support to the transpiler https://github.com/influxdata/influxdb/issues/10759",
	"derivative_percentile_10": "add derivative support to the transpiler https://github.com/influxdata/influxdb/issues/10759",
	"derivative_percentile_50": "add derivative support to the transpiler https://github.com/influxdata/influxdb/issues/10759",
	"derivative_percentile_90": "add derivative support to the transpiler https://github.com/influxdata/influxdb/issues/10759",
	"derivative_sum":           "add derivative support to the transpiler https://github.com/influxdata/influxdb/issues/10759",
	"regex_measurement_0":      "Transpiler: regex on measurements not evaluated https://github.com/influxdata/influxdb/issues/10740",
	"regex_measurement_1":      "Transpiler: regex on measurements not evaluated https://github.com/influxdata/influxdb/issues/10740",
	"regex_measurement_2":      "Transpiler: regex on measurements not evaluated https://github.com/influxdata/influxdb/issues/10740",
	"regex_measurement_3":      "Transpiler: regex on measurements not evaluated https://github.com/influxdata/influxdb/issues/10740",
	"regex_measurement_4":      "Transpiler: regex on measurements not evaluated https://github.com/influxdata/influxdb/issues/10740",
	"regex_measurement_5":      "Transpiler: regex on measurements not evaluated https://github.com/influxdata/influxdb/issues/10740",
	"regex_tag_0":              "Transpiler: Returns results in wrong sort order for regex filter on tags https://github.com/influxdata/influxdb/issues/10739",
	"regex_tag_1":              "Transpiler: Returns results in wrong sort order for regex filter on tags https://github.com/influxdata/influxdb/issues/10739",
	"regex_tag_2":              "Transpiler: Returns results in wrong sort order for regex filter on tags https://github.com/influxdata/influxdb/issues/10739",
	"regex_tag_3":              "Transpiler: Returns results in wrong sort order for regex filter on tags https://github.com/influxdata/influxdb/issues/10739",
	"explicit_type_0":          "Transpiler should remove _start column https://github.com/influxdata/influxdb/issues/10742",
	"explicit_type_1":          "Transpiler should remove _start column https://github.com/influxdata/influxdb/issues/10742",
	"fills_0":                  "need fill/Interpolate function https://github.com/influxdata/flux/issues/436",
	"random_math_0":            "transpiler does not implement joining fields within a cursor https://github.com/influxdata/influxdb/issues/10743",
	"selector_0":               "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"selector_1":               "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"selector_2":               "Transpiler: first function uses different series than influxQL https://github.com/influxdata/influxdb/issues/10737",
	"selector_6":               "Transpiler: first function uses different series than influxQL https://github.com/influxdata/influxdb/issues/10737",
	"selector_7":               "Transpiler: first function uses different series than influxQL https://github.com/influxdata/influxdb/issues/10737",
	"series_agg_3":             "Transpiler: Implement elapsed https://github.com/influxdata/influxdb/issues/10733",
	"series_agg_4":             "Transpiler: Implement cumulative_sum https://github.com/influxdata/influxdb/issues/10732",
	"series_agg_5":             "add derivative support to the transpiler https://github.com/influxdata/influxdb/issues/10759",
	"series_agg_6":             "Transpiler: Implement non_negative_derivative https://github.com/influxdata/influxdb/issues/10731",
	"Subquery_0":               "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"Subquery_1":               "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"Subquery_2":               "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"Subquery_3":               "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"Subquery_4":               "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"Subquery_5":               "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"NestedSubquery_0":         "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"NestedSubquery_1":         "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"NestedSubquery_2":         "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"NestedSubquery_3":         "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"SimulatedHTTP_0":          "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"SimulatedHTTP_1":          "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"SimulatedHTTP_2":          "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"SimulatedHTTP_3":          "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"SimulatedHTTP_4":          "Implement subqueries in the transpiler https://github.com/influxdata/influxdb/issues/10660",
	"SelectorMath_0":           "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_1":           "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_2":           "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_3":           "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_4":           "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_5":           "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_6":           "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_7":           "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_8":           "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_9":           "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_10":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_11":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_12":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_13":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_14":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_15":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_16":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_17":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_18":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_19":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_20":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_21":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_22":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_23":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_24":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_25":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_26":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_27":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_28":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_29":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_30":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"SelectorMath_31":          "Transpiler: unimplemented functions: top and bottom https://github.com/influxdata/influxdb/issues/10738",
	"ands":                     "algo-w: https://github.com/influxdata/influxdb/issues/16811",
	"ors":                      "algo-w: https://github.com/influxdata/influxdb/issues/16811",
}

var querier = fluxquerytest.NewQuerier()

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

	if err := executetest.EqualResults(exp, got); err != nil {
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

func resultsFromQuerier(querier *fluxquerytest.Querier, compiler flux.Compiler) (flux.ResultIterator, error) {
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

func influxQLCompiler(query, filename string) flux.Compiler {
	compiler := influxql.NewCompiler(dbrpMappingSvcE2E)
	compiler.Cluster = "cluster"
	compiler.DB = "db0"
	compiler.Query = query
	querytest.MakeFromInfluxJSONCompiler(compiler, filename)
	return compiler
}

func queryToJSON(querier *fluxquerytest.Querier, req *query.ProxyRequest) (io.ReadCloser, error) {
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
