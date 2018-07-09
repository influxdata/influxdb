package functions_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/andreyvit/diff"
	"github.com/influxdata/platform/query"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/query/csv"
	"github.com/influxdata/platform/query/influxql"
	"github.com/influxdata/platform/query/querytest"
)

var skipTests = map[string]string{
	"derivative":                "derivative not supported by influxql (https://github.com/influxdata/platform/issues/93)",
	"window_group_mean_ungroup": "error in influxql: failed to run query: timeValue column \"_start\" does not exist (https://github.com/influxdata/platform/issues/97)",
	"string_max":                "panic because no max() implementation for string (https://github.com/influxdata/platform/issues/352)",
	"null_as_value":             "null not supported as value in influxql (https://github.com/influxdata/platform/issues/353)",
}

// Change as needed

func Test_QueryEndToEnd(t *testing.T) {
	qs := querytest.GetQueryServiceBridge()

	influxqlTranspiler := influxql.NewTranspiler()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "testdata")

	fluxFiles, err := filepath.Glob(filepath.Join(path, "*.flux"))
	if err != nil {
		t.Fatalf("error searching for Flux files: %s", err)
	}

	for _, fluxFile := range fluxFiles {
		ext := filepath.Ext(fluxFile)
		prefix := fluxFile[0 : len(fluxFile)-len(ext)]

		_, caseName := filepath.Split(prefix)
		if reason, ok := skipTests[caseName]; ok {
			t.Run(caseName, func(t *testing.T) {
				t.Skip(reason)
			})
			continue
		}

		fluxName := caseName + ".flux"
		influxqlName := caseName + ".influxql"
		t.Run(fluxName, func(t *testing.T) {
			queryTester(t, qs, prefix, ".flux")
		})
		t.Run(influxqlName, func(t *testing.T) {
			queryTranspileTester(t, influxqlTranspiler, qs, prefix, ".influxql")
		})
	}
}

func queryTester(t *testing.T, qs query.QueryService, prefix, queryExt string) {
	q, err := querytest.GetTestData(prefix, queryExt)
	if err != nil {
		t.Fatal(err)
	}

	csvOut, err := querytest.GetTestData(prefix, ".out.csv")
	if err != nil {
		t.Fatal(err)
	}

	spec, err := query.Compile(context.Background(), q)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	csvIn := prefix + ".in.csv"
	enc := csv.NewMultiResultEncoder(csv.DefaultEncoderConfig())
	QueryTestCheckSpec(t, qs, spec, csvIn, csvOut, enc)
}

func queryTranspileTester(t *testing.T, transpiler query.Transpiler, qs query.QueryService, prefix, queryExt string) {
	q, err := querytest.GetTestData(prefix, queryExt)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skip("query missing")
		} else {
			t.Fatal(err)
		}
	}

	csvOut, err := querytest.GetTestData(prefix, ".out.csv")
	if err != nil {
		t.Fatal(err)
	}

	spec, err := transpiler.Transpile(context.Background(), q)
	if err != nil {
		t.Fatalf("failed to transpile: %v", err)
	}

	csvIn := prefix + ".in.csv"
	enc := csv.NewMultiResultEncoder(csv.DefaultEncoderConfig())
	QueryTestCheckSpec(t, qs, spec, csvIn, csvOut, enc)

	enc = influxql.NewMultiResultEncoder()
	jsonOut, err := querytest.GetTestData(prefix, ".out.json")
	if err != nil {
		t.Logf("skipping json evaluation: %s", err)
		return
	}
	QueryTestCheckSpec(t, qs, spec, csvIn, jsonOut, enc)
}

func QueryTestCheckSpec(t *testing.T, qs query.QueryService, spec *query.Spec, inputFile, want string, enc query.MultiResultEncoder) {
	t.Helper()
	querytest.ReplaceFromSpec(spec, inputFile)

	got, err := querytest.GetQueryEncodedResults(qs, spec, inputFile, enc)
	if err != nil {
		t.Errorf("failed to run query: %v", err)
	}
	if g, w := strings.TrimSpace(got), strings.TrimSpace(want); g != w {
		t.Errorf("result not as expected want(-) got (+):\n%v", diff.LineDiff(w, g))
	}
}
