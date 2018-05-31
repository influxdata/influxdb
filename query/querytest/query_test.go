package querytest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/influxdata/platform/query"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/query/influxql"

	"github.com/andreyvit/diff"
)

var skipTests = map[string]string{
	"derivative":                "derivative not supported by influxql (https://github.com/influxdata/platform/issues/93)",
	"filter_by_tags":            "arbitrary filtering not supported by influxql (https://github.com/influxdata/platform/issues/94)",
	"group_ungroup":             "influxql/ifql disagreement on keycols (https://github.com/influxdata/platform/issues/95)",
	"window":                    "ordering of results differs between queries (https://github.com/influxdata/platform/issues/96)",
	"window_group_mean_ungroup": "error in influxql: failed to run query: timeValue column \"_start\" does not exist (https://github.com/influxdata/platform/issues/97)",
}

func Test_QueryEndToEnd(t *testing.T) {
	qs := GetQueryServiceBridge()

	influxqlTranspiler := influxql.NewTranspiler()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "test_cases")
	if err != nil {
		t.Fatal(err)
	}

	ifqlFiles, err := filepath.Glob(filepath.Join(path, "*.ifql"))
	if err != nil {
		t.Fatalf("error searching for ifql files: %s", err)
	}

	for _, ifqlFile := range ifqlFiles {
		ext := filepath.Ext(ifqlFile)
		prefix := ifqlFile[0 : len(ifqlFile)-len(ext)]

		_, caseName := filepath.Split(prefix)
		if reason, ok := skipTests[caseName]; ok {
			t.Run(caseName, func(t *testing.T) {
				t.Skip(reason)
			})
			continue
		}

		ifqlName := caseName + ".ifql"
		influxqlName := caseName + ".influxql"
		t.Run(ifqlName, func(t *testing.T) {
			queryTester(t, qs, prefix, ".ifql")
		})
		t.Run(influxqlName, func(t *testing.T) {
			queryTranspileTester(t, influxqlTranspiler, qs, prefix, ".influxql")
		})
	}
}

func queryTester(t *testing.T, qs query.QueryService, prefix, queryExt string) {
	q, err := GetTestData(prefix, queryExt)
	if err != nil {
		t.Fatal(err)
	}

	csvOut, err := GetTestData(prefix, ".out.csv")
	if err != nil {
		t.Fatal(err)
	}

	spec, err := query.Compile(context.Background(), q)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}

	csvIn := prefix + ".in.csv"
	QueryTestCheckSpec(t, qs, spec, csvIn, csvOut)
}

func queryTranspileTester(t *testing.T, transpiler query.Transpiler, qs query.QueryService, prefix, queryExt string) {
	q, err := GetTestData(prefix, queryExt)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skip("query missing")
		} else {
			t.Fatal(err)
		}
	}

	csvOut, err := GetTestData(prefix, ".out.csv")
	if err != nil {
		t.Fatal(err)
	}

	spec, err := transpiler.Transpile(context.Background(), q)
	if err != nil {
		t.Fatalf("failed to transpile: %v", err)
	}

	csvIn := prefix + ".in.csv"
	QueryTestCheckSpec(t, qs, spec, csvIn, csvOut)
}

func QueryTestCheckSpec(t *testing.T, qs query.QueryService, spec *query.Spec, inputFile, want string) {
	t.Helper()
	ReplaceFromSpec(spec, inputFile)

	got, err := GetQueryEncodedResults(qs, spec, inputFile)
	if err != nil {
		t.Fatalf("failed to run query: %v", err)
	}
	if g, w := strings.TrimSpace(got), strings.TrimSpace(want); g != w {
		t.Fatalf("result not as expected want(-) got (+):\n%v", diff.LineDiff(w, g))
	}
}
