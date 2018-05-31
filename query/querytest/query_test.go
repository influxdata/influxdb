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

		ifqlName := caseName + ".ifql"
		influxqlName := caseName + ".influxql"
		t.Run(ifqlName, func(t *testing.T) {
			queryTester(t, qs, ifqlName, prefix, ".ifql")
		})
		t.Run(influxqlName, func(t *testing.T) {
			queryTranspileTester(t, influxqlTranspiler, qs, influxqlName, prefix, ".influxql")
		})
	}
}

func queryTester(t *testing.T, qs query.QueryService, name, prefix, queryExt string) {
	q, err := GetTestData(prefix, queryExt)
	if err != nil {
		t.Fatal(err)
	}

	csvOut, err := GetTestData(prefix, ".out.csv")
	if err != nil {
		t.Fatalf("error in test case %s: %s", prefix, err)
	}

	spec, err := query.Compile(context.Background(), q)
	if err != nil {
		t.Fatalf("error in test case %s: %s", prefix, err)
	}

	csvIn := prefix + ".in.csv"
	QueryTestCheckSpec(t, qs, spec, csvIn, csvOut)
}

func queryTranspileTester(t *testing.T, transpiler query.Transpiler, qs query.QueryService, name, prefix, queryExt string) {
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
		t.Fatalf("error in test case %s: %s", prefix, err)
	}

	spec, err := transpiler.Transpile(context.Background(), q)
	if err != nil {
		t.Fatalf("failed to transpile query to spec error=%s", err)
	}

	csvIn := prefix + ".in.csv"
	QueryTestCheckSpec(t, qs, spec, csvIn, csvOut)
}

func QueryTestCheckSpec(t *testing.T, qs query.QueryService, spec *query.Spec, inputFile, want string) {
	t.Helper()
	ReplaceFromSpec(spec, inputFile)

	got, err := GetQueryEncodedResults(qs, spec, inputFile)
	if err != nil {
		t.Fatalf("failed to run query error=%s", err)
	}
	if g, w := strings.TrimSpace(got), strings.TrimSpace(want); g != w {
		t.Fatalf("result not as expected want(-) got (+):\n%v", diff.LineDiff(w, g))
	}
}
