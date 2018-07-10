package functions_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/influxdata/platform/query"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/query/csv"
	"github.com/influxdata/platform/query/influxql"
	"github.com/influxdata/platform/query/querytest"

	"github.com/andreyvit/diff"
)

var skipTests = map[string]string{
	"derivative":                "derivative not supported by influxql (https://github.com/influxdata/platform/issues/93)",
	"filter_by_tags":            "arbitrary filtering not supported by influxql (https://github.com/influxdata/platform/issues/94)",
	"window_group_mean_ungroup": "error in influxql: failed to run query: timeValue column \"_start\" does not exist (https://github.com/influxdata/platform/issues/97)",
	"string_max":                "error: invalid use of function: *functions.MaxSelector has no implementation for type string",
	"null_as_value":             "null not supported as value in influxql (https://github.com/influxdata/platform/issues/353)",
}

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
			err := queryTester(t, qs, prefix, ".flux")
			if err != nil {
				qs = querytest.GetQueryServiceBridge()
			}
		})
		t.Run(influxqlName, func(t *testing.T) {
			err := queryTranspileTester(t, influxqlTranspiler, qs, prefix, ".influxql")
			if err != nil {
				qs = querytest.GetQueryServiceBridge()
			}
		})
	}
}

func queryTester(t *testing.T, qs query.QueryService, prefix, queryExt string) error {
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

	return QueryTestCheckSpec(t, qs, spec, csvIn, csvOut, enc)
}

func queryTranspileTester(t *testing.T, transpiler query.Transpiler, qs query.QueryService, prefix, queryExt string) error {
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
		return nil
	}
	return QueryTestCheckSpec(t, qs, spec, csvIn, jsonOut, enc)
}

func QueryTestCheckSpec(t *testing.T, qs query.QueryService, spec *query.Spec, inputFile, want string, enc query.MultiResultEncoder) error {
	t.Helper()

	querytest.ReplaceFromSpec(spec, inputFile)

	got, err := querytest.GetQueryEncodedResults(qs, spec, inputFile, enc)
	if err != nil {
		t.Errorf("failed to run query: %v", err)
		return fmt.Errorf("Error running query")
	}

	if g, w := strings.TrimSpace(got), strings.TrimSpace(want); g != w {
		t.Errorf("result not as expected want(-) got (+):\n%v", diff.LineDiff(w, g))
	}
	return nil
}
