package querytest

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/influxdata/platform/query"
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
		csvIn := prefix + ".in.csv"

		csvOut, err := GetTestData(prefix, ".out.csv")
		if err != nil {
			t.Fatalf("error in test case %s: %s", prefix, err)
		}

		ifqlQuery, err := GetTestData(prefix, ".ifql")
		if err != nil {
			t.Fatalf("error in test case %s: %s", prefix, err)
		}

		ifqlSpec, err := query.Compile(context.Background(), ifqlQuery)
		if err != nil {
			t.Fatalf("error in test case %s: %s", prefix, err)
		}

		err = QueryTestCheckSpec(t, qs, ifqlSpec, caseName+".ifql", csvIn, csvOut)
		if err != nil {
			t.Errorf("failed to run ifql query spec for test case %s. error=%s", prefix, err)
		}

		influxqlQuery, err := GetTestData(prefix, ".influxql")
		if err != nil {
			t.Logf("skipping influxql for test case %s: %s", prefix, err)
		} else {
			if err != nil {
				t.Fatalf("error in test case %s: %s", prefix, err)
			}

			influxqlSpec, err := influxqlTranspiler.Transpile(context.Background(), influxqlQuery)
			if err != nil {
				t.Errorf("failed to obtain transpiled influxql query spec for test case %s. error=%s", prefix, err)
			}

			err = QueryTestCheckSpec(t, qs, influxqlSpec, "influxql::"+caseName, csvIn, csvOut)
			if err != nil {
				t.Errorf("failed to run influxql query spec for test case %s. error=%s", prefix, err)
			}
		}

	}
}

func QueryTestCheckSpec(t *testing.T, qs *query.QueryServiceBridge, spec *query.Spec, caseName, inputFile, want string) error {
	t.Helper()
	ReplaceFromSpec(spec, inputFile)

	//log.Println("QueryTestCheckSpec", query.Formatted(spec, query.FmtJSON))
	log.Println("QueryTestCheckSpec")

	got, err := GetQueryEncodedResults(qs, spec, inputFile)
	if err != nil {
		t.Errorf("case %s: failed to run query spec error=%s", caseName, err)
		return err
	}
	if g, w := strings.TrimSpace(got), strings.TrimSpace(want); g != w {
		t.Errorf("case %s: result not as expected want(-) got (+):\n%v", caseName, diff.LineDiff(w, g))
	}
	return nil
}
