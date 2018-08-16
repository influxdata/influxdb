package functions_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
	"github.com/influxdata/platform/query"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/query/csv"
	"github.com/influxdata/platform/query/influxql"
	"github.com/influxdata/platform/query/querytest"

	"github.com/andreyvit/diff"
)

var dbrpMappingSvc = mock.NewDBRPMappingService()

func init() {
	mapping := platform.DBRPMapping{
		Cluster:         "cluster",
		Database:        "db0",
		RetentionPolicy: "autogen",
		Default:         true,
		OrganizationID:  platform.ID("org"),
		BucketID:        platform.ID("bucket"),
	}
	dbrpMappingSvc.FindByFn = func(ctx context.Context, cluster string, db string, rp string) (*platform.DBRPMapping, error) {
		return &mapping, nil
	}
	dbrpMappingSvc.FindFn = func(ctx context.Context, filter platform.DBRPMappingFilter) (*platform.DBRPMapping, error) {
		return &mapping, nil
	}
	dbrpMappingSvc.FindManyFn = func(ctx context.Context, filter platform.DBRPMappingFilter, opt ...platform.FindOptions) ([]*platform.DBRPMapping, int, error) {
		return []*platform.DBRPMapping{&mapping}, 1, nil
	}
}

var skipTests = map[string]string{
	"derivative":                "derivative not supported by influxql (https://github.com/influxdata/platform/issues/93)",
	"filter_by_tags":            "arbitrary filtering not supported by influxql (https://github.com/influxdata/platform/issues/94)",
	"window_group_mean_ungroup": "error in influxql: failed to run query: timeValue column \"_start\" does not exist (https://github.com/influxdata/platform/issues/97)",
	"string_max":                "error: invalid use of function: *functions.MaxSelector has no implementation for type string (https://github.com/influxdata/platform/issues/224)",
	"null_as_value":             "null not supported as value in influxql (https://github.com/influxdata/platform/issues/353)",
	"difference_panic":          "difference() panics when no table is supplied",
	"string_interp":             "string interpolation not working as expected in flux (https://github.com/influxdata/platform/issues/404)",
}

var pqs = querytest.GetProxyQueryServiceBridge()

func withEachFluxFile(t testing.TB, fn func(prefix, caseName string)) {
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
		fn(prefix, caseName)
	}
}

func Test_QueryEndToEnd(t *testing.T) {
	withEachFluxFile(t, func(prefix, caseName string) {
		reason, skip := skipTests[caseName]

		fluxName := caseName + ".flux"
		influxqlName := caseName + ".influxql"
		t.Run(fluxName, func(t *testing.T) {
			if skip {
				t.Skip(reason)
			}
			testFlux(t, pqs, prefix, ".flux")
		})
		t.Run(influxqlName, func(t *testing.T) {
			if skip {
				t.Skip(reason)
			}
			testInfluxQL(t, pqs, prefix, ".influxql")
		})
	})
}

func Benchmark_QueryEndToEnd(b *testing.B) {
	withEachFluxFile(b, func(prefix, caseName string) {
		reason, skip := skipTests[caseName]
		if skip {
			b.Skip(reason)
		}

		fluxName := caseName + ".flux"
		influxqlName := caseName + ".influxql"
		b.Run(fluxName, func(b *testing.B) {
			if skip {
				b.Skip(reason)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				testFlux(b, pqs, prefix, ".flux")
			}
		})
		b.Run(influxqlName, func(b *testing.B) {
			if skip {
				b.Skip(reason)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				testInfluxQL(b, pqs, prefix, ".influxql")
			}
		})
	})
}

func testFlux(t testing.TB, pqs query.ProxyQueryService, prefix, queryExt string) {
	q, err := ioutil.ReadFile(prefix + queryExt)
	if err != nil {
		t.Fatal(err)
	}

	csvInFilename := prefix + ".in.csv"
	csvOut, err := ioutil.ReadFile(prefix + ".out.csv")
	if err != nil {
		t.Fatal(err)
	}

	compiler := query.FluxCompiler{
		Query: string(q),
	}
	req := &query.ProxyRequest{
		Request: query.Request{
			Compiler: querytest.FromCSVCompiler{
				Compiler:  compiler,
				InputFile: csvInFilename,
			},
		},
		Dialect: csv.DefaultDialect(),
	}

	QueryTestCheckSpec(t, pqs, req, string(csvOut))
}

func testInfluxQL(t testing.TB, pqs query.ProxyQueryService, prefix, queryExt string) {
	q, err := ioutil.ReadFile(prefix + queryExt)
	if err != nil {
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		t.Skip("influxql query is missing")
	}

	csvInFilename := prefix + ".in.csv"
	csvOut, err := ioutil.ReadFile(prefix + ".out.csv")
	if err != nil {
		t.Fatal(err)
	}

	compiler := influxql.NewCompiler(dbrpMappingSvc)
	compiler.Cluster = "cluster"
	compiler.DB = "db0"
	compiler.Query = string(q)
	req := &query.ProxyRequest{
		Request: query.Request{
			Compiler: querytest.FromCSVCompiler{
				Compiler:  compiler,
				InputFile: csvInFilename,
			},
		},
		Dialect: csv.DefaultDialect(),
	}
	QueryTestCheckSpec(t, pqs, req, string(csvOut))

	// Rerun test for InfluxQL JSON dialect
	req.Dialect = new(influxql.Dialect)

	jsonOut, err := ioutil.ReadFile(prefix + ".out.json")
	if err != nil {
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		t.Skip("influxql expected json is missing")
	}
	QueryTestCheckSpec(t, pqs, req, string(jsonOut))
}

func QueryTestCheckSpec(t testing.TB, pqs query.ProxyQueryService, req *query.ProxyRequest, want string) {
	t.Helper()

	var buf bytes.Buffer
	_, err := pqs.Query(context.Background(), &buf, req)
	if err != nil {
		t.Errorf("failed to run query: %v", err)
		return
	}

	got := buf.String()

	if g, w := strings.TrimSpace(got), strings.TrimSpace(want); g != w {
		t.Errorf("result not as expected want(-) got (+):\n%v", diff.LineDiff(w, g))
	}
}
