package testing_test

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/flux/stdlib"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/query"
	itesting "github.com/influxdata/influxdb/query/stdlib/testing"

	_ "github.com/influxdata/flux/stdlib"           // Import the built-in functions
	_ "github.com/influxdata/influxdb/query/stdlib" // Import the stdlib
)

// Default context.
var ctx = context.Background()

func init() {
	flux.FinalizeBuiltIns()
}

func TestFluxEndToEnd(t *testing.T) {
	t.Skip("flaky test: https://github.com/influxdata/influxdb/issues/14193")
	runEndToEnd(t, stdlib.FluxTestPackages)
}
func BenchmarkFluxEndToEnd(b *testing.B) {
	b.Skip("flaky test: https://github.com/influxdata/influxdb/issues/14193")
	benchEndToEnd(b, stdlib.FluxTestPackages)
}

func runEndToEnd(t *testing.T, pkgs []*ast.Package) {
	l := launcher.RunTestLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)
	for _, pkg := range pkgs {
		pkg := pkg.Copy().(*ast.Package)
		name := pkg.Files[0].Name
		t.Run(name, func(t *testing.T) {
			if reason, ok := itesting.FluxEndToEndSkipList[strings.TrimSuffix(name, ".flux")]; ok {
				t.Skip(reason)
			}
			testFlux(t, l, pkg)
		})
	}
}

func benchEndToEnd(b *testing.B, pkgs []*ast.Package) {
	l := launcher.RunTestLauncherOrFail(b, ctx)
	l.SetupOrFail(b)
	defer l.ShutdownOrFail(b, ctx)
	for _, pkg := range pkgs {
		pkg := pkg.Copy().(*ast.Package)
		name := pkg.Files[0].Name
		b.Run(name, func(b *testing.B) {
			if reason, ok := itesting.FluxEndToEndSkipList[strings.TrimSuffix(name, ".flux")]; ok {
				b.Skip(reason)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				testFlux(b, l, pkg)
			}
		})
	}
}

var optionsSource = `
import "testing"
import c "csv"

// Options bucket and org are defined dynamically per test

option testing.loadStorage = (csv) => {
	c.from(csv: csv) |> to(bucket: bucket, org: org)
	return from(bucket: bucket)
}
`
var optionsAST *ast.File

func init() {
	pkg := parser.ParseSource(optionsSource)
	if ast.Check(pkg) > 0 {
		panic(ast.GetError(pkg))
	}
	optionsAST = pkg.Files[0]
}

func testFlux(t testing.TB, l *launcher.TestLauncher, pkg *ast.Package) {

	// Query server to ensure write persists.

	b := &platform.Bucket{
		OrgID:           l.Org.ID,
		Name:            t.Name(),
		RetentionPeriod: 0,
	}

	s := l.BucketService()
	if err := s.CreateBucket(context.Background(), b); err != nil {
		t.Fatal(err)
	}

	// Define bucket and org options
	bucketOpt := &ast.OptionStatement{
		Assignment: &ast.VariableAssignment{
			ID:   &ast.Identifier{Name: "bucket"},
			Init: &ast.StringLiteral{Value: b.Name},
		},
	}
	orgOpt := &ast.OptionStatement{
		Assignment: &ast.VariableAssignment{
			ID:   &ast.Identifier{Name: "org"},
			Init: &ast.StringLiteral{Value: l.Org.Name},
		},
	}
	options := optionsAST.Copy().(*ast.File)
	options.Body = append([]ast.Statement{bucketOpt, orgOpt}, options.Body...)

	// Add options to pkg
	pkg.Files = append(pkg.Files, options)

	// Add testing.inspect call to ensure the data is loaded
	inspectCalls := stdlib.TestingInspectCalls(pkg)
	pkg.Files = append(pkg.Files, inspectCalls)

	req := &query.Request{
		OrganizationID: l.Org.ID,
		Compiler:       lang.ASTCompiler{AST: pkg},
	}
	if r, err := l.FluxQueryService().Query(ctx, req); err != nil {
		t.Fatal(err)
	} else {
		for r.More() {
			v := r.Next()
			if err := v.Tables().Do(func(tbl flux.Table) error {
				return nil
			}); err != nil {
				t.Error(err)
			}
		}
	}

	// quirk: our execution engine doesn't guarantee the order of execution for disconnected DAGS
	// so that our function-with-side effects call to `to` may run _after_ the test instead of before.
	// running twice makes sure that `to` happens at least once before we run the test.
	// this time we use a call to `run` so that the assertion error is triggered
	runCalls := stdlib.TestingRunCalls(pkg)
	pkg.Files[len(pkg.Files)-1] = runCalls
	r, err := l.FluxQueryService().Query(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	for r.More() {
		v := r.Next()
		if err := v.Tables().Do(func(tbl flux.Table) error {
			return nil
		}); err != nil {
			t.Error(err)
		}
	}
	if err := r.Err(); err != nil {
		t.Error(err)
		// Replace the testing.run calls with testing.inspect calls.
		pkg.Files[len(pkg.Files)-1] = inspectCalls
		r, err := l.FluxQueryService().Query(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		defer func() {
			if t.Failed() {
				scanner := bufio.NewScanner(&out)
				for scanner.Scan() {
					t.Log(scanner.Text())
				}
			}
		}()
		for r.More() {
			v := r.Next()
			err := execute.FormatResult(&out, v)
			if err != nil {
				t.Error(err)
			}
		}
		if err := r.Err(); err != nil {
			t.Error(err)
		}
	}
}
