package testing_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/flux/stdlib"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/kit/feature/override"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query"
	_ "github.com/influxdata/influxdb/v2/query/stdlib"
	itesting "github.com/influxdata/influxdb/v2/query/stdlib/testing" // Import the stdlib
)

// Flagger for end-to-end test cases. This flagger contains a pointer to a
// single struct instance that all the test cases will consult. It will return flags
// based on the contents of FluxEndToEndFeatureFlags and the currently active
// test case. This works only because tests are serialized. We can set the
// current test case in the common flagger state, then run the test. If we were
// to run tests in parallel we would need to create multiple users and assign
// them different flags combinations, then run the tests under different users.

type Flagger struct {
	flaggerState *FlaggerState
}

type FlaggerState struct {
	Path           string
	Name           string
	FeatureFlags   itesting.PerTestFeatureFlagMap
	DefaultFlagger feature.Flagger
}

func newFlagger(featureFlagMap itesting.PerTestFeatureFlagMap) Flagger {
	flaggerState := &FlaggerState{}
	flaggerState.FeatureFlags = featureFlagMap
	flaggerState.DefaultFlagger = feature.DefaultFlagger()
	return Flagger{flaggerState}
}

func (f Flagger) SetActiveTestCase(path string, name string) {
	f.flaggerState.Path = path
	f.flaggerState.Name = name
}

func (f Flagger) Flags(ctx context.Context, _f ...feature.Flag) (map[string]interface{}, error) {
	// If an override is set for the test case, construct an override flagger
	// and use it's computed flags.
	overrides := f.flaggerState.FeatureFlags[f.flaggerState.Path][f.flaggerState.Name]
	if overrides != nil {
		f, err := override.Make(overrides, nil)
		if err != nil {
			panic("failed to construct override flagger, probably an invalid flag in FluxEndToEndFeatureFlags")
		}
		return f.Flags(ctx)
	}

	// Otherwise use flags from a default flagger.
	return f.flaggerState.DefaultFlagger.Flags(ctx)
}

// Default context.
var ctx = influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(true, nil))

func init() {
	runtime.FinalizeBuiltIns()
}

func TestFluxEndToEnd(t *testing.T) {
	runEndToEnd(t, stdlib.FluxTestPackages)
}
func BenchmarkFluxEndToEnd(b *testing.B) {
	benchEndToEnd(b, stdlib.FluxTestPackages)
}

func runEndToEnd(t *testing.T, pkgs []*ast.Package) {
	flagger := newFlagger(itesting.FluxEndToEndFeatureFlags)
	l := launcher.RunTestLauncherOrFail(t, ctx, flagger)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)
	for _, pkg := range pkgs {
		test := func(t *testing.T, f func(t *testing.T)) {
			t.Run(pkg.Path, f)
		}
		if pkg.Path == "universe" {
			test = func(t *testing.T, f func(t *testing.T)) {
				f(t)
			}
		}

		test(t, func(t *testing.T) {
			for _, file := range pkg.Files {
				name := strings.TrimSuffix(file.Name, "_test.flux")
				t.Run(name, func(t *testing.T) {
					if reason, ok := itesting.FluxEndToEndSkipList[pkg.Path][name]; ok {
						t.Skip(reason)
					}

					flagger.SetActiveTestCase(pkg.Path, name)
					testFlux(t, l, file)
				})
			}
		})
	}
}

func benchEndToEnd(b *testing.B, pkgs []*ast.Package) {
	// TODO(jsternberg): These benchmarks don't run properly
	// and need to be fixed. Commenting out the code for now.
	b.Skip("https://github.com/influxdata/influxdb/issues/15391")
	// l := launcher.RunTestLauncherOrFail(b, ctx)
	// l.SetupOrFail(b)
	// defer l.ShutdownOrFail(b, ctx)
	// for _, pkg := range pkgs {
	// 	pkg := pkg.Copy().(*ast.Package)
	// 	name := pkg.Files[0].Name
	// 	b.Run(name, func(b *testing.B) {
	// 		if reason, ok := itesting.FluxEndToEndSkipList[strings.TrimSuffix(name, ".flux")]; ok {
	// 			b.Skip(reason)
	// 		}
	// 		b.ResetTimer()
	// 		b.ReportAllocs()
	// 		for i := 0; i < b.N; i++ {
	// 			testFlux(b, l, pkg)
	// 		}
	// 	})
	// }
}

func makeTestPackage(file *ast.File) *ast.Package {
	file = file.Copy().(*ast.File)
	file.Package.Name.Name = "main"
	pkg := &ast.Package{
		Package: "main",
		Files:   []*ast.File{file},
	}
	return pkg
}

var optionsSource = `
import "testing"
import c "csv"
import "experimental"

// Options bucket and org are defined dynamically per test

option testing.loadStorage = (csv) => {
	return experimental.chain(
		first:  c.from(csv: csv) |> to(bucket: bucket, org: org),
		second: from(bucket:bucket)
	)
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

func testFlux(t testing.TB, l *launcher.TestLauncher, file *ast.File) {

	b := &platform.Bucket{
		OrgID:           l.Org.ID,
		Name:            t.Name(),
		RetentionPeriod: 0,
	}

	s := l.BucketService(t)
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
	pkg := makeTestPackage(file)
	pkg.Files = append(pkg.Files, options)

	// Use testing.inspect call to get all of diff, want, and got
	inspectCalls := stdlib.TestingInspectCalls(pkg)
	pkg.Files = append(pkg.Files, inspectCalls)

	bs, err := json.Marshal(pkg)
	if err != nil {
		t.Fatal(err)
	}

	req := &query.Request{
		OrganizationID: l.Org.ID,
		Compiler:       lang.ASTCompiler{AST: bs},
	}

	if r, err := l.FluxQueryService().Query(ctx, req); err != nil {
		t.Fatal(err)
	} else {
		results := make(map[string]*bytes.Buffer)

		for r.More() {
			v := r.Next()

			if _, ok := results[v.Name()]; !ok {
				results[v.Name()] = &bytes.Buffer{}
			}
			err := execute.FormatResult(results[v.Name()], v)
			if err != nil {
				t.Error(err)
			}
		}
		if err := r.Err(); err != nil {
			t.Error(err)
		}

		logFormatted := func(name string, results map[string]*bytes.Buffer) {
			if _, ok := results[name]; ok {
				scanner := bufio.NewScanner(results[name])
				for scanner.Scan() {
					t.Log(scanner.Text())
				}
			} else {
				t.Log("table ", name, " not present in results")
			}
		}
		if _, ok := results["diff"]; ok {
			t.Error("diff table was not empty")
			logFormatted("diff", results)
			logFormatted("want", results)
			logFormatted("got", results)
		}
	}
}
