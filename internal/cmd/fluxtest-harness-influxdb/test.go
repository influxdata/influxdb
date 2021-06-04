package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/cmd/flux/cmd"
	"github.com/influxdata/flux/execute/table"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/spf13/cobra"
)

type testExecutor struct {
	ctx         context.Context
	l           *launcher.TestLauncher
	writeOptAST *ast.File
	readOptAST  *ast.File
	errOutput   bytes.Buffer
	i           int
	failed      bool
}

func NewTestExecutor(ctx context.Context) (cmd.TestExecutor, error) {
	e := &testExecutor{ctx: ctx}
	e.init()

	e.l = launcher.NewTestLauncher()
	if err := e.l.Run(e, ctx); err != nil {
		_ = e.l.Shutdown(context.Background())
		return nil, err
	}

	if err := e.l.Setup(); err != nil {
		_ = e.l.Shutdown(context.Background())
		return nil, err
	}
	return e, nil
}

func (t *testExecutor) init() {
	t.writeOptAST = prepareOptions(writeOptSource)
	t.readOptAST = prepareOptions(readOptSource)
}

func (t *testExecutor) Close() error {
	if t.l == nil {
		return nil
	}

	if err := t.l.Shutdown(context.Background()); err != nil {
		return err
	}
	t.l = nil

	if t.Failed() {
		_, _ = io.Copy(os.Stdout, &t.errOutput)
	}
	return nil
}

func (t *testExecutor) Run(pkg *ast.Package) error {
	l := t.l.Launcher
	b := &influxdb.Bucket{
		OrgID: t.l.Org.ID,
		Name:  fmt.Sprintf("%04d", t.i),
	}
	t.i++

	s := l.BucketService()
	if err := s.CreateBucket(t.ctx, b); err != nil {
		return err
	}
	defer func() { _ = s.DeleteBucket(t.ctx, b.ID) }()

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
			Init: &ast.StringLiteral{Value: t.l.Org.Name},
		},
	}

	// During the first execution, we are performing the writes
	// that are in the testcase. We do not care about errors.
	_ = t.executeWithOptions(bucketOpt, orgOpt, t.writeOptAST, pkg)

	// Execute the read pass.
	return t.executeWithOptions(bucketOpt, orgOpt, t.readOptAST, pkg)
}

func (t *testExecutor) executeWithOptions(bucketOpt, orgOpt *ast.OptionStatement, optionsAST *ast.File, pkg *ast.Package) error {
	options := optionsAST.Copy().(*ast.File)
	options.Body = append([]ast.Statement{bucketOpt, orgOpt}, options.Body...)

	// Add options to pkg
	pkg = pkg.Copy().(*ast.Package)
	pkg.Files = append([]*ast.File{options}, pkg.Files...)

	bs, err := json.Marshal(pkg)
	if err != nil {
		return err
	}

	req := &query.Request{
		OrganizationID: t.l.Org.ID,
		Compiler:       lang.ASTCompiler{AST: bs},
	}

	r, err := t.l.FluxQueryService().Query(t.ctx, req)
	if err != nil {
		return err
	}
	defer r.Release()

	var output strings.Builder
	for r.More() {
		v := r.Next()

		if err := v.Tables().Do(func(tbl flux.Table) error {
			// The data returned here is the result of `testing.diff`, so any result means that
			// a comparison of two tables showed inequality. Capture that inequality as part of the error.
			// XXX: rockstar (08 Dec 2020) - This could use some ergonomic work, as the diff testOutput
			// is not exactly "human readable."
			_, _ = fmt.Fprint(&output, table.Stringify(tbl))
			return nil
		}); err != nil {
			return err
		}
	}
	if output.Len() > 0 {
		return errors.New(output.String())
	}
	r.Release()
	return r.Err()
}

// This options definition puts to() in the path of the CSV input. The tests
// get run in this case and they would normally pass, if we checked the
// results, but don't look at them.
const writeOptSource = `
import "testing"
import c "csv"

option testing.loadStorage = (csv) => {
	return c.from(csv: csv) |> to(bucket: bucket, org: org)
}
option testing.load = (tables=<-) => {
	return tables |> to(bucket: bucket, org: org)
}
`

// This options definition is for the second run, the test run. It loads the
// data from previously written bucket. We check the results after running this
// second pass and report on them.
const readOptSource = `
import "testing"
import c "csv"

option testing.loadStorage = (csv) => {
	return from(bucket: bucket)
}
option testing.load = (tables=<-) => {
	return from(bucket: bucket)
}
`

func prepareOptions(optionsSource string) *ast.File {
	pkg := parser.ParseSource(optionsSource)
	if ast.Check(pkg) > 0 {
		panic(ast.GetError(pkg))
	}
	return pkg.Files[0]
}

func (t *testExecutor) Logf(s string, i ...interface{}) {
	_, _ = fmt.Fprintf(&t.errOutput, s, i...)
	_, _ = fmt.Fprintln(&t.errOutput)
}

func (t *testExecutor) Errorf(s string, i ...interface{}) {
	t.Logf(s, i...)
	t.Fail()
}

func (t *testExecutor) Fail() {
	t.failed = true
}

func (t *testExecutor) Failed() bool {
	return t.failed
}

func (t *testExecutor) Name() string {
	return "flux"
}

func (t *testExecutor) FailNow() {
	t.Fail()
	panic(errors.New("abort"))
}

func tryExec(cmd *cobra.Command) (err error) {
	defer func() {
		if e := recover(); e != nil {
			var ok bool
			err, ok = e.(error)
			if !ok {
				err = errors.New(fmt.Sprint(e))
			}
		}
	}()
	err = cmd.Execute()
	return
}

func main() {
	c := cmd.TestCommand(NewTestExecutor)
	c.Use = "fluxtest-harness-influxdb"
	if err := tryExec(c); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
