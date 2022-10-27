package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/cmd/flux/cmd"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/execute/table"
	"github.com/influxdata/flux/parser"
	fluxClient "github.com/influxdata/influxdb/flux/client"
	"github.com/influxdata/influxdb/tests"
	"github.com/spf13/cobra"
)

type testExecutor struct {
	ctx         context.Context
	writeOptAST *ast.File
	readOptAST  *ast.File
	i           int
}

func NewTestExecutor(ctx context.Context) (cmd.TestExecutor, error) {
	e := &testExecutor{ctx: ctx}
	e.init()
	return e, nil
}

func (t *testExecutor) init() {
	t.writeOptAST = prepareOptions(writeOptSource)
	t.readOptAST = prepareOptions(readOptSource)
}

func (t *testExecutor) Close() error {
	// Servers are closed as part of Run
	return nil
}

// Run executes an e2e test case for every supported index type.
// On failure, logs collected from the server will be printed to stderr.
func (t *testExecutor) Run(pkg *ast.Package) error {
	var failed bool
	for _, idx := range []string{"inmem", "tsi1"} {
		logOut := &bytes.Buffer{}
		if err := t.run(pkg, idx, logOut); err != nil {
			failed = true
			_, _ = fmt.Fprintf(os.Stderr, "Failed for index %s:\n%v\n", idx, err)
			_, _ = io.Copy(os.Stderr, logOut)
		}
	}

	if failed {
		return errors.New("test failed for some index, see logs for details")
	}
	return nil
}

// run executes an e2e test case against a specific index type.
// Server logs will be written to the specified logOut writer, for reporting.
func (t *testExecutor) run(pkg *ast.Package, index string, logOut io.Writer) error {
	_, _ = fmt.Fprintf(os.Stderr, "Testing %s...\n", index)

	config := tests.NewConfig()
	config.HTTPD.FluxEnabled = true
	config.HTTPD.FluxLogEnabled = true
	config.HTTPD.FluxTesting = true
	config.Data.Index = index

	s := tests.NewServer(config)
	s.SetLogOutput(logOut)
	if err := s.Open(); err != nil {
		return err
	}
	defer s.Close()

	dbName := fmt.Sprintf("%04d", t.i)
	t.i++

	if _, err := s.CreateDatabase(dbName); err != nil {
		return err
	}
	defer func() { _ = s.DropDatabase(dbName) }()

	// Define bucket and org options
	bucketOpt := &ast.OptionStatement{
		Assignment: &ast.VariableAssignment{
			ID:   &ast.Identifier{Name: "bucket"},
			Init: &ast.StringLiteral{Value: dbName + "/autogen"},
		},
	}

	// During the first execution, we are performing the writes
	// that are in the testcase. We do not care about errors.
	_ = t.executeWithOptions(bucketOpt, t.writeOptAST, pkg, s.URL(), logOut, false)

	// Execute the read pass.
	return t.executeWithOptions(bucketOpt, t.readOptAST, pkg, s.URL(), logOut, true)
}

// executeWithOptions runs a Flux query against a running server via the HTTP API.
// Flux queries executed by this method are expected to return no output on success. If the API call returns any data,
// it is formatted as a table and returned wrapped in an error.
func (t *testExecutor) executeWithOptions(
	bucketOpt *ast.OptionStatement,
	optionsAST *ast.File,
	pkg *ast.Package,
	serverUrl string,
	logOut io.Writer,
	checkOutput bool,
) error {
	options := optionsAST.Copy().(*ast.File)
	options.Body = append([]ast.Statement{bucketOpt}, options.Body...)

	// Add options to pkg
	pkg = pkg.Copy().(*ast.Package)
	pkg.Files = append([]*ast.File{options}, pkg.Files...)

	bs, err := json.Marshal(pkg)
	if err != nil {
		return err
	}

	query := fluxClient.QueryRequest{}.WithDefaults()
	query.AST = bs
	query.Dialect.Annotations = csv.DefaultDialect().Annotations
	j, err := json.Marshal(query)
	if err != nil {
		return err
	}

	u, err := url.Parse(serverUrl)
	if err != nil {
		return err
	}
	u.Path = "/api/v2/query"
	req, err := http.NewRequest("POST", u.String(), bytes.NewBuffer(j))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error response from flux query: %s", string(b))
	}

	decoder := csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
	r, err := decoder.Decode(resp.Body)
	if err != nil {
		return err
	}
	defer r.Release()

	wasDiff := false
	if checkOutput {
		for r.More() {
			wasDiff = true
			v := r.Next()
			if err := v.Tables().Do(func(tbl flux.Table) error {
				// The data returned here is the result of `testing.diff`, so any result means that
				// a comparison of two tables showed inequality. Capture that inequality as part of the error.
				// XXX: rockstar (08 Dec 2020) - This could use some ergonomic work, as the diff testOutput
				// is not exactly "human readable."
				_, _ = fmt.Fprintln(logOut, table.Stringify(tbl))
				return nil
			}); err != nil {
				return err
			}
		}
	}
	r.Release()
	if err := r.Err(); err != nil {
		return err
	}
	if wasDiff {
		return errors.New("test failed - diff table in output")
	}
	return nil
}

// This options definition puts to() in the path of the CSV input. The tests
// get run in this case and they would normally pass, if we checked the
// results, but don't look at them.
const writeOptSource = `
import "testing"
import c "csv"

option testing.loadStorage = (csv) => {
	return c.from(csv: csv) |> to(bucket: bucket)
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
`

func prepareOptions(optionsSource string) *ast.File {
	pkg := parser.ParseSource(optionsSource)
	if ast.Check(pkg) > 0 {
		panic(ast.GetError(pkg))
	}
	return pkg.Files[0]
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
		_, _ = fmt.Fprintf(os.Stderr, "Tests failed: %v\n", err)
		os.Exit(1)
	}
}
