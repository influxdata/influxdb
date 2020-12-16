package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/query"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
)

type fakeQueryService struct {
	mu       sync.Mutex
	queries  map[string]*fakeQuery
	queryErr error
	// The most recent ctx received in the Query method.
	// Used to validate that the executor applied the correct authorizer.
	mostRecentCtx context.Context
}

var _ query.AsyncQueryService = (*fakeQueryService)(nil)

func makeAST(q string) lang.ASTCompiler {
	pkg, err := runtime.ParseToJSON(q)
	if err != nil {
		panic(err)
	}
	return lang.ASTCompiler{
		AST: pkg,
		Now: time.Unix(123, 0),
	}
}

func makeASTString(q lang.ASTCompiler) string {
	b, err := json.Marshal(q)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func newFakeQueryService() *fakeQueryService {
	return &fakeQueryService{queries: make(map[string]*fakeQuery)}
}

func (s *fakeQueryService) Query(ctx context.Context, req *query.Request) (flux.Query, error) {
	if req.Authorization == nil {
		panic("authorization required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mostRecentCtx = ctx
	if s.queryErr != nil {
		err := s.queryErr
		s.queryErr = nil
		return nil, err
	}

	astc, ok := req.Compiler.(lang.ASTCompiler)
	if !ok {
		return nil, fmt.Errorf("fakeQueryService only supports the ASTCompiler, got %T", req.Compiler)
	}

	fq := &fakeQuery{
		wait:    make(chan struct{}),
		results: make(chan flux.Result),
	}
	s.queries[makeASTString(astc)] = fq

	go fq.run(ctx)

	return fq, nil
}

// SucceedQuery allows the running query matching the given script to return on its Ready channel.
func (s *fakeQueryService) SucceedQuery(script string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Unblock the flux.
	ast := makeAST(script)
	spec := makeASTString(ast)
	fq, ok := s.queries[spec]
	if !ok {
		ast.Now = ast.Now.UTC()
		spec = makeASTString(ast)
		fq = s.queries[spec]
	}
	close(fq.wait)
	delete(s.queries, spec)
}

// FailQuery closes the running query's Ready channel and sets its error to the given value.
func (s *fakeQueryService) FailQuery(script string, forced error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Unblock the flux.
	ast := makeAST(script)
	spec := makeASTString(ast)
	fq, ok := s.queries[spec]
	if !ok {
		ast.Now = ast.Now.UTC()
		spec = makeASTString(ast)
		fq = s.queries[spec]
	}
	fq.forcedError = forced
	close(fq.wait)
	delete(s.queries, spec)
}

// FailNextQuery causes the next call to QueryWithCompile to return the given error.
func (s *fakeQueryService) FailNextQuery(forced error) {
	s.queryErr = forced
}

// WaitForQueryLive ensures that the query has made it into the service.
// This is particularly useful for the synchronous executor,
// because the execution starts on a separate goroutine.
func (s *fakeQueryService) WaitForQueryLive(t *testing.T, script string) {
	t.Helper()

	const attempts = 10
	ast := makeAST(script)
	astUTC := makeAST(script)
	astUTC.Now = ast.Now.UTC()
	spec := makeASTString(ast)
	specUTC := makeASTString(astUTC)
	for i := 0; i < attempts; i++ {
		if i != 0 {
			time.Sleep(5 * time.Millisecond)
		}

		s.mu.Lock()
		_, ok := s.queries[spec]
		s.mu.Unlock()
		if ok {
			return
		}
		s.mu.Lock()
		_, ok = s.queries[specUTC]
		s.mu.Unlock()
		if ok {
			return
		}

	}

	t.Fatalf("Did not see live query %q in time", script)
}

type fakeQuery struct {
	results     chan flux.Result
	wait        chan struct{} // Blocks Ready from returning.
	forcedError error         // Value to return from Err() method.

	ctxErr error // Error from ctx.Done.
}

var _ flux.Query = (*fakeQuery)(nil)

func (q *fakeQuery) Done()                                         {}
func (q *fakeQuery) Cancel()                                       { close(q.results) }
func (q *fakeQuery) Statistics() flux.Statistics                   { return flux.Statistics{} }
func (q *fakeQuery) Results() <-chan flux.Result                   { return q.results }
func (q *fakeQuery) ProfilerResults() (flux.ResultIterator, error) { return nil, nil }

func (q *fakeQuery) Err() error {
	if q.ctxErr != nil {
		return q.ctxErr
	}
	return q.forcedError
}

// run is intended to be run on its own goroutine.
// It blocks until q.wait is closed, then sends a fake result on the q.results channel.
func (q *fakeQuery) run(ctx context.Context) {
	defer close(q.results)

	// Wait for call to set query success/fail.
	select {
	case <-ctx.Done():
		q.ctxErr = ctx.Err()
		return
	case <-q.wait:
		// Normal case.
	}

	if q.forcedError == nil {
		res := newFakeResult()
		q.results <- res
	}
}

// fakeResult is a dumb implementation of flux.Result that always returns the same values.
type fakeResult struct {
	name  string
	table flux.Table
}

var _ flux.Result = (*fakeResult)(nil)

func newFakeResult() *fakeResult {
	meta := []flux.ColMeta{{Label: "x", Type: flux.TInt}}
	vals := []values.Value{values.NewInt(int64(1))}
	gk := execute.NewGroupKey(meta, vals)
	a := &memory.Allocator{}
	b := execute.NewColListTableBuilder(gk, a)
	i, _ := b.AddCol(meta[0])
	b.AppendInt(i, int64(1))
	t, err := b.Table()
	if err != nil {
		panic(err)
	}
	return &fakeResult{name: "res", table: t}
}

func (r *fakeResult) Statistics() flux.Statistics {
	return flux.Statistics{}
}

func (r *fakeResult) Name() string               { return r.name }
func (r *fakeResult) Tables() flux.TableIterator { return tables{r.table} }

// tables makes a TableIterator out of a slice of Tables.
type tables []flux.Table

var _ flux.TableIterator = tables(nil)

func (ts tables) Do(f func(flux.Table) error) error {
	for _, t := range ts {
		if err := f(t); err != nil {
			return err
		}
	}
	return nil
}

func (ts tables) Statistics() flux.Statistics { return flux.Statistics{} }

type testCreds struct {
	OrgID, UserID influxdb.ID
	Auth          *influxdb.Authorization
}

func createCreds(t *testing.T, orgSvc influxdb.OrganizationService, userSvc influxdb.UserService, authSvc influxdb.AuthorizationService) testCreds {
	t.Helper()

	org := &influxdb.Organization{Name: t.Name() + "-org"}
	if err := orgSvc.CreateOrganization(context.Background(), org); err != nil {
		t.Fatal(err)
	}

	user := &influxdb.User{Name: t.Name() + "-user"}
	if err := userSvc.CreateUser(context.Background(), user); err != nil {
		t.Fatal(err)
	}

	readPerm, err := influxdb.NewGlobalPermission(influxdb.ReadAction, influxdb.BucketsResourceType)
	if err != nil {
		t.Fatal(err)
	}
	writePerm, err := influxdb.NewGlobalPermission(influxdb.WriteAction, influxdb.BucketsResourceType)
	if err != nil {
		t.Fatal(err)
	}
	auth := &influxdb.Authorization{
		OrgID:       org.ID,
		UserID:      user.ID,
		Token:       "hifriend!",
		Permissions: []influxdb.Permission{*readPerm, *writePerm},
	}
	if err := authSvc.CreateAuthorization(context.Background(), auth); err != nil {
		t.Fatal(err)
	}

	return testCreds{OrgID: org.ID, Auth: auth}
}

// Some tests use t.Parallel, and the fake query service depends on unique scripts,
// so format a new script with the test name in each test.
const fmtTestScript = `
option task = {
			name: %q,
			every: 1m,
}
from(bucket: "one") |> to(bucket: "two", orgID: "0000000000000000")`
