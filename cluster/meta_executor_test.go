package cluster

import (
	"fmt"
	"sync"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
)

func Test_ExecuteStatement(t *testing.T) {
	numOfNodes := 3

	mock := newMockExecutor()
	// Expect each statement twice because we have 3 nodes, 2 of which
	// are remote and should be executed on.
	mock.expect("DROP RETENTION POLICY rp0 on foo")
	mock.expect("DROP RETENTION POLICY rp0 on foo")
	mock.expect("DROP DATABASE foo")
	mock.expect("DROP DATABASE foo")

	e := NewMetaExecutor()
	e.MetaClient = newMockMetaClient(numOfNodes)
	e.Node = influxdb.NewNode("/tmp/node")
	e.Node.ID = 1
	// Replace MetaExecutor's nodeExecutor with our mock.
	e.nodeExecutor = mock

	if err := e.ExecuteStatement(mustParseStatement("DROP RETENTION POLICY rp0 on foo"), "foo"); err != nil {
		t.Fatal(err)
	}
	if err := e.ExecuteStatement(mustParseStatement("DROP DATABASE foo"), "foo"); err != nil {
		t.Fatal(err)
	}

	if err := mock.done(); err != nil {
		t.Fatal(err)
	}
}

type mockExecutor struct {
	mu               sync.Mutex
	expectStatements []influxql.Statement
	idx              int
}

func newMockExecutor() *mockExecutor {
	return &mockExecutor{
		idx: -1,
	}
}

func (e *mockExecutor) expect(stmt string) {
	s := mustParseStatement(stmt)
	e.expectStatements = append(e.expectStatements, s)
}

func (e *mockExecutor) done() error {
	if e.idx+1 != len(e.expectStatements) {
		return fmt.Errorf("expected %d mockExecuteOnNode calls, got %d", len(e.expectStatements), e.idx+1)
	}
	return nil
}

func (e *mockExecutor) executeOnNode(stmt influxql.Statement, database string, node *meta.NodeInfo) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.idx++

	if e.idx > len(e.expectStatements)-1 {
		return fmt.Errorf("extra statement: %s", stmt.String())
	}

	if e.expectStatements[e.idx].String() != stmt.String() {
		return fmt.Errorf("unexpected statement:\n\texp: %s\n\tgot: %s\n", e.expectStatements[e.idx].String(), stmt.String())
	}
	return nil
}

func mustParseStatement(stmt string) influxql.Statement {
	s, err := influxql.ParseStatement(stmt)
	if err != nil {
		panic(err)
	}
	return s
}

type mockMetaClient struct {
	nodes []meta.NodeInfo
}

func newMockMetaClient(nodeCnt int) *mockMetaClient {
	c := &mockMetaClient{}
	for i := 0; i < nodeCnt; i++ {
		n := meta.NodeInfo{
			ID:      uint64(i + 1),
			Host:    fmt.Sprintf("localhost:%d", 8000+i),
			TCPHost: fmt.Sprintf("localhost:%d", 9000+i),
		}
		c.nodes = append(c.nodes, n)
	}

	return c
}

func (c *mockMetaClient) DataNode(id uint64) (ni *meta.NodeInfo, err error) {
	for i := 0; i < len(c.nodes); i++ {
		if c.nodes[i].ID == id {
			ni = &c.nodes[i]
			return
		}
	}
	return
}

func (c *mockMetaClient) DataNodes() ([]meta.NodeInfo, error) {
	return c.nodes, nil
}
