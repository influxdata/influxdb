package cluster

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
)

// MetaExecutor executes meta queries on all data nodes.
type MetaExecutor struct {
	mu             sync.RWMutex
	timeout        time.Duration
	pool           *clientPool
	maxConnections int
	Logger         *log.Logger
	Node           *influxdb.Node

	MetaClient interface {
		DataNode(id uint64) (ni *meta.NodeInfo, err error)
		DataNodes() ([]meta.NodeInfo, error)
	}
}

// NewMetaExecutor returns a new initialized *MetaExecutor.
func NewMetaExecutor() *MetaExecutor {
	return &MetaExecutor{
		timeout:        DefaultWriteTimeout,
		pool:           newClientPool(),
		maxConnections: 1000,
		Logger:         log.New(os.Stderr, "[meta-executor] ", log.LstdFlags),
	}
}

// ExecuteStatement executes a single InfluxQL statement on all nodes in the cluster concurrently.
func (m *MetaExecutor) ExecuteStatement(stmt influxql.Statement, database string) error {
	println("MetaExecutor.ExecuteStatement start")
	defer println("MetaExecutor.ExecuteStatement end")
	// Get a list of all nodes the query needs to be executed on.
	nodes, err := m.MetaClient.DataNodes()
	if err != nil {
		return err
	}

	// Start a goroutine to execute the statement on each of the remote nodes.
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	errs := make(chan error)
	defer close(errs)

	for _, node := range nodes {
		go func() {
			defer wg.Done()
			if err := m.executeOnNode(stmt, database, &node); err != nil {
				errs <- err
			}
		}()
	}

	// Wait on all nodes to execute the statement and respond.
	wg.Wait()

	select {
	case err = <-errs:
		return err
	default:
		return nil
	}
}

// executeOnNode executes a single InfluxQL statement on a single node.
func (m *MetaExecutor) executeOnNode(stmt influxql.Statement, database string, node *meta.NodeInfo) error {
	println("MetaExecutor.executeOnNode start")
	defer println("MetaExecutor.executeOnNode end")
	// Executing statement on the local node?
	//if node.ID == m.Node.ID {
	//	panic("fix me !!!!!!!!!!!!!!!!!!!")
	//}

	// We're executing on a remote node so establish a connection.
	c, err := m.dial(node.ID)
	if err != nil {
		return err
	}

	conn, ok := c.(*pooledConn)
	if !ok {
		panic("wrong connection type in MetaExecutor")
	}
	// Return connection to pool by "closing" it.
	defer func(conn net.Conn) { conn.Close() }(conn)

	// Build RPC request.
	var request ExecuteStatementRequest
	request.SetStatement(stmt.String())
	request.SetDatabase(database)

	// Marshal into protocol buffer.
	buf, err := request.MarshalBinary()
	if err != nil {
		return err
	}

	// Send request.
	conn.SetWriteDeadline(time.Now().Add(m.timeout))
	if err := WriteTLV(conn, executeStatementRequestMessage, buf); err != nil {
		conn.MarkUnusable()
		return err
	}

	// Read the response.
	conn.SetReadDeadline(time.Now().Add(m.timeout))
	_, buf, err = ReadTLV(conn)
	if err != nil {
		conn.MarkUnusable()
		return err
	}

	// Unmarshal response.
	var response ExecuteStatementResponse
	if err := response.UnmarshalBinary(buf); err != nil {
		return err
	}

	if response.Code() != 0 {
		return fmt.Errorf("error code %d: %s", response.Code(), response.Message())
	}

	return nil
}

// dial returns a connection to a single node in the cluster.
func (m *MetaExecutor) dial(nodeID uint64) (net.Conn, error) {
	// If we don't have a connection pool for that addr yet, create one
	_, ok := m.pool.getPool(nodeID)
	if !ok {
		factory := &connFactory{nodeID: nodeID, clientPool: m.pool, timeout: m.timeout}
		factory.metaClient = m.MetaClient

		p, err := NewBoundedPool(1, m.maxConnections, m.timeout, factory.dial)
		if err != nil {
			return nil, err
		}
		m.pool.setPool(nodeID, p)
	}
	return m.pool.conn(nodeID)
}
