package cluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/fatih/pool"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
)

const (
	writeShardRequestMessage byte = iota + 1
	writeShardResponseMessage
)

const (
	maxConnections = 500
	maxRetries     = 3
)

var errMaxConnectionsExceeded = fmt.Errorf("can not exceed max connections of %d", maxConnections)

type metaStore interface {
	Node(id uint64) (ni *meta.NodeInfo, err error)
}

type connFactory struct {
	metaStore  metaStore
	nodeID     uint64
	timeout    time.Duration
	clientPool interface {
		size() int
	}
}

func (c *connFactory) dial() (net.Conn, error) {
	if c.clientPool.size() > maxConnections {
		return nil, errMaxConnectionsExceeded
	}

	nodeInfo, err := c.metaStore.Node(c.nodeID)
	if err != nil {
		return nil, err
	}

	var retries int
	for {
		conn, err := net.DialTimeout("tcp", nodeInfo.Host, c.timeout)
		if err != nil && retries == maxRetries {
			return nil, err
		} else if err == nil {
			return conn, nil
		}
		retries++
	}
}

type Writer struct {
	pool      *clientPool
	metaStore metaStore
	timeout   time.Duration
}

func NewWriter(m metaStore, timeout time.Duration) *Writer {
	return &Writer{
		pool:      newClientPool(),
		metaStore: m,
		timeout:   timeout,
	}
}

func (c *Writer) dial(nodeID uint64) (net.Conn, error) {
	// if we don't have a connection pool for that addr yet, create one
	_, ok := c.pool.getPool(nodeID)
	if !ok {
		factory := &connFactory{nodeID: nodeID, metaStore: c.metaStore, clientPool: c.pool, timeout: c.timeout}
		p, err := pool.NewChannelPool(1, 3, factory.dial)
		if err != nil {
			return nil, err
		}
		c.pool.setPool(nodeID, p)
	}
	return c.pool.conn(nodeID)
}

func (w *Writer) Write(shardID, ownerID uint64, points []tsdb.Point) error {
	conn, err := w.dial(ownerID)
	if err != nil {
		return err
	}

	// This will return the connection to the data pool
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(w.timeout))
	var mt byte = writeShardRequestMessage
	if err := binary.Write(conn, binary.LittleEndian, &mt); err != nil {
		return err
	}

	var request WriteShardRequest
	request.SetShardID(shardID)
	request.AddPoints(points)

	b, err := request.MarshalBinary()
	if err != nil {
		return err
	}

	size := int64(len(b))

	conn.SetWriteDeadline(time.Now().Add(w.timeout))
	if err := binary.Write(conn, binary.LittleEndian, &size); err != nil {
		return err
	}

	conn.SetWriteDeadline(time.Now().Add(w.timeout))
	if _, err := conn.Write(b); err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(w.timeout))
	// read back our response
	if err := binary.Read(conn, binary.LittleEndian, &mt); err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(w.timeout))
	if err := binary.Read(conn, binary.LittleEndian, &size); err != nil {
		return err
	}

	message := make([]byte, size)

	reader := io.LimitReader(conn, size)
	conn.SetReadDeadline(time.Now().Add(w.timeout))
	_, err = reader.Read(message)
	if err != nil {
		return err
	}

	var response WriteShardResponse
	if err := response.UnmarshalBinary(message); err != nil {
		return err
	}

	if response.Code() != 0 {
		return fmt.Errorf("error code %d: %s", response.Code(), response.Message())
	}

	return nil
}

func (w *Writer) Close() error {
	if w.pool == nil {
		return fmt.Errorf("client already closed")
	}
	w.pool.close()
	w.pool = nil
	return nil
}
