package cluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/fatih/pool"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
)

const (
	writeShardRequestMessage byte = iota + 1
	writeShardResponseMessage
)

const maxConnections = 500

var errMaxConnectionsExceeded = fmt.Errorf("can not exceed max connections of %d", maxConnections)

type metaStore interface {
	Node(id uint64) (ni *meta.NodeInfo, err error)
}

type connFactory struct {
	nodeInfo   *meta.NodeInfo
	clientPool interface {
		size() int
	}
}

func (c *connFactory) dial() (net.Conn, error) {
	if c.clientPool.size() > maxConnections {
		return nil, errMaxConnectionsExceeded
	}

	conn, err := net.Dial("tcp", c.nodeInfo.Host)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type Writer struct {
	pool      *clientPool
	metaStore metaStore
}

func NewWriter(m metaStore) *Writer {
	return &Writer{
		pool:      newClientPool(),
		metaStore: m,
	}
}

func (c *Writer) dial(nodeID uint64) (net.Conn, error) {
	nodeInfo, err := c.metaStore.Node(nodeID)
	if err != nil {
		return nil, err
	}

	// if we don't have a connection pool for that addr yet, create one
	_, ok := c.pool.getPool(nodeInfo)
	if !ok {
		factory := &connFactory{nodeInfo: nodeInfo, clientPool: c.pool}
		p, err := pool.NewChannelPool(1, 3, factory.dial)
		if err != nil {
			return nil, err
		}
		c.pool.setPool(nodeInfo, p)
	}
	return c.pool.conn(nodeInfo)
}

func (w *Writer) Write(shardID, ownerID uint64, points []tsdb.Point) error {
	conn, err := w.dial(ownerID)
	if err != nil {
		return err
	}

	// This will return the connection to the data pool
	defer conn.Close()

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

	if err := binary.Write(conn, binary.LittleEndian, &size); err != nil {
		return err
	}

	if _, err := conn.Write(b); err != nil {
		return err
	}

	// read back our response
	if err := binary.Read(conn, binary.LittleEndian, &mt); err != nil {
		return err
	}

	if err := binary.Read(conn, binary.LittleEndian, &size); err != nil {
		return err
	}

	message := make([]byte, size)

	reader := io.LimitReader(conn, size)
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
