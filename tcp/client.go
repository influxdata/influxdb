package tcp

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/tsdb"
)

const maxConnections = 500

var errMaxConnectionsExceeded = fmt.Errorf("can not exceed max connections of %d", maxConnections)

type clientConn struct {
	client *Client
	addr   string
}

func newClientConn(addr string, c *Client) *clientConn {
	return &clientConn{
		addr:   addr,
		client: c,
	}
}
func (c *clientConn) dial() (net.Conn, error) {
	if c.client.poolSize() > maxConnections {
		return nil, errMaxConnectionsExceeded
	}

	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type Client struct {
	pool *connectionPool
}

func NewClient() *Client {
	return &Client{
		pool: newConnectionPool(),
	}
}

func (c *Client) poolSize() int {
	if c.pool == nil {
		return 0
	}

	return c.pool.size()
}

func (c *Client) dial(addr string) (net.Conn, error) {
	addr = strings.ToLower(addr)
	// if we don't have a connection pool for that addr yet, create one
	_, ok := c.pool.getPool(addr)
	if !ok {
		conn := newClientConn(addr, c)
		p, err := NewChannelPool(1, 3, conn.dial)
		if err != nil {
			return nil, err
		}
		c.pool.setPool(addr, p)
	}
	return c.pool.conn(addr)
}

func (c *Client) WriteShard(addr string, shardID uint64, points []tsdb.Point) error {
	conn, err := c.dial(addr)
	if err != nil {
		return err
	}

	// This will return the connection to the data pool
	defer conn.Close()

	var mt byte = writeShardRequestMessage
	if err := binary.Write(conn, binary.LittleEndian, &mt); err != nil {
		return err
	}

	var request cluster.WriteShardRequest
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

	var response cluster.WriteShardResponse
	if err := response.UnmarshalBinary(message); err != nil {
		return err
	}

	if response.Code() != 0 {
		return fmt.Errorf("error code %d: %s", response.Code(), response.Message())
	}

	return nil
}

func (c *Client) Close() error {
	if c.pool == nil {
		return fmt.Errorf("client already closed")
	}
	c.pool = nil
	return nil
}
