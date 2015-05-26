package tcp

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

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
	pool map[string]Pool
	mu   sync.RWMutex
}

func NewClient() *Client {
	return &Client{
		pool: make(map[string]Pool),
	}
}

func (c *Client) poolSize() int {
	if c.pool == nil {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	var size int
	for _, p := range c.pool {
		size += p.Len()
	}
	return size
}

func (c *Client) dial(addr string) (net.Conn, error) {
	addr = strings.ToLower(addr)
	// if we don't have a connection pool for that addr yet, create one
	c.mu.Lock()
	if _, ok := c.pool[addr]; !ok {
		c.mu.Unlock()
		conn := newClientConn(addr, c)
		p, err := NewChannelPool(1, 3, conn.dial)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		c.pool[addr] = p
		c.mu.Unlock()
	}
	c.mu.Lock()
	conn, err := c.pool[addr].Get()
	c.mu.Unlock()
	return conn, err
}

func (c *Client) WriteShard(addr string, shardID uint64, points []tsdb.Point) error {
	conn, err := c.dial(addr)
	if err != nil {
		return err
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.pool {
		p.Close()
	}
	c.pool = nil
	return nil
}
