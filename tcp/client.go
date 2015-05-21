package tcp

import (
	"encoding/binary"
	"net"

	"github.com/influxdb/influxdb/data"
)

type Client struct {
	conn net.Conn
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	c.conn = conn
	return nil
}

func (c *Client) WriteShardRequest(shardID uint64, points []data.Point) error {
	mt := writeShardRequestMessage
	if err := binary.Write(c.conn, binary.LittleEndian, &mt); err != nil {
		return err
	}

	var wpr data.WriteShardRequest
	wpr.SetShardID(shardID)
	wpr.AddPoints(points)

	b, err := wpr.MarshalBinary()
	if err != nil {
		return err
	}

	size := int64(len(b))

	if err := binary.Write(c.conn, binary.LittleEndian, &size); err != nil {
		return err
	}

	if _, err := c.conn.Write(b); err != nil {
		return err
	}

	return nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}
