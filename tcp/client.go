package tcp

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/tsdb"
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

func (c *Client) WriteShard(shardID uint64, points []tsdb.Point) error {
	var mt byte = writeShardRequestMessage
	if err := binary.Write(c.conn, binary.LittleEndian, &mt); err != nil {
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

	if err := binary.Write(c.conn, binary.LittleEndian, &size); err != nil {
		return err
	}

	if _, err := c.conn.Write(b); err != nil {
		return err
	}

	// read back our response
	if err := binary.Read(c.conn, binary.LittleEndian, &mt); err != nil {
		return err
	}

	if err := binary.Read(c.conn, binary.LittleEndian, &size); err != nil {
		return err
	}

	message := make([]byte, size)

	reader := io.LimitReader(c.conn, size)
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
	return c.conn.Close()
}
