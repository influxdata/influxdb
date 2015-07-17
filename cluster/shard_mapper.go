package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
	"gopkg.in/fatih/pool.v2"
)

// ShardMapper is responsible for providing mappers for requested shards. It is
// responsible for creating those mappers from the local store, or reaching
// out to another node on the cluster.
type ShardMapper struct {
	MetaStore interface {
		NodeID() uint64
		Node(id uint64) (ni *meta.NodeInfo, err error)
	}

	TSDBStore interface {
		CreateMapper(shardID uint64, query string, chunkSize int) (tsdb.Mapper, error)
	}
}

// NewShardMapper returns a mapper of local and remote shards.
func NewShardMapper() *ShardMapper {
	return &ShardMapper{}
}

// CreateMapper returns a Mapper for the given shard ID.
func (r *ShardMapper) CreateMapper(sh meta.ShardInfo, stmt string, chunkSize int) (tsdb.Mapper, error) {
	var err error
	var m tsdb.Mapper
	if sh.OwnedBy(r.MetaStore.NodeID()) {
		m, err = r.TSDBStore.CreateMapper(sh.ID, stmt, chunkSize)
		if err != nil {
			return nil, err
		}
	} else {
		rm := NewRemoteMaper(sh.OwnerIDs[0], sh.ID, stmt, chunkSize)
		rm.MetaStore = r.MetaStore
		m = rm
	}

	return m, nil
}

// RemoteMapper implements the tsdb.Mapper interface. It connects to a remote node,
// sends a query, and interprets the stream of data that comes back.
type RemoteMapper struct {
	MetaStore interface {
		Node(id uint64) (ni *meta.NodeInfo, err error)
	}

	nodeID    uint64
	shardID   uint64
	stmt      string
	chunkSize int

	tagsets []string

	pool    *clientPool
	timeout time.Duration
	conn    *pool.PoolConn

	bufferedResponse *MapShardResponse
}

// NewRemoteMaper returns a new remote mapper.
func NewRemoteMaper(nodeID, shardID uint64, stmt string, chunkSize int) *RemoteMapper {
	return &RemoteMapper{
		nodeID:    nodeID,
		shardID:   shardID,
		stmt:      stmt,
		chunkSize: chunkSize,
	}
}

// Open connects to the remote node and starts receiving data.
func (r *RemoteMapper) Open() (err error) {
	defer func() {
		if err != nil {
			r.conn.MarkUnusable()
			r.conn.Close()
		}
	}()

	c, err := r.dial(r.nodeID)
	if err != nil {
		return err
	}

	r.conn = c.(*pool.PoolConn)

	// Build Map request.
	var request MapShardRequest
	request.SetShardID(r.shardID)
	request.SetQuery(r.stmt)
	request.SetChunkSize(int32(r.chunkSize))

	// Marshal into protocol buffers.
	buf, err := request.MarshalBinary()
	if err != nil {
		return err
	}

	// Write request.
	r.conn.SetWriteDeadline(time.Now().Add(r.timeout))
	if err := WriteTLV(r.conn, mapShardRequestMessage, buf); err != nil {
		return err
	}

	// Read the response.
	r.conn.SetReadDeadline(time.Now().Add(r.timeout))
	_, buf, err = ReadTLV(r.conn)
	if err != nil {
		return err
	}

	// Unmarshal response.
	r.bufferedResponse = &MapShardResponse{}
	if err := r.bufferedResponse.UnmarshalBinary(buf); err != nil {
		return err
	}

	if r.bufferedResponse.Code() != 0 {
		return fmt.Errorf("error code %d: %s", r.bufferedResponse.Code(), r.bufferedResponse.Message())
	}

	// Decode the first response to get the TagSets.
	r.tagsets = r.bufferedResponse.TagSets()

	return nil
}

func (r *RemoteMapper) TagSets() []string {
	return r.tagsets
}

// NextChunk returns the next chunk read from the remote node to the client.
func (r *RemoteMapper) NextChunk() (interface{}, error) {
	output := &tsdb.MapperOutput{}
	var response *MapShardResponse

	if r.bufferedResponse != nil {
		response = r.bufferedResponse
		r.bufferedResponse = nil
	} else {
		response = &MapShardResponse{}

		// Read the response.
		r.conn.SetReadDeadline(time.Now().Add(r.timeout))
		_, buf, err := ReadTLV(r.conn)
		if err != nil {
			r.conn.MarkUnusable()
			return nil, err
		}

		// Unmarshal response.
		if err := response.UnmarshalBinary(buf); err != nil {
			return nil, err
		}

		if response.Code() != 0 {
			return nil, fmt.Errorf("error code %d: %s", response.Code(), response.Message())
		}
	}

	err := json.Unmarshal(response.Data(), output)
	return output, err
}

// Close the Mapper
func (r *RemoteMapper) Close() {
	r.conn.Close()
}

func (r *RemoteMapper) dial(nodeID uint64) (net.Conn, error) {
	// If we don't have a connection pool for that addr yet, create one
	_, ok := r.pool.getPool(nodeID)
	if !ok {
		factory := &connFactory{nodeID: nodeID, clientPool: r.pool, timeout: r.timeout}
		factory.metaStore = r.MetaStore

		p, err := pool.NewChannelPool(1, 3, factory.dial)
		if err != nil {
			return nil, err
		}
		r.pool.setPool(nodeID, p)
	}
	return r.pool.conn(nodeID)
}
