package cluster

import (
	"net"
	"time"

	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
	"gopkg.in/fatih/pool.v2"
)

const (
	MAX_MAP_RESPONSE_SIZE = 1024 * 1024 * 1024
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

	buffer     []byte
	bufferSent bool
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
func (r *RemoteMapper) Open() error {
	c, err := r.dial(r.nodeID)
	if err != nil {
		return err
	}

	conn, ok := c.(*pool.PoolConn)
	if !ok {
		panic("wrong connection type")
	}
	defer func(conn net.Conn) {
		conn.Close() // return to pool
	}(conn)

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
	conn.SetWriteDeadline(time.Now().Add(r.timeout))
	if err := WriteTLV(conn, mapShardRequestMessage, buf); err != nil {
		conn.MarkUnusable()
		return err
	}

	// Read the response.
	conn.SetReadDeadline(time.Now().Add(r.timeout))
	_, buf, err = ReadTLV(conn)
	if err != nil {
		conn.MarkUnusable()
		return err
	}

	// Unmarshal response.
	var response MapShardResponse
	if err := response.UnmarshalBinary(buf); err != nil {
		return err
	}

	if r.Code() != 0 {
		return fmt.Errorf("error code %d: %s", response.Code(), response.Message())
	}

	// Decode the first response to get the TagSets.
	r.tagsets = response.TagSets()

	// Buffer the remaining data.
	r.buffer = response.Data()

	return nil
}

func (r *RemoteMapper) TagSets() []string {
	return r.tagsets
}

// NextChunk returns the next chunk read from the remote node to the client.
func (r *RemoteMapper) NextChunk() (interface{}, error) {
	if !r.bufferSent {
		r.bufferSent = true
		// Decode buffer and return
	}
	return nil, nil
}

// Close the Mapper
func (r *RemoteMapper) Close() {
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
