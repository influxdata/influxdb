package data

import (
	"sync"

	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/influxdb/influxdb/tsdb"
)

func NewDataNode(id uint64) *Node {
	return &Node{
		id:     id,
		Logger: log.New(os.Stderr, "[node] ", log.LstdFlags),
	}
}

var (
	ErrShardNotFound = fmt.Errorf("shard not found")
)

type ClusterWriter interface {
	Write(shardID, nodeID uint64, points []tsdb.Point) error
}

type Node struct {
	id   uint64
	path string

	mu sync.RWMutex

	index *tsdb.DatabaseIndex

	shards map[uint64]*tsdb.Shard

	ClusterWriter ClusterWriter

	Logger *log.Logger
}

func (n *Node) Open() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.shards = map[uint64]*tsdb.Shard{}

	// Open shards
	// Start AE for Node

	// FIXME: Need to use config data dir
	path, err := ioutil.TempDir("", "influxdb-alpha1")
	if err != nil {
		return err
	}
	n.path = filepath.Join(path, "1")

	n.index = tsdb.NewDatabaseIndex()
	shard := tsdb.NewShard(n.index, n.path)

	if err := shard.Open(); err != nil {
		return err
	}
	n.Logger.Printf("opened temp shard at %s", n.path)

	n.shards[uint64(1)] = shard

	return nil
}

func (n *Node) WriteShard(shardID uint64, points []tsdb.Point) error {
	//TODO: Find the Shard for shardID
	//TODO: Write points to the shard
	sh, ok := n.shards[shardID]
	if !ok {
		return ErrShardNotFound
	}
	fmt.Printf("> WriteShard %d, %d points\n", shardID, len(points))

	if err := sh.WritePoints(points); err != nil {
		return err
	}
	return nil

}

func (n *Node) Write(shardID, nodeID uint64, points []tsdb.Point) error {
	if n.id != nodeID {
		n.ClusterWriter.Write(shardID, nodeID, points)
	}
	return n.WriteShard(shardID, points)
}

func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, sh := range n.shards {
		if err := sh.Close(); err != nil {
			return err
		}
	}
	n.shards = nil

	return nil
}
