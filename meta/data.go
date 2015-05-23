package meta

import (
	"errors"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta/internal"
)

//go:generate protoc --gogo_out=. internal/meta.proto

var (
	// ErrNodeExists is returned when creating a node that already exists.
	ErrNodeExists = errors.New("node already exists")
)

// Data represents the top level collection of all metadata.
type Data struct {
	Version   uint64 // autoincrementing version
	MaxNodeID uint64
	Nodes     []NodeInfo
	Databases []DatabaseInfo
	Users     []UserInfo
}

// CreateNode returns a new instance of Data with a new node.
func (data *Data) CreateNode(host string) (*Data, error) {
	if data.NodeByHost(host) != nil {
		return nil, ErrNodeExists
	}

	// Clone and append new node.
	other := data.Clone()
	other.MaxNodeID++
	other.Nodes = append(other.Nodes, NodeInfo{
		ID:   other.MaxNodeID,
		Host: host,
	})

	return other, nil
}

// NodeByHost returns a node by hostname.
func (data *Data) NodeByHost(host string) *NodeInfo {
	for i := range data.Nodes {
		if data.Nodes[i].Host == host {
			return &data.Nodes[i]
		}
	}
	return nil
}

// Clone returns a copy of data with a new version.
func (data *Data) Clone() *Data {
	other := &Data{Version: data.Version + 1}

	// Copy nodes.
	other.Nodes = make([]NodeInfo, len(data.Nodes))
	for i := range data.Nodes {
		other.Nodes[i] = data.Nodes[i].Clone()
	}

	// Deep copy databases.
	other.Databases = make([]DatabaseInfo, len(data.Databases))
	for i := range data.Databases {
		other.Databases[i] = data.Databases[i].Clone()
	}

	// Copy users.
	other.Users = make([]UserInfo, len(data.Users))
	for i := range data.Users {
		other.Users[i] = data.Users[i].Clone()
	}

	return other
}

// NodeInfo represents information about a single node in the cluster.
type NodeInfo struct {
	ID   uint64
	Host string
}

// Clone returns a deep copy of ni.
func (ni NodeInfo) Clone() NodeInfo { return ni }

// MarshalBinary encodes the object to a binary format.
func (info *NodeInfo) MarshalBinary() ([]byte, error) {
	var pb internal.NodeInfo
	pb.ID = &info.ID
	pb.Host = &info.Host
	return proto.Marshal(&pb)
}

// MarshalBinary decodes the object from a binary format.
func (info *NodeInfo) UnmarshalBinary(buf []byte) error {
	var pb internal.NodeInfo
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	info.ID = pb.GetID()
	info.Host = pb.GetHost()
	return nil
}

// DatabaseInfo represents information about a database in the system.
type DatabaseInfo struct {
	Name                   string
	DefaultRetentionPolicy string
	Policies               []RetentionPolicyInfo
	ContinuousQueries      []ContinuousQueryInfo
}

// Clone returns a deep copy of di.
func (di DatabaseInfo) Clone() DatabaseInfo {
	other := di

	other.Policies = make([]RetentionPolicyInfo, len(di.Policies))
	for i := range di.Policies {
		other.Policies[i] = di.Policies[i].Clone()
	}

	other.ContinuousQueries = make([]ContinuousQueryInfo, len(di.ContinuousQueries))
	for i := range di.ContinuousQueries {
		other.ContinuousQueries[i] = di.ContinuousQueries[i].Clone()
	}

	return other
}

// RetentionPolicy returns a policy on the database by name.
func (db *DatabaseInfo) RetentionPolicy(name string) *RetentionPolicyInfo {
	panic("not yet implemented")
}

// RetentionPolicyInfo represents metadata about a retention policy.
type RetentionPolicyInfo struct {
	Name               string
	ReplicaN           int
	Duration           time.Duration
	ShardGroupDuration time.Duration
	ShardGroups        []ShardGroupInfo
}

// Clone returns a deep copy of rpi.
func (rpi RetentionPolicyInfo) Clone() RetentionPolicyInfo {
	other := rpi

	other.ShardGroups = make([]ShardGroupInfo, len(rpi.ShardGroups))
	for i := range rpi.ShardGroups {
		other.ShardGroups[i] = rpi.ShardGroups[i].Clone()
	}

	return other
}

// ShardGroupInfo represents metadata about a shard group.
type ShardGroupInfo struct {
	ID        uint64
	StartTime time.Time
	EndTime   time.Time
	Shards    []ShardInfo
}

// Clone returns a deep copy of sgi.
func (sgi ShardGroupInfo) Clone() ShardGroupInfo {
	other := sgi

	other.Shards = make([]ShardInfo, len(sgi.Shards))
	for i := range sgi.Shards {
		other.Shards[i] = sgi.Shards[i].Clone()
	}

	return other
}

// ShardFor returns the ShardInfo for a Point hash
func (s *ShardGroupInfo) ShardFor(hash uint64) ShardInfo {
	return s.Shards[hash%uint64(len(s.Shards))]
}

// ShardInfo represents metadata about a shard.
type ShardInfo struct {
	ID       uint64
	OwnerIDs []uint64
}

// Clone returns a deep copy of si.
func (si ShardInfo) Clone() ShardInfo {
	other := si

	other.OwnerIDs = make([]uint64, len(si.OwnerIDs))
	copy(other.OwnerIDs, si.OwnerIDs)

	return other
}

// ContinuousQueryInfo represents metadata about a continuous query.
type ContinuousQueryInfo struct {
	Query string
}

// Clone returns a deep copy of cqi.
func (cqi ContinuousQueryInfo) Clone() ContinuousQueryInfo { return cqi }

// UserInfo represents metadata about a user in the system.
type UserInfo struct {
	Name       string
	Hash       string
	Admin      bool
	Privileges map[string]influxql.Privilege
}

// Clone returns a deep copy of si.
func (ui UserInfo) Clone() UserInfo {
	other := ui

	other.Privileges = make(map[string]influxql.Privilege)
	for k, v := range ui.Privileges {
		other.Privileges[k] = v
	}

	return other
}
