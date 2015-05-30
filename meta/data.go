package meta

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta/internal"
)

//go:generate protoc --gogo_out=. internal/meta.proto

const (
	// DefaultRetentionPolicyReplicaN is the default value of RetentionPolicyInfo.ReplicaN.
	DefaultRetentionPolicyReplicaN = 1

	// DefaultRetentionPolicyDuration is the default value of RetentionPolicyInfo.Duration.
	DefaultRetentionPolicyDuration = 7 * (24 * time.Hour)

	// MinRetentionPolicyDuration represents the minimum duration for a policy.
	MinRetentionPolicyDuration = time.Hour
)

// Data represents the top level collection of all metadata.
type Data struct {
	Version   uint64 // autoincrementing version
	Nodes     []NodeInfo
	Databases []DatabaseInfo
	Users     []UserInfo

	MaxNodeID       uint64
	MaxShardGroupID uint64
	MaxShardID      uint64
}

// Node returns a node by id.
func (data *Data) Node(id uint64) *NodeInfo {
	for i := range data.Nodes {
		if data.Nodes[i].ID == id {
			return &data.Nodes[i]
		}
	}
	return nil
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

// CreateNode adds a node to the metadata.
func (data *Data) CreateNode(host string) error {
	// Ensure a node with the same host doesn't already exist.
	if data.NodeByHost(host) != nil {
		return ErrNodeExists
	}

	// Append new node.
	data.MaxNodeID++
	data.Nodes = append(data.Nodes, NodeInfo{
		ID:   data.MaxNodeID,
		Host: host,
	})

	return nil
}

// DeleteNode removes a node from the metadata.
func (data *Data) DeleteNode(id uint64) error {
	for i := range data.Nodes {
		if data.Nodes[i].ID == id {
			data.Nodes = append(data.Nodes[:i], data.Nodes[i+1:]...)
			return nil
		}
	}
	return ErrNodeNotFound
}

// Database returns a database by name.
func (data *Data) Database(name string) *DatabaseInfo {
	for i := range data.Databases {
		if data.Databases[i].Name == name {
			return &data.Databases[i]
		}
	}
	return nil
}

// CreateDatabase creates a new database.
// Returns an error if name is blank or if a database with the same name already exists.
func (data *Data) CreateDatabase(name string) error {
	if name == "" {
		return ErrDatabaseNameRequired
	} else if data.Database(name) != nil {
		return ErrDatabaseExists
	}

	// Append new node.
	data.Databases = append(data.Databases, DatabaseInfo{Name: name})

	return nil
}

// DropDatabase removes a database by name.
func (data *Data) DropDatabase(name string) error {
	for i := range data.Databases {
		if data.Databases[i].Name == name {
			data.Databases = append(data.Databases[:i], data.Databases[i+1:]...)
			return nil
		}
	}
	return ErrDatabaseNotFound
}

// RetentionPolicy returns a retention policy for a database by name.
func (data *Data) RetentionPolicy(database, name string) (*RetentionPolicyInfo, error) {
	di := data.Database(database)
	if di == nil {
		return nil, ErrDatabaseNotFound
	}

	for i := range di.RetentionPolicies {
		if di.RetentionPolicies[i].Name == name {
			return &di.RetentionPolicies[i], nil
		}
	}
	return nil, nil
}

// CreateRetentionPolicy creates a new retention policy on a database.
// Returns an error if name is blank or if a database does not exist.
func (data *Data) CreateRetentionPolicy(database string, rpi *RetentionPolicyInfo) error {
	// Validate retention policy.
	if rpi.Name == "" {
		return ErrRetentionPolicyNameRequired
	}

	// Find database.
	di := data.Database(database)
	if di == nil {
		return ErrDatabaseNotFound
	} else if di.RetentionPolicy(rpi.Name) != nil {
		return ErrRetentionPolicyExists
	}

	// Append new policy.
	di.RetentionPolicies = append(di.RetentionPolicies, RetentionPolicyInfo{
		Name:               rpi.Name,
		Duration:           rpi.Duration,
		ShardGroupDuration: shardGroupDuration(rpi.Duration),
		ReplicaN:           rpi.ReplicaN,
	})

	return nil
}

// DropRetentionPolicy removes a retention policy from a database by name.
func (data *Data) DropRetentionPolicy(database, name string) error {
	// Find database.
	di := data.Database(database)
	if di == nil {
		return ErrDatabaseNotFound
	}

	// Remove from list.
	for i := range di.RetentionPolicies {
		if di.RetentionPolicies[i].Name == name {
			di.RetentionPolicies = append(di.RetentionPolicies[:i], di.RetentionPolicies[i+1:]...)
			return nil
		}
	}
	return ErrRetentionPolicyNotFound
}

// UpdateRetentionPolicy updates an existing retention policy.
func (data *Data) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate) error {
	// Find database.
	di := data.Database(database)
	if di == nil {
		return ErrDatabaseNotFound
	}

	// Find policy.
	rpi := di.RetentionPolicy(name)
	if rpi == nil {
		return ErrRetentionPolicyNotFound
	}

	// Ensure new policy doesn't match an existing policy.
	if rpu.Name != nil && *rpu.Name != name && di.RetentionPolicy(*rpu.Name) != nil {
		return ErrRetentionPolicyNameExists
	}

	// Enforce duration of at least MinRetentionPolicyDuration
	if rpu.Duration != nil && *rpu.Duration < MinRetentionPolicyDuration && *rpu.Duration != 0 {
		return ErrRetentionPolicyDurationTooLow
	}

	// Update fields.
	if rpu.Name != nil {
		rpi.Name = *rpu.Name
	}
	if rpu.Duration != nil {
		rpi.Duration = *rpu.Duration
	}
	if rpu.ReplicaN != nil {
		rpi.ReplicaN = *rpu.ReplicaN
	}

	return nil
}

// SetDefaultRetentionPolicy sets the default retention policy for a database.
func (data *Data) SetDefaultRetentionPolicy(database, name string) error {
	// Find database and verify policy exists.
	di := data.Database(database)
	if di == nil {
		return ErrDatabaseNotFound
	} else if di.RetentionPolicy(name) == nil {
		return ErrRetentionPolicyNotFound
	}

	// Set default policy.
	di.DefaultRetentionPolicy = name

	return nil
}

// ShardGroup returns a list of all shard groups on a database and policy.
func (data *Data) ShardGroups(database, policy string) ([]ShardGroupInfo, error) {
	// Find retention policy.
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, ErrRetentionPolicyNotFound
	}
	return rpi.ShardGroups, nil
}

// ShardGroupByTimestamp returns the shard group on a database and policy for a given timestamp.
func (data *Data) ShardGroupByTimestamp(database, policy string, timestamp time.Time) (*ShardGroupInfo, error) {
	// Find retention policy.
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return nil, err
	} else if rpi == nil {
		return nil, ErrRetentionPolicyNotFound
	}

	return rpi.ShardGroupByTimestamp(timestamp), nil
}

// CreateShardGroup creates a shard group on a database and policy for a given timestamp.
func (data *Data) CreateShardGroup(database, policy string, timestamp time.Time) error {
	// Ensure there are nodes in the metadata.
	if len(data.Nodes) == 0 {
		return ErrNodesRequired
	}

	// Find retention policy.
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return err
	} else if rpi == nil {
		return ErrRetentionPolicyNotFound
	}

	// Verify that shard group doesn't already exist for this timestamp.
	if rpi.ShardGroupByTimestamp(timestamp) != nil {
		return ErrShardGroupExists
	}

	// Require at least one replica but no more replicas than nodes.
	replicaN := rpi.ReplicaN
	if replicaN == 0 {
		replicaN = 1
	} else if replicaN > len(data.Nodes) {
		replicaN = len(data.Nodes)
	}

	// Determine shard count by node count divided by replication factor.
	// This will ensure nodes will get distributed across nodes evenly and
	// replicated the correct number of times.
	shardN := len(data.Nodes) / replicaN

	// Create the shard group.
	data.MaxShardGroupID++
	sgi := ShardGroupInfo{}
	sgi.ID = data.MaxShardGroupID
	sgi.StartTime = timestamp.Truncate(rpi.ShardGroupDuration).UTC()
	sgi.EndTime = sgi.StartTime.Add(rpi.ShardGroupDuration).UTC()

	// Create shards on the group.
	sgi.Shards = make([]ShardInfo, shardN)
	for i := range sgi.Shards {
		data.MaxShardID++
		sgi.Shards[i] = ShardInfo{ID: data.MaxShardID}
	}

	// Assign data nodes to shards via round robin.
	// Start from a repeatably "random" place in the node list.
	nodeIndex := int(data.Version % uint64(len(data.Nodes)))
	for i := range sgi.Shards {
		si := &sgi.Shards[i]
		for j := 0; j < replicaN; j++ {
			nodeID := data.Nodes[nodeIndex%len(data.Nodes)].ID
			si.OwnerIDs = append(si.OwnerIDs, nodeID)
			nodeIndex++
		}
	}

	// Retention policy has a new shard group, so update the policy.
	rpi.ShardGroups = append(rpi.ShardGroups, sgi)

	return nil
}

// DeleteShardGroup removes a shard group from a database and retention policy by id.
func (data *Data) DeleteShardGroup(database, policy string, id uint64) error {
	// Find retention policy.
	rpi, err := data.RetentionPolicy(database, policy)
	if err != nil {
		return err
	} else if rpi == nil {
		return ErrRetentionPolicyNotFound
	}

	// Find shard group by ID and remove it.
	for i := range rpi.ShardGroups {
		if rpi.ShardGroups[i].ID == id {
			rpi.ShardGroups = append(rpi.ShardGroups[:i], rpi.ShardGroups[i+1:]...)
			return nil
		}
	}

	return ErrShardGroupNotFound
}

// CreateContinuousQuery adds a named continuous query to a database.
func (data *Data) CreateContinuousQuery(database, name, query string) error {
	di := data.Database(database)
	if di == nil {
		return ErrDatabaseNotFound
	}

	// Ensure the name doesn't already exist.
	for i := range di.ContinuousQueries {
		if di.ContinuousQueries[i].Name == name {
			return ErrContinuousQueryExists
		}
	}

	// Append new query.
	di.ContinuousQueries = append(di.ContinuousQueries, ContinuousQueryInfo{
		Name:  name,
		Query: query,
	})

	return nil
}

// DropContinuousQuery removes a continuous query.
func (data *Data) DropContinuousQuery(database, name string) error {
	di := data.Database(database)
	if di == nil {
		return ErrDatabaseNotFound
	}

	for i := range di.ContinuousQueries {
		if di.ContinuousQueries[i].Name == name {
			di.ContinuousQueries = append(di.ContinuousQueries[:i], di.ContinuousQueries[i+1:]...)
			return nil
		}
	}
	return ErrContinuousQueryNotFound
}

// User returns a user by username.
func (data *Data) User(username string) *UserInfo {
	for i := range data.Users {
		if data.Users[i].Name == username {
			return &data.Users[i]
		}
	}
	return nil
}

// CreateUser creates a new user.
func (data *Data) CreateUser(name, hash string, admin bool) error {
	// Ensure the user doesn't already exist.
	if name == "" {
		return ErrUsernameRequired
	} else if data.User(name) != nil {
		return ErrUserExists
	}

	// Append new user.
	data.Users = append(data.Users, UserInfo{
		Name:  name,
		Hash:  hash,
		Admin: admin,
	})

	return nil
}

// DropUser removes an existing user by name.
func (data *Data) DropUser(name string) error {
	for i := range data.Users {
		if data.Users[i].Name == name {
			data.Users = append(data.Users[:i], data.Users[i+1:]...)
			return nil
		}
	}
	return ErrUserNotFound
}

// UpdateUser updates the password hash of an existing user.
func (data *Data) UpdateUser(name, hash string) error {
	for i := range data.Users {
		if data.Users[i].Name == name {
			data.Users[i].Hash = hash
			return nil
		}
	}
	return ErrUserNotFound
}

// SetPrivilege sets a privilege for a user on a database.
func (data *Data) SetPrivilege(name, database string, p influxql.Privilege) error {
	ui := data.User(name)
	if ui == nil {
		return ErrUserNotFound
	}

	if ui.Privileges == nil {
		ui.Privileges = make(map[string]influxql.Privilege)
	}
	ui.Privileges[database] = p

	return nil
}

// Clone returns a copy of data with a new version.
func (data *Data) Clone() *Data {
	other := *data
	other.Version++

	// Copy nodes.
	if data.Nodes != nil {
		other.Nodes = make([]NodeInfo, len(data.Nodes))
		for i := range data.Nodes {
			other.Nodes[i] = data.Nodes[i].clone()
		}
	}

	// Deep copy databases.
	if data.Databases != nil {
		other.Databases = make([]DatabaseInfo, len(data.Databases))
		for i := range data.Databases {
			other.Databases[i] = data.Databases[i].clone()
		}
	}

	// Copy users.
	if data.Users != nil {
		other.Users = make([]UserInfo, len(data.Users))
		for i := range data.Users {
			other.Users[i] = data.Users[i].clone()
		}
	}

	return &other
}

// NodeInfo represents information about a single node in the cluster.
type NodeInfo struct {
	ID   uint64
	Host string
}

// clone returns a deep copy of ni.
func (ni NodeInfo) clone() NodeInfo { return ni }

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
	RetentionPolicies      []RetentionPolicyInfo
	ContinuousQueries      []ContinuousQueryInfo
}

// RetentionPolicy returns a retention policy by name.
func (di DatabaseInfo) RetentionPolicy(name string) *RetentionPolicyInfo {
	for i := range di.RetentionPolicies {
		if di.RetentionPolicies[i].Name == name {
			return &di.RetentionPolicies[i]
		}
	}
	return nil
}

// clone returns a deep copy of di.
func (di DatabaseInfo) clone() DatabaseInfo {
	other := di

	if di.RetentionPolicies != nil {
		other.RetentionPolicies = make([]RetentionPolicyInfo, len(di.RetentionPolicies))
		for i := range di.RetentionPolicies {
			other.RetentionPolicies[i] = di.RetentionPolicies[i].clone()
		}
	}

	// Copy continuous queries.
	if di.ContinuousQueries != nil {
		other.ContinuousQueries = make([]ContinuousQueryInfo, len(di.ContinuousQueries))
		for i := range di.ContinuousQueries {
			other.ContinuousQueries[i] = di.ContinuousQueries[i].clone()
		}
	}

	return other
}

// RetentionPolicyInfo represents metadata about a retention policy.
type RetentionPolicyInfo struct {
	Name               string
	ReplicaN           int
	Duration           time.Duration
	ShardGroupDuration time.Duration
	ShardGroups        []ShardGroupInfo
}

// NewRetentionPolicyInfo returns a new instance of RetentionPolicyInfo with defaults set.
func NewRetentionPolicyInfo(name string) *RetentionPolicyInfo {
	return &RetentionPolicyInfo{
		Name:     name,
		ReplicaN: DefaultRetentionPolicyReplicaN,
		Duration: DefaultRetentionPolicyDuration,
	}
}

// ShardGroupByTimestamp returns the shard group in the policy that contains the timestamp.
func (rpi *RetentionPolicyInfo) ShardGroupByTimestamp(timestamp time.Time) *ShardGroupInfo {
	for i := range rpi.ShardGroups {
		if rpi.ShardGroups[i].Contains(timestamp) {
			return &rpi.ShardGroups[i]
		}
	}
	return nil
}

// protobuf returns a protocol buffers object.
func (rpi *RetentionPolicyInfo) protobuf() *internal.RetentionPolicyInfo {
	return &internal.RetentionPolicyInfo{
		Name:               proto.String(rpi.Name),
		ReplicaN:           proto.Uint32(uint32(rpi.ReplicaN)),
		Duration:           proto.Int64(int64(rpi.Duration)),
		ShardGroupDuration: proto.Int64(int64(rpi.ShardGroupDuration)),
	}
}

// clone returns a deep copy of rpi.
func (rpi RetentionPolicyInfo) clone() RetentionPolicyInfo {
	other := rpi

	if rpi.ShardGroups != nil {
		other.ShardGroups = make([]ShardGroupInfo, len(rpi.ShardGroups))
		for i := range rpi.ShardGroups {
			other.ShardGroups[i] = rpi.ShardGroups[i].clone()
		}
	}

	return other
}

// shardGroupDuration returns the duration for a shard group based on a policy duration.
func shardGroupDuration(d time.Duration) time.Duration {
	if d >= 180*24*time.Hour || d == 0 { // 6 months or 0
		return 7 * 24 * time.Hour
	} else if d >= 2*24*time.Hour { // 2 days
		return 1 * 24 * time.Hour
	}
	return 1 * time.Hour
}

// ShardGroupInfo represents metadata about a shard group.
type ShardGroupInfo struct {
	ID        uint64
	StartTime time.Time
	EndTime   time.Time
	Shards    []ShardInfo
}

// Contains return true if the shard group contains data for the timestamp.
func (sgi *ShardGroupInfo) Contains(timestamp time.Time) bool {
	return !sgi.StartTime.After(timestamp) && sgi.EndTime.After(timestamp)
}

// Overlaps return whether the shard group contains data for the time range between min and max
func (sgi *ShardGroupInfo) Overlaps(min, max time.Time) bool {
	return !sgi.StartTime.After(max) && sgi.EndTime.After(min)
}

// clone returns a deep copy of sgi.
func (sgi ShardGroupInfo) clone() ShardGroupInfo {
	other := sgi

	if sgi.Shards != nil {
		other.Shards = make([]ShardInfo, len(sgi.Shards))
		for i := range sgi.Shards {
			other.Shards[i] = sgi.Shards[i].clone()
		}
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

// clone returns a deep copy of si.
func (si ShardInfo) clone() ShardInfo {
	other := si

	if si.OwnerIDs != nil {
		other.OwnerIDs = make([]uint64, len(si.OwnerIDs))
		copy(other.OwnerIDs, si.OwnerIDs)
	}

	return other
}

// ContinuousQueryInfo represents metadata about a continuous query.
type ContinuousQueryInfo struct {
	Name  string
	Query string
}

// clone returns a deep copy of cqi.
func (cqi ContinuousQueryInfo) clone() ContinuousQueryInfo { return cqi }

// UserInfo represents metadata about a user in the system.
type UserInfo struct {
	Name       string
	Hash       string
	Admin      bool
	Privileges map[string]influxql.Privilege
}

// Authorize returns true if the user is authorized and false if not.
func (ui *UserInfo) Authorize(privilege influxql.Privilege, database string) bool {
	p, ok := ui.Privileges[database]
	return (ok && p >= privilege) || (ui.Admin)
}

// clone returns a deep copy of si.
func (ui UserInfo) clone() UserInfo {
	other := ui

	if ui.Privileges != nil {
		other.Privileges = make(map[string]influxql.Privilege)
		for k, v := range ui.Privileges {
			other.Privileges[k] = v
		}
	}

	return other
}
