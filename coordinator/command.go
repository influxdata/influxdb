package coordinator

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/_vendor/raft"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/protocol"
)

const (
	SetContinuousQueryTimestampCommandID int = iota + 1
)

type SetContinuousQueryTimestampCommand struct {
	Timestamp time.Time `json:"timestamp"`
}

func (c *SetContinuousQueryTimestampCommand) Apply() (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.SetContinuousQueryTimestamp(c.Timestamp)
	return nil, err
}

type CreateContinuousQueryCommand struct {
	Database string `json:"database"`
	Query    string `json:"query"`
}

func (c *CreateContinuousQueryCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.CreateContinuousQuery(c.Database, c.Query)
	return nil, err
}

type DeleteContinuousQueryCommand struct {
	Database string `json:"database"`
	Id       uint32 `json:"id"`
}

func (c *DeleteContinuousQueryCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.DeleteContinuousQuery(c.Database, c.Id)
	return nil, err
}

type DropDatabaseCommand struct {
	Name string `json:"name"`
}

func (c *DropDatabaseCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.DropDatabase(c.Name)
	return nil, err
}

type CreateDatabaseCommand struct {
	Name string `json:"name"`
}

func (c *CreateDatabaseCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.CreateDatabase(c.Name)
	return nil, err
}

type SaveDbUserCommand struct {
	User *cluster.DbUser `json:"user"`
}

func (c *SaveDbUserCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	config.SaveDbUser(c.User)
	log.Debug("(raft:%s) Created user %s:%s", server.Name(), c.User.Db, c.User.Name)
	return nil, nil
}

type ChangeDbUserPassword struct {
	Database string
	Username string
	Hash     string
}

func (c *ChangeDbUserPassword) Apply(server raft.Server) (interface{}, error) {
	log.Debug("(raft:%s) changing db user password for %s:%s", server.Name(), c.Database, c.Username)
	config := server.Context().(*cluster.ClusterConfiguration)
	return nil, config.ChangeDbUserPassword(c.Database, c.Username, c.Hash)
}

type ChangeDbUserPermissions struct {
	Database         string
	Username         string
	ReadPermissions  string
	WritePermissions string
}

func (c *ChangeDbUserPermissions) Apply(server raft.Server) (interface{}, error) {
	log.Debug("(raft:%s) changing db user permissions for %s:%s", server.Name(), c.Database, c.Username)
	config := server.Context().(*cluster.ClusterConfiguration)
	return nil, config.ChangeDbUserPermissions(c.Database, c.Username, c.ReadPermissions, c.WritePermissions)
}

type SaveClusterAdminCommand struct {
	User *cluster.ClusterAdmin `json:"user"`
}

func (c *SaveClusterAdminCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	config.SaveClusterAdmin(c.User)
	return nil, nil
}

type InfluxJoinCommand struct {
	Name                     string `json:"name"`
	ConnectionString         string `json:"connectionString"`
	ProtobufConnectionString string `json:"protobufConnectionString"`
}

func (c *InfluxJoinCommand) Apply(server raft.Server) (interface{}, error) {
	err := server.AddPeer(c.Name, c.ConnectionString)
	if err != nil {
		return nil, err
	}

	clusterConfig := server.Context().(*cluster.ClusterConfiguration)

	newServer := clusterConfig.GetServerByRaftName(c.Name)
	// it's a new server the cluster has never seen, make it a potential
	if newServer != nil {
		return nil, fmt.Errorf("Server %s already exist", c.Name)
	}

	log.Info("Adding new server to the cluster config %s", c.Name)
	clusterServer := cluster.NewClusterServer(c.Name,
		c.ConnectionString,
		c.ProtobufConnectionString,
		nil,
		clusterConfig.GetLocalConfiguration())
	clusterConfig.AddPotentialServer(clusterServer)
	return nil, nil
}

type InfluxForceLeaveCommand struct {
	Id uint32 `json:"id"`
}

func (c *InfluxForceLeaveCommand) Apply(server raft.Server) (interface{}, error) {
	clusterConfig := server.Context().(*cluster.ClusterConfiguration)
	s := clusterConfig.GetServerById(&c.Id)

	if s == nil {
		return nil, nil
	}

	if err := server.RemovePeer(s.RaftName); err != nil {
		log.Warn("Cannot remove peer: %s", err)
	}

	if err := clusterConfig.RemoveServer(s); err != nil {
		log.Warn("Cannot remove peer from cluster config: %s", err)
	}
	server.FlushCommitIndex()
	return nil, nil
}

type InfluxChangeConnectionStringCommand struct {
	Name                     string `json:"name"`
	Force                    bool   `json:"force"`
	ConnectionString         string `json:"connectionString"`
	ProtobufConnectionString string `json:"protobufConnectionString"`
}

func (c *InfluxChangeConnectionStringCommand) Apply(server raft.Server) (interface{}, error) {
	if c.Name == server.Name() {
		return nil, nil
	}

	server.RemovePeer(c.Name)
	server.AddPeer(c.Name, c.ConnectionString)

	clusterConfig := server.Context().(*cluster.ClusterConfiguration)

	newServer := clusterConfig.GetServerByRaftName(c.Name)
	// it's a new server the cluster has never seen, make it a potential
	if newServer == nil {
		return nil, fmt.Errorf("Server %s doesn't exist", c.Name)
	}

	newServer.RaftConnectionString = c.ConnectionString
	newServer.ProtobufConnectionString = c.ProtobufConnectionString
	server.Context().(*cluster.ClusterConfiguration).ChangeProtobufConnectionString(newServer)
	server.FlushCommitIndex()
	return nil, nil
}

type CreateShardsCommand struct {
	Shards    []*cluster.NewShardData
	SpaceName string
}

func (c *CreateShardsCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	createdShards, err := config.AddShards(c.Shards)
	if err != nil {
		return nil, err
	}
	createdShardData := make([]*cluster.NewShardData, 0)
	for _, s := range createdShards {
		createdShardData = append(createdShardData, s.ToNewShardData())
	}
	return createdShardData, nil
}

type DropShardCommand struct {
	ShardId   uint32
	ServerIds []uint32
}

func (c *DropShardCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.DropShard(c.ShardId, c.ServerIds)
	return nil, err
}

type CreateSeriesFieldIdsCommand struct {
	Database string
	Series   []*protocol.Series
}

func (c *CreateSeriesFieldIdsCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.MetaStore.GetOrSetFieldIds(c.Database, c.Series)
	return c.Series, err
}

type DropSeriesCommand struct {
	Database string
	Series   string
}

func (c *DropSeriesCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.DropSeries(c.Database, c.Series)
	return nil, err
}

type CreateShardSpaceCommand struct {
	ShardSpace *cluster.ShardSpace
}

func (c *CreateShardSpaceCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.AddShardSpace(c.ShardSpace)
	return nil, err
}

type DropShardSpaceCommand struct {
	Database string
	Name     string
}

func (c *DropShardSpaceCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.RemoveShardSpace(c.Database, c.Name)
	return nil, err
}

type UpdateShardSpaceCommand struct {
	ShardSpace *cluster.ShardSpace
}

func (c *UpdateShardSpaceCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.UpdateShardSpace(c.ShardSpace)
	return nil, err
}
