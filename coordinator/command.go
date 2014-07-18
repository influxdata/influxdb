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

var internalRaftCommands map[string]raft.Command

func init() {
	internalRaftCommands = map[string]raft.Command{}
	for _, command := range []raft.Command{
		&InfluxJoinCommand{},
		&InfluxForceLeaveCommand{},
		&InfluxChangeConnectionStringCommand{},
		&CreateDatabaseCommand{},
		&DropDatabaseCommand{},
		&SaveDbUserCommand{},
		&SaveClusterAdminCommand{},
		&ChangeDbUserPassword{},
		&ChangeDbUserPermissions{},
		&CreateContinuousQueryCommand{},
		&DeleteContinuousQueryCommand{},
		&SetContinuousQueryTimestampCommand{},
		&CreateShardsCommand{},
		&DropShardCommand{},
		&CreateSeriesFieldIdsCommand{},
		&DropSeriesCommand{},
		&CreateShardSpaceCommand{},
		&DropShardSpaceCommand{},
	} {
		internalRaftCommands[command.CommandName()] = command
	}
}

type SetContinuousQueryTimestampCommand struct {
	Timestamp time.Time `json:"timestamp"`
}

func NewSetContinuousQueryTimestampCommand(timestamp time.Time) *SetContinuousQueryTimestampCommand {
	return &SetContinuousQueryTimestampCommand{timestamp}
}

func (c *SetContinuousQueryTimestampCommand) CommandName() string {
	return "set_cq_ts"
}

func (c *SetContinuousQueryTimestampCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.SetContinuousQueryTimestamp(c.Timestamp)
	return nil, err
}

type CreateContinuousQueryCommand struct {
	Database string `json:"database"`
	Query    string `json:"query"`
}

func NewCreateContinuousQueryCommand(database string, query string) *CreateContinuousQueryCommand {
	return &CreateContinuousQueryCommand{database, query}
}

func (c *CreateContinuousQueryCommand) CommandName() string {
	return "create_cq"
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

func NewDeleteContinuousQueryCommand(database string, id uint32) *DeleteContinuousQueryCommand {
	return &DeleteContinuousQueryCommand{database, id}
}

func (c *DeleteContinuousQueryCommand) CommandName() string {
	return "delete_cq"
}

func (c *DeleteContinuousQueryCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.DeleteContinuousQuery(c.Database, c.Id)
	return nil, err
}

type DropDatabaseCommand struct {
	Name string `json:"name"`
}

func NewDropDatabaseCommand(name string) *DropDatabaseCommand {
	return &DropDatabaseCommand{name}
}

func (c *DropDatabaseCommand) CommandName() string {
	return "drop_db"
}

func (c *DropDatabaseCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.DropDatabase(c.Name)
	return nil, err
}

type CreateDatabaseCommand struct {
	Name string `json:"name"`
}

func NewCreateDatabaseCommand(name string) *CreateDatabaseCommand {
	return &CreateDatabaseCommand{name}
}

func (c *CreateDatabaseCommand) CommandName() string {
	return "create_db"
}

func (c *CreateDatabaseCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.CreateDatabase(c.Name)
	return nil, err
}

type SaveDbUserCommand struct {
	User *cluster.DbUser `json:"user"`
}

func NewSaveDbUserCommand(u *cluster.DbUser) *SaveDbUserCommand {
	return &SaveDbUserCommand{
		User: u,
	}
}

func (c *SaveDbUserCommand) CommandName() string {
	return "save_db_user"
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

func NewChangeDbUserPasswordCommand(db, username, hash string) *ChangeDbUserPassword {
	return &ChangeDbUserPassword{
		Database: db,
		Username: username,
		Hash:     hash,
	}
}

func (c *ChangeDbUserPassword) CommandName() string {
	return "change_db_user_password"
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

func NewChangeDbUserPermissionsCommand(db, username, readPermissions, writePermissions string) *ChangeDbUserPermissions {
	return &ChangeDbUserPermissions{
		Database:         db,
		Username:         username,
		ReadPermissions:  readPermissions,
		WritePermissions: writePermissions,
	}
}

func (c *ChangeDbUserPermissions) CommandName() string {
	return "change_db_user_permissions"
}

func (c *ChangeDbUserPermissions) Apply(server raft.Server) (interface{}, error) {
	log.Debug("(raft:%s) changing db user permissions for %s:%s", server.Name(), c.Database, c.Username)
	config := server.Context().(*cluster.ClusterConfiguration)
	return nil, config.ChangeDbUserPermissions(c.Database, c.Username, c.ReadPermissions, c.WritePermissions)
}

type SaveClusterAdminCommand struct {
	User *cluster.ClusterAdmin `json:"user"`
}

func NewSaveClusterAdminCommand(u *cluster.ClusterAdmin) *SaveClusterAdminCommand {
	return &SaveClusterAdminCommand{
		User: u,
	}
}

func (c *SaveClusterAdminCommand) CommandName() string {
	return "save_cluster_admin_user"
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

// The name of the Join command in the log
func (c *InfluxJoinCommand) CommandName() string {
	return "join"
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

func (c *InfluxJoinCommand) NodeName() string {
	return c.Name
}

type InfluxForceLeaveCommand struct {
	Id uint32 `json:"id"`
}

// The name of the ForceLeave command in the log
func (c *InfluxForceLeaveCommand) CommandName() string {
	return "force_leave"
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

// The name of the ChangeConnectionString command in the log
func (c *InfluxChangeConnectionStringCommand) CommandName() string {
	return "change_connection_string"
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

func (c *InfluxChangeConnectionStringCommand) NodeName() string {
	return c.Name
}

type CreateShardsCommand struct {
	Shards    []*cluster.NewShardData
	SpaceName string
}

func NewCreateShardsCommand(shards []*cluster.NewShardData) *CreateShardsCommand {
	return &CreateShardsCommand{Shards: shards}
}

func (c *CreateShardsCommand) CommandName() string {
	return "create_shards"
}

// TODO: Encode/Decode are not needed once this pr
// https://github.com/influxdb/influxdb/vendor/raft/pull/221 is merged in and our goraft
// is updated to a commit that includes the pr

func (c *CreateShardsCommand) Encode(w io.Writer) error {
	return json.NewEncoder(w).Encode(c)
}
func (c *CreateShardsCommand) Decode(r io.Reader) error {
	return json.NewDecoder(r).Decode(c)
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

func NewDropShardCommand(id uint32, serverIds []uint32) *DropShardCommand {
	return &DropShardCommand{ShardId: id, ServerIds: serverIds}
}

func (c *DropShardCommand) CommandName() string {
	return "drop_shard"
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

func NewCreateSeriesFieldIdsCommand(database string, series []*protocol.Series) *CreateSeriesFieldIdsCommand {
	return &CreateSeriesFieldIdsCommand{Database: database, Series: series}
}

func (c *CreateSeriesFieldIdsCommand) CommandName() string {
	return "create_series_field_ids"
}

// TODO: Encode/Decode are not needed once this pr
// https://github.com/goraft/raft/pull/221 is merged in and our goraft
// is updated to a commit that includes the pr

func (c *CreateSeriesFieldIdsCommand) Encode(w io.Writer) error {
	return json.NewEncoder(w).Encode(c)
}

func (c *CreateSeriesFieldIdsCommand) Decode(r io.Reader) error {
	return json.NewDecoder(r).Decode(c)
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

func NewDropSeriesCommand(database, series string) *DropSeriesCommand {
	return &DropSeriesCommand{Database: database, Series: series}
}

func (c *DropSeriesCommand) CommandName() string {
	return "drop_series"
}

func (c *DropSeriesCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.DropSeries(c.Database, c.Series)
	return nil, err
}

type CreateShardSpaceCommand struct {
	ShardSpace *cluster.ShardSpace
}

func NewCreateShardSpaceCommand(space *cluster.ShardSpace) *CreateShardSpaceCommand {
	return &CreateShardSpaceCommand{ShardSpace: space}
}

func (c *CreateShardSpaceCommand) CommandName() string {
	return "add_shard_space"
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

func NewDropShardSpaceCommand(database, name string) *DropShardSpaceCommand {
	return &DropShardSpaceCommand{Database: database, Name: name}
}

func (c *DropShardSpaceCommand) CommandName() string {
	return "remove_shard_space"
}

func (c *DropShardSpaceCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*cluster.ClusterConfiguration)
	err := config.RemoveShardSpace(c.Database, c.Name)
	return nil, err
}
