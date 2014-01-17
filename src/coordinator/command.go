package coordinator

import (
	log "code.google.com/p/log4go"
	"github.com/goraft/raft"
	"time"
)

var internalRaftCommands map[string]raft.Command

func init() {
	internalRaftCommands = map[string]raft.Command{}
	for _, command := range []raft.Command{
		&AddPotentialServerCommand{},
		&UpdateServerStateCommand{},
		&CreateDatabaseCommand{},
		&DropDatabaseCommand{},
		&SaveDbUserCommand{},
		&SaveClusterAdminCommand{},
		&ChangeDbUserPassword{},
		&CreateContinuousQueryCommand{},
		&DeleteContinuousQueryCommand{},
		&SetContinuousQueryTimestampCommand{},
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
	config := server.Context().(*ClusterConfiguration)
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
	config := server.Context().(*ClusterConfiguration)
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
	config := server.Context().(*ClusterConfiguration)
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
	config := server.Context().(*ClusterConfiguration)
	err := config.DropDatabase(c.Name)
	return nil, err
}

type CreateDatabaseCommand struct {
	Name              string `json:"name"`
	ReplicationFactor uint8  `json:"replicationFactor"`
}

func NewCreateDatabaseCommand(name string, replicationFactor uint8) *CreateDatabaseCommand {
	return &CreateDatabaseCommand{name, replicationFactor}
}

func (c *CreateDatabaseCommand) CommandName() string {
	return "create_db"
}

func (c *CreateDatabaseCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*ClusterConfiguration)
	err := config.CreateDatabase(c.Name, c.ReplicationFactor)
	return nil, err
}

type SaveDbUserCommand struct {
	User *dbUser `json:"user"`
}

func NewSaveDbUserCommand(u *dbUser) *SaveDbUserCommand {
	return &SaveDbUserCommand{
		User: u,
	}
}

func (c *SaveDbUserCommand) CommandName() string {
	return "save_db_user"
}

func (c *SaveDbUserCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*ClusterConfiguration)
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
	config := server.Context().(*ClusterConfiguration)
	return nil, config.ChangeDbUserPassword(c.Database, c.Username, c.Hash)
}

type SaveClusterAdminCommand struct {
	User *clusterAdmin `json:"user"`
}

func NewSaveClusterAdminCommand(u *clusterAdmin) *SaveClusterAdminCommand {
	return &SaveClusterAdminCommand{
		User: u,
	}
}

func (c *SaveClusterAdminCommand) CommandName() string {
	return "save_cluster_admin_user"
}

func (c *SaveClusterAdminCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*ClusterConfiguration)
	config.SaveClusterAdmin(c.User)
	return nil, nil
}

type AddPotentialServerCommand struct {
	Server *ClusterServer
}

func NewAddPotentialServerCommand(s *ClusterServer) *AddPotentialServerCommand {
	return &AddPotentialServerCommand{Server: s}
}

func (c *AddPotentialServerCommand) CommandName() string {
	return "add_server"
}

func (c *AddPotentialServerCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*ClusterConfiguration)
	config.AddPotentialServer(c.Server)
	return nil, nil
}

type UpdateServerStateCommand struct {
	ServerId uint32
	State    ServerState
}

func NewUpdateServerStateCommand(serverId uint32, state ServerState) *UpdateServerStateCommand {
	return &UpdateServerStateCommand{ServerId: serverId, State: state}
}

func (c *UpdateServerStateCommand) CommandName() string {
	return "update_state"
}

func (c *UpdateServerStateCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*ClusterConfiguration)
	err := config.UpdateServerState(c.ServerId, c.State)
	return nil, err
}

type InfluxJoinCommand struct {
	Name                     string `json:"name"`
	ConnectionString         string `json:"connectionString"`
	ProtobufConnectionString string `json:"protobufConnectionString"`
}

// The name of the Join command in the log
func (c *InfluxJoinCommand) CommandName() string {
	return "raft:join"
}

func (c *InfluxJoinCommand) Apply(server raft.Server) (interface{}, error) {
	err := server.AddPeer(c.Name, c.ConnectionString)

	return []byte("join"), err
}

func (c *InfluxJoinCommand) NodeName() string {
	return c.Name
}
