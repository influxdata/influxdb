package coordinator

import (
	"github.com/goraft/raft"
)

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
	Name string `json:"name"`
}

func NewCreateDatabaseCommand(name string) *CreateDatabaseCommand {
	return &CreateDatabaseCommand{name}
}

func (c *CreateDatabaseCommand) CommandName() string {
	return "create_db"
}

func (c *CreateDatabaseCommand) Apply(server raft.Server) (interface{}, error) {
	config := server.Context().(*ClusterConfiguration)
	err := config.CreateDatabase(c.Name)
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
	return nil, nil
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
