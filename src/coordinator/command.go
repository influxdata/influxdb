package coordinator

import (
	"github.com/goraft/raft"
)

type AddServerToLocationCommand struct {
	Host     string `json:"host"`
	Location int64  `json:"location"`
}

func NewAddServerToLocationCommand(host string, location int64) *AddServerToLocationCommand {
	return &AddServerToLocationCommand{
		Host:     host,
		Location: location,
	}
}

func (c *AddServerToLocationCommand) CommandName() string {
	return "add"
}

func (c *AddServerToLocationCommand) Apply(server *raft.Server) (interface{}, error) {
	config := server.Context().(*ClusterConfiguration)
	config.AddRingLocationToServer(c.Host, c.Location)
	return nil, nil
}

type RemoveServerFromLocationCommand struct {
	Host     string `json:"host"`
	Location int64  `json:"location"`
}

func NewRemoveServerFromLocationCommand(host string, location int64) *RemoveServerFromLocationCommand {
	return &RemoveServerFromLocationCommand{
		Host:     host,
		Location: location,
	}
}

func (c *RemoveServerFromLocationCommand) CommandName() string {
	return "remove"
}

func (c *RemoveServerFromLocationCommand) Apply(server *raft.Server) (interface{}, error) {
	config := server.Context().(*ClusterConfiguration)
	config.RemoveRingLocationFromServer(c.Host, c.Location)
	return nil, nil
}

type AddApiKeyCommand struct {
	Database string     `json:"database"`
	ApiKey   string     `json:"api_key"`
	KeyType  ApiKeyType `json:"key_type"`
}

func NewAddApikeyCommand(db, key string, keyType ApiKeyType) *AddApiKeyCommand {
	return &AddApiKeyCommand{
		Database: db,
		ApiKey:   key,
		KeyType:  keyType,
	}
}

func (c *AddApiKeyCommand) CommandName() string {
	return "add_key"
}

func (c *AddApiKeyCommand) Apply(server *raft.Server) (interface{}, error) {
	config := server.Context().(*ClusterConfiguration)
	config.AddApiKey(c.Database, c.ApiKey, c.KeyType)
	return nil, nil
}

type RemoveApiKeyCommand struct {
	Database string `json:"database"`
	ApiKey   string `json:"api_key"`
}

func NewRemoveApiKeyCommand(db, key string) *RemoveApiKeyCommand {
	return &RemoveApiKeyCommand{
		Database: db,
		ApiKey:   key,
	}
}

func (c *RemoveApiKeyCommand) CommandName() string {
	return "remove_key"
}

func (c *RemoveApiKeyCommand) Apply(server *raft.Server) (interface{}, error) {
	config := server.Context().(*ClusterConfiguration)
	config.DeleteApiKey(c.Database, c.ApiKey)
	return nil, nil
}

type NextDatabaseIdCommand struct {
	LastId int `json:"last_id"`
}

func NewNextDatabaseIdCommand(lastId int) *NextDatabaseIdCommand {
	return &NextDatabaseIdCommand{lastId}
}

func (c *NextDatabaseIdCommand) CommandName() string {
	return "next_db"
}

func (c *NextDatabaseIdCommand) Apply(server *raft.Server) (interface{}, error) {
	config := server.Context().(*ClusterConfiguration)
	id := config.NextDatabaseId()
	return id, nil
}
