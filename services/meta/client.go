package meta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/services/meta/internal"

	"github.com/gogo/protobuf/proto"
)

const (
	// errSleep is the time to sleep after we've failed on every metaserver
	// before making another pass
	errSleep = 100 * time.Millisecond

	// maxRetries is the maximum number of attemps to make before returning
	// a failure to the caller
	maxRetries = 10
)

type Client struct {
	tls    bool
	logger *log.Logger

	mu          sync.RWMutex
	metaServers []string
	changed     chan struct{}
	closing     chan struct{}
	data        *Data

	executor *StatementExecutor
}

func NewClient(metaServers []string, tls bool) *Client {
	client := &Client{
		data:        &Data{},
		metaServers: metaServers,
		tls:         tls,
		logger:      log.New(os.Stderr, "[metaclient] ", log.LstdFlags),
	}
	client.executor = &StatementExecutor{Store: client}
	return client
}

func (c *Client) Open() error {
	c.changed = make(chan struct{})
	c.closing = make(chan struct{})
	c.data = c.retryUntilSnapshot(0)

	go c.pollForUpdates()

	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	close(c.closing)

	return nil
}

func (c *Client) ClusterID() (id uint64, err error) {
	return 0, nil
}

// Node returns a node by id.
func (c *Client) DataNode(id uint64) (*NodeInfo, error) {
	return nil, nil
}

func (c *Client) DataNodes() ([]NodeInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.data.DataNodes, nil
}

func (c *Client) DeleteDataNode(nodeID uint64) error {
	return nil
}

func (c *Client) MetaNodes() ([]NodeInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.data.MetaNodes, nil
}

func (c *Client) MetaNodeByAddr(addr string) *NodeInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, n := range c.data.MetaNodes {
		if n.Host == addr {
			return &n
		}
	}
	return nil
}

// Database returns info for the requested database.
func (c *Client) Database(name string) (*DatabaseInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, d := range c.data.Databases {
		if d.Name == name {
			return &d, nil
		}
	}

	return nil, influxdb.ErrDatabaseNotFound(name)
}

// Databases returns a list of all database infos.
func (c *Client) Databases() ([]DatabaseInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.data.Databases == nil {
		return []DatabaseInfo{}, nil
	}
	return c.data.Databases, nil
}

// CreateDatabase creates a database.
func (c *Client) CreateDatabase(name string, ifNotExists bool) (*DatabaseInfo, error) {
	cmd := &internal.CreateDatabaseCommand{
		Name:        proto.String(name),
		IfNotExists: proto.Bool(ifNotExists),
	}

	err := c.retryUntilExec(internal.Command_CreateDatabaseCommand, internal.E_CreateDatabaseCommand_Command, cmd)
	if err != nil {
		return nil, err
	}

	return c.Database(name)
}

// CreateDatabaseWithRetentionPolicy creates a database with the specified retention policy.
func (c *Client) CreateDatabaseWithRetentionPolicy(name string, ifNotExists bool, rpi *RetentionPolicyInfo) (*DatabaseInfo, error) {
	if rpi.Duration < MinRetentionPolicyDuration && rpi.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	if _, err := c.CreateDatabase(name, ifNotExists); err != nil {
		return nil, err
	}

	cmd := &internal.CreateRetentionPolicyCommand{
		Database:        proto.String(name),
		RetentionPolicy: rpi.marshal(),
		IfNotExists:     proto.Bool(false),
	}

	if err := c.retryUntilExec(internal.Command_CreateRetentionPolicyCommand, internal.E_CreateRetentionPolicyCommand_Command, cmd); err != nil {
		return nil, err
	}

	return c.Database(name)
}

// DropDatabase deletes a database.
func (c *Client) DropDatabase(name string) error {
	cmd := &internal.DropDatabaseCommand{
		Name: proto.String(name),
	}

	return c.retryUntilExec(internal.Command_DropDatabaseCommand, internal.E_DropDatabaseCommand_Command, cmd)
}

// CreateRetentionPolicy creates a retention policy on the specified database.
func (c *Client) CreateRetentionPolicy(database string, rpi *RetentionPolicyInfo, ifNotExists bool) (*RetentionPolicyInfo, error) {
	if rpi.Duration < MinRetentionPolicyDuration && rpi.Duration != 0 {
		return nil, ErrRetentionPolicyDurationTooLow
	}

	cmd := &internal.CreateRetentionPolicyCommand{
		Database:        proto.String(database),
		RetentionPolicy: rpi.marshal(),
		IfNotExists:     proto.Bool(ifNotExists),
	}

	if err := c.retryUntilExec(internal.Command_CreateRetentionPolicyCommand, internal.E_CreateRetentionPolicyCommand_Command, cmd); err != nil {
		return nil, err
	}

	return c.RetentionPolicy(database, rpi.Name)
}

// RetentionPolicy returns the requested retention policy info.
func (c *Client) RetentionPolicy(database, name string) (rpi *RetentionPolicyInfo, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db, err := c.Database(database)
	if err != nil {
		return nil, err
	}

	return db.RetentionPolicy(name), nil
}

// VisitRetentionPolicies executes the given function on all retention policies in all databases.
func (c *Client) VisitRetentionPolicies(f func(d DatabaseInfo, r RetentionPolicyInfo)) {

}

// DropRetentionPolicy drops a retention policy from a database.
func (c *Client) DropRetentionPolicy(database, name string, ifExists bool) error {
	cmd := &internal.DropRetentionPolicyCommand{
		Database: proto.String(database),
		Name:     proto.String(name),
		IfExists: proto.Bool(ifExists),
	}

	return c.retryUntilExec(internal.Command_DropRetentionPolicyCommand, internal.E_DropRetentionPolicyCommand_Command, cmd)
}

func (c *Client) SetDefaultRetentionPolicy(database, name string) error {
	return nil
}

func (c *Client) UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate) error {
	return nil
}

// IsLeader - should get rid of this
func (c *Client) IsLeader() bool {
	return false
}

// WaitForLeader - should get rid of this
func (c *Client) WaitForLeader(timeout time.Duration) error {
	return nil
}

func (c *Client) Users() (a []UserInfo, err error) {
	return nil, nil
}

func (c *Client) User(name string) (*UserInfo, error) {
	return nil, nil
}

func (c *Client) CreateUser(name, password string, admin bool) (*UserInfo, error) {
	return nil, nil
}

func (c *Client) UpdateUser(name, password string) error {
	return nil
}

func (c *Client) DropUser(name string) error {
	return nil
}

func (c *Client) SetPrivilege(username, database string, p influxql.Privilege) error {
	return nil
}

func (c *Client) SetAdminPrivilege(username string, admin bool) error {
	return nil
}

func (c *Client) UserPrivileges(username string) (map[string]influxql.Privilege, error) {
	return nil, nil
}

func (c *Client) UserPrivilege(username, database string) (*influxql.Privilege, error) {
	return nil, nil
}

func (c *Client) AdminUserExists() (bool, error) {
	return false, nil
}

func (c *Client) Authenticate(username, password string) (*UserInfo, error) {
	return nil, nil
}

func (c *Client) UserCount() (int, error) {
	return 0, nil
}

func (c *Client) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []ShardGroupInfo, err error) {
	return nil, nil
}

func (c *Client) CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) (*ShardGroupInfo, error) {
	return nil, nil
}

func (c *Client) DeleteShardGroup(database, policy string, id uint64) error {
	return nil
}

func (c *Client) PrecreateShardGroups(from, to time.Time) error {
	return nil
}

func (c *Client) ShardOwner(shardID uint64) (string, string, *ShardGroupInfo) {
	return "", "", nil
}

// JoinMetaServer will add the passed in tcpAddr to the raft peers and add a MetaNode to
// the metastore
func (c *Client) JoinMetaServer(httpAddr, tcpAddr string) error {
	node := &NodeInfo{
		Host:    httpAddr,
		TCPHost: tcpAddr,
	}
	b, err := json.Marshal(node)
	if err != nil {
		return err
	}

	currentServer := 0
	redirectServer := ""
	for {
		// get the server to try to join against
		var url string
		if redirectServer != "" {
			url = redirectServer
			redirectServer = ""
		} else {
			c.mu.RLock()

			if currentServer >= len(c.metaServers) {
				currentServer = 0
			}
			server := c.metaServers[currentServer]
			c.mu.RUnlock()

			url = c.url(server) + "/join"
		}

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(b))
		if err != nil {
			currentServer++
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusTemporaryRedirect {
			redirectServer = resp.Header.Get("Location")
			continue
		}

		return nil
	}
}

func (c *Client) CreateMetaNode(httpAddr, tcpAddr string) error {
	cmd := &internal.CreateMetaNodeCommand{
		HTTPAddr: proto.String(httpAddr),
		TCPAddr:  proto.String(tcpAddr),
	}

	return c.retryUntilExec(internal.Command_CreateMetaNodeCommand, internal.E_CreateMetaNodeCommand_Command, cmd)
}

func (c *Client) DeleteMetaNode(id uint64) error {
	cmd := &internal.DeleteMetaNodeCommand{
		ID: proto.Uint64(id),
	}

	return c.retryUntilExec(internal.Command_DeleteMetaNodeCommand, internal.E_DeleteMetaNodeCommand_Command, cmd)
}

func (c *Client) CreateContinuousQuery(database, name, query string) error {
	return nil
}

func (c *Client) DropContinuousQuery(database, name string) error {
	return nil
}

func (c *Client) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	return nil
}

func (c *Client) DropSubscription(database, rp, name string) error {
	return nil
}

func (c *Client) ExecuteStatement(stmt influxql.Statement) *influxql.Result {
	return c.executor.ExecuteStatement(stmt)
}

// WaitForDataChanged will return a channel that will get closed when
// the metastore data has changed
func (c *Client) WaitForDataChanged() chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.changed
}

func (c *Client) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (c *Client) index() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.data.Index
}

// retryUntilExec will attempt the command on each of the metaservers until it either succeeds or
// hits the max number of tries
func (c *Client) retryUntilExec(typ internal.Command_Type, desc *proto.ExtensionDesc, value interface{}) error {
	var err error
	var index uint64
	tries := 0
	currentServer := 0
	var redirectServer string

	for {
		// build the url to hit the redirect server or the next metaserver
		var url string
		if redirectServer != "" {
			url = redirectServer
			redirectServer = ""
		} else {
			c.mu.RLock()
			if currentServer >= len(c.metaServers) {
				currentServer = 0
			}
			server := c.metaServers[currentServer]
			c.mu.RUnlock()

			url = fmt.Sprintf("://%s/execute", server)
			if c.tls {
				url = "https" + url
			} else {
				url = "http" + url
			}
		}

		index, err = c.exec(url, typ, desc, value)
		tries++

		if err == nil {
			c.waitForIndex(index)
			return nil
		}

		if e, ok := err.(errRedirect); ok {
			redirectServer = e.host
			continue
		}

		time.Sleep(errSleep)
	}
}

func (c *Client) exec(url string, typ internal.Command_Type, desc *proto.ExtensionDesc, value interface{}) (index uint64, err error) {
	// Create command.
	cmd := &internal.Command{Type: &typ}
	if err := proto.SetExtension(cmd, desc, value); err != nil {
		panic(err)
	}

	b, err := proto.Marshal(cmd)
	if err != nil {
		return 0, err
	}

	resp, err := http.Post(url, "application/octet-stream", bytes.NewBuffer(b))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// read the response
	if resp.StatusCode == http.StatusTemporaryRedirect {
		return 0, errRedirect{host: resp.Header.Get("Location")}
	} else if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected result:\n\texp: %d\n\tgot: %d\n", http.StatusOK, resp.StatusCode)
	}

	res := &internal.Response{}

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	if err := proto.Unmarshal(b, res); err != nil {
		return 0, err
	}
	es := res.GetError()
	if es != "" {
		return 0, fmt.Errorf("exec err: %s", es)
	}

	return res.GetIndex(), nil
}

func (c *Client) waitForIndex(idx uint64) {
	for {
		c.mu.RLock()
		if c.data.Index >= idx {
			c.mu.RUnlock()
			return
		}
		ch := c.changed
		c.mu.RUnlock()
		<-ch
	}
}

func (c *Client) pollForUpdates() {
	for {
		data := c.retryUntilSnapshot(c.index())

		// update the data and notify of the change
		c.mu.Lock()
		idx := c.data.Index
		c.data = data
		if idx < data.Index {
			close(c.changed)
			c.changed = make(chan struct{})
		}
		c.mu.Unlock()
	}
}

func (c *Client) getSnapshot(server string, index uint64) (*Data, error) {
	resp, err := http.Get(c.url(server) + fmt.Sprintf("?index=%d", index))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	data := &Data{}
	if err := data.UnmarshalBinary(b); err != nil {
		return nil, err
	}

	return data, nil
}

func (c *Client) url(server string) string {
	url := fmt.Sprintf("://%s", server)

	if c.tls {
		url = "https" + url
	} else {
		url = "http" + url
	}

	return url
}

func (c *Client) retryUntilSnapshot(idx uint64) *Data {
	currentServer := 0
	for {
		// get the index to look from and the server to poll
		c.mu.RLock()

		if currentServer >= len(c.metaServers) {
			currentServer = 0
		}
		server := c.metaServers[currentServer]
		c.mu.RUnlock()

		data, err := c.getSnapshot(server, idx)

		if err == nil {
			return data
		}

		c.logger.Printf("failure getting snapshot from %s: %s", server, err.Error())
		time.Sleep(errSleep)

		currentServer++
	}
}

type errRedirect struct {
	host string
}

func (e errRedirect) Error() string {
	return fmt.Sprintf("redirect to %s", e.host)
}
