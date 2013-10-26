package coordinator

import (
	. "checkers"
	. "common"
	"datastore"
	"encoding/json"
	"fmt"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"net/http"
	"os"
	"protocol"
	"strings"
	"testing"
	"time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type CoordinatorSuite struct{}

var _ = Suite(&CoordinatorSuite{})
var nextPortNum int
var nextDirNum int

const (
	SERVER_STARTUP_TIME = time.Second * 2 // new cluster will have to create the root user and encrypt the password which takes little over a sec
	REPLICATION_LAG     = time.Millisecond * 500
)

type DatastoreMock struct {
	datastore.Datastore
	Series *protocol.Series
}

func (self *DatastoreMock) WriteSeriesData(database string, series *protocol.Series) error {
	self.Series = series
	return nil
}

func stringToSeries(seriesString string, c *C) *protocol.Series {
	series := &protocol.Series{}
	err := json.Unmarshal([]byte(seriesString), &series)
	c.Assert(err, IsNil)
	return series
}

func nextPort() int {
	nextPortNum += 1
	// this is a hack for OSX boxes running spotify. It binds to 127.0.0.1:8099. net.Listen doesn't return an
	// error when listening to :8099. ugh.
	if 8090+nextPortNum == 8099 {
		nextPortNum += 2
	}
	return 8090 + nextPortNum
}

func nextDir() string {
	nextDirNum += 1
	return fmt.Sprintf("test_%d", nextDirNum)
}

func startAndVerifyCluster(count int, c *C) []*RaftServer {
	nextPortNum = 0
	servers := make([]*RaftServer, count, count)
	errs := make([]error, count, count)
	for i := 0; i < count; i++ {
		path := nextDir()
		port := nextPort()
		_, server := newConfigAndServer(path, port)
		servers[i] = server

		var err error
		if i == 0 {
			go func() {
				err = server.ListenAndServe([]string{}, false)
			}()
		} else {
			go func() {
				err = server.ListenAndServe([]string{fmt.Sprintf("http://localhost:%d", servers[0].port)}, true)
			}()
		}
		errs[i] = err
	}
	time.Sleep(SERVER_STARTUP_TIME)
	for _, err := range errs {
		c.Assert(err, Equals, nil)
	}
	return servers
}

func clean(servers []*RaftServer) {
	for _, server := range servers {
		server.Close()
	}
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	clearPath("")
}

func newConfigAndServer(path string, port int) (*ClusterConfiguration, *RaftServer) {
	fullPath := path
	if !strings.HasPrefix(fullPath, "/tmp") {
		fullPath = "/tmp/chronos_coordinator_test/" + path
	}
	os.MkdirAll(fullPath, 0744)
	config := NewClusterConfiguration()
	server := NewRaftServer(fullPath, "localhost", port, config)
	return config, server
}

func clearPath(path string) {
	os.RemoveAll("/tmp/chronos_coordinator_test/" + path)
}

func assertConfigContains(port int, contains string, isContained bool, c *C) {
	host := fmt.Sprintf("localhost:%d", port)
	resp, err1 := http.Get("http://" + host + "/cluster_config")
	c.Assert(err1, Equals, nil)
	defer resp.Body.Close()
	body, err2 := ioutil.ReadAll(resp.Body)
	c.Assert(err2, Equals, nil)
	c.Assert(strings.Contains(string(body), contains), Equals, isContained)
}

func (self *CoordinatorSuite) TestCanCreateCoordinatorWithNoSeed(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir := nextDir()
	port := nextPort()
	_, server := newConfigAndServer(logDir, port)
	defer server.Close()
	defer clearPath(logDir)
	var err error
	go func() {
		err = server.ListenAndServe([]string{}, false)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
}

func (self *CoordinatorSuite) TestCanCreateCoordinatorWithSeedThatIsNotRunning(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir := nextDir()
	port := nextPort()
	_, server := newConfigAndServer(logDir, port)
	defer clearPath(logDir)
	var err error
	go func() {
		err = server.ListenAndServe([]string{"localhost:8079"}, false)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
	server.Close()
}

func (self *CoordinatorSuite) TestCanRecoverCoordinatorWithData(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir := nextDir()
	port := nextPort()
	_, server := newConfigAndServer(logDir, port)
	defer clearPath(logDir)
	var err error
	go func() {
		err = server.ListenAndServe([]string{}, false)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
	server.CreateDatabase("db1")

	assertConfigContains(port, "db1", true, c)

	server.Close()
	time.Sleep(SERVER_STARTUP_TIME)

	_, server = newConfigAndServer(logDir, port)
	defer server.Close()
	err = nil
	go func() {
		err = server.ListenAndServe([]string{}, false)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
	assertConfigContains(port, "db1", true, c)
}

func (self *CoordinatorSuite) TestCanCreateCoordinatorsAndReplicate(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir1 := nextDir()
	port1 := nextPort()
	logDir2 := nextDir()
	port2 := nextPort()

	_, server := newConfigAndServer(logDir1, port1)
	defer server.Close()
	defer clearPath(logDir1)

	var err error
	go func() {
		err = server.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port2)}, false)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	_, server2 := newConfigAndServer(logDir2, port2)
	defer server2.Close()
	defer clearPath(logDir2)

	var err2 error
	go func() {
		err2 = server2.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port1)}, true)
	}()

	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err2, Equals, nil)
	c.Assert(err, Equals, nil)

	server.CreateDatabase("db2")
	time.Sleep(REPLICATION_LAG)
	assertConfigContains(port1, "db2", true, c)
	assertConfigContains(port2, "db2", true, c)
}

func (self *CoordinatorSuite) TestDoWriteOperationsFromNonLeaderServer(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir1 := nextDir()
	port1 := nextPort()
	logDir2 := nextDir()
	port2 := nextPort()

	_, server := newConfigAndServer(logDir1, port1)
	defer server.Close()
	defer clearPath(logDir1)

	var err error
	go func() {
		err = server.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port2)}, false)
	}()

	_, server2 := newConfigAndServer(logDir2, port2)
	defer server2.Close()
	defer clearPath(logDir2)

	var err2 error
	go func() {
		err2 = server2.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port1)}, true)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
	c.Assert(err2, Equals, nil)
	err = server2.CreateDatabase("db3")
	c.Assert(err, Equals, nil)
	time.Sleep(REPLICATION_LAG)
	assertConfigContains(port1, "db3", true, c)
	assertConfigContains(port2, "db3", true, c)
}

func (self *CoordinatorSuite) TestNewServerJoiningClusterWillPickUpData(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir := nextDir()
	// TODO: make the next port method actually check that the port is open. Skipping some here to make it actually work. ugh.
	port := nextPort() + 3
	logDir2 := nextDir()
	port2 := nextPort() + 3
	defer clearPath(logDir)
	defer clearPath(logDir2)

	_, server := newConfigAndServer(logDir, port)
	var err error
	go func() {
		err = server.ListenAndServe([]string{}, false)
	}()
	defer server.Close()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
	server.CreateDatabase("db4")

	assertConfigContains(port, "db4", true, c)

	_, server2 := newConfigAndServer(logDir2, port2)
	go func() {
		err = server2.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port)}, true)
	}()
	defer server2.Close()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)
	assertConfigContains(port2, "db4", true, c)
}

func (self *CoordinatorSuite) TestCanElectNewLeaderAndRecover(c *C) {
	servers := startAndVerifyCluster(3, c)
	defer clean(servers)

	err := servers[0].CreateDatabase("db5")
	c.Assert(err, Equals, nil)
	time.Sleep(REPLICATION_LAG)
	assertConfigContains(servers[0].port, "db5", true, c)
	assertConfigContains(servers[1].port, "db5", true, c)
	assertConfigContains(servers[2].port, "db5", true, c)

	leader, _ := servers[1].leaderConnectString()
	c.Assert(leader, Equals, fmt.Sprintf("http://localhost:%d", servers[0].port))

	// kill the leader
	servers[0].Close()
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	err = nil
	time.Sleep(SERVER_STARTUP_TIME)
	leader, _ = servers[1].leaderConnectString()
	c.Assert(leader, Not(Equals), fmt.Sprintf("http://localhost:%d", servers[0].port))
	err = servers[1].CreateDatabase("db6")
	c.Assert(err, Equals, nil)
	time.Sleep(REPLICATION_LAG)
	assertConfigContains(servers[1].port, "db6", true, c)
	assertConfigContains(servers[2].port, "db6", true, c)

	_, server := newConfigAndServer(servers[0].path, servers[0].port)
	defer server.Close()
	err = nil
	go func() {
		err = server.ListenAndServe([]string{fmt.Sprintf("localhost:%d", servers[1].port), fmt.Sprintf("localhost:%d", servers[2].port)}, false)
	}()
	time.Sleep(SERVER_STARTUP_TIME)
	c.Assert(err, Equals, nil)

	c.Assert(server.clusterConfig.databaseNames["db5"], Equals, true)
	c.Assert(server.clusterConfig.databaseNames["db6"], Equals, true)
	assertConfigContains(server.port, "db5", true, c)
	assertConfigContains(server.port, "db6", true, c)

	err = server.CreateDatabase("db7")
	c.Assert(err, Equals, nil)
	time.Sleep(REPLICATION_LAG)
	assertConfigContains(servers[0].port, "db7", true, c)
	assertConfigContains(servers[1].port, "db7", true, c)
	assertConfigContains(servers[2].port, "db7", true, c)
}

func (self *CoordinatorSuite) TestCanJoinAClusterWhenNotInitiallyPointedAtLeader(c *C) {
	defer http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	logDir1 := nextDir()
	port1 := nextPort()
	logDir2 := nextDir()
	port2 := nextPort()
	logDir3 := nextDir()
	port3 := nextPort()
	defer clearPath(logDir1)
	defer clearPath(logDir2)
	defer clearPath(logDir3)

	_, server := newConfigAndServer(logDir1, port1)

	var err error
	go func() {
		err = server.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port2)}, false)
	}()

	_, server2 := newConfigAndServer(logDir2, port2)
	defer server2.Close()

	var err2 error
	go func() {
		err2 = server2.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port1), fmt.Sprintf("localhost:%d", port3)}, true)
	}()

	_, server3 := newConfigAndServer(logDir3, port3)
	defer server3.Close()

	var err3 error
	go func() {
		err3 = server3.ListenAndServe([]string{fmt.Sprintf("localhost:%d", port2)}, true)
	}()
	time.Sleep(SERVER_STARTUP_TIME)

	c.Assert(err, Equals, nil)
	c.Assert(err2, Equals, nil)
	c.Assert(err3, Equals, nil)
	leader, _ := server2.leaderConnectString()
	c.Assert(leader, Equals, fmt.Sprintf("http://localhost:%d", port1))

	err = server.CreateDatabase("db8")
	c.Assert(err, Equals, nil)
	time.Sleep(REPLICATION_LAG)
	assertConfigContains(port1, "db8", true, c)
	assertConfigContains(port2, "db8", true, c)
	assertConfigContains(port3, "db8", true, c)
}

func (self *UserSuite) BenchmarkHashing(c *C) {
	for i := 0; i < c.N; i++ {
		pwd := fmt.Sprintf("password%d", i)
		_, err := hashPassword(pwd)
		c.Assert(err, IsNil)
	}
}

func (self *CoordinatorSuite) TestAutomaitcDbCreations(c *C) {
	servers := startAndVerifyCluster(1, c)
	defer clean(servers)

	coordinator := NewCoordinatorImpl(nil, servers[0], servers[0].clusterConfig)

	time.Sleep(REPLICATION_LAG)

	// Root user is created
	var root User
	var err error
	// we should have the root user
	root, err = coordinator.AuthenticateClusterAdmin("root", "root")
	c.Assert(err, IsNil)
	c.Assert(root.IsClusterAdmin(), Equals, true)

	// can create db users
	c.Assert(coordinator.CreateDbUser(root, "db1", "db_user"), IsNil)
	c.Assert(coordinator.ChangeDbUserPassword(root, "db1", "db_user", "pass"), Equals, nil)

	// the db should be in the index now
	dbs, err := coordinator.ListDatabases(root)
	c.Assert(err, IsNil)
	c.Assert(dbs, DeepEquals, []string{"db1"})

	// if the db is dropped it should remove the users as well
	c.Assert(coordinator.DropDatabase(root, "db1"), IsNil)
	_, err = coordinator.AuthenticateDbUser("db1", "db_user", "pass")
	c.Assert(err, ErrorMatches, ".*Invalid.*")
}

func (self *CoordinatorSuite) TestAdminOperations(c *C) {
	servers := startAndVerifyCluster(1, c)
	defer clean(servers)

	coordinator := NewCoordinatorImpl(nil, servers[0], servers[0].clusterConfig)

	time.Sleep(REPLICATION_LAG)

	// Root user is created
	var root User
	var err error
	// we should have the root user
	root, err = coordinator.AuthenticateClusterAdmin("root", "root")
	c.Assert(err, IsNil)
	c.Assert(root.IsClusterAdmin(), Equals, true)

	// Can change it's own password
	c.Assert(coordinator.ChangeClusterAdminPassword(root, "root", "password"), Equals, nil)
	root, err = coordinator.AuthenticateClusterAdmin("root", "password")
	c.Assert(err, IsNil)
	c.Assert(root.IsClusterAdmin(), Equals, true)

	// Can create other cluster admin
	c.Assert(coordinator.CreateClusterAdminUser(root, "another_cluster_admin"), IsNil)
	c.Assert(coordinator.ChangeClusterAdminPassword(root, "another_cluster_admin", "pass"), IsNil)
	u, err := coordinator.AuthenticateClusterAdmin("another_cluster_admin", "pass")
	c.Assert(err, IsNil)
	c.Assert(u.IsClusterAdmin(), Equals, true)

	// can get other cluster admin
	admins, err := coordinator.ListClusterAdmins(root)
	c.Assert(err, IsNil)
	c.Assert(admins, DeepEquals, []string{"root", "another_cluster_admin"})

	// can create db users
	c.Assert(coordinator.CreateDbUser(root, "db1", "db_user"), IsNil)
	c.Assert(coordinator.ChangeDbUserPassword(root, "db1", "db_user", "db_pass"), IsNil)
	u, err = coordinator.AuthenticateDbUser("db1", "db_user", "db_pass")
	c.Assert(err, IsNil)
	c.Assert(u.IsClusterAdmin(), Equals, false)
	c.Assert(u.IsDbAdmin("db1"), Equals, false)

	// can make db users db admins
	c.Assert(coordinator.SetDbAdmin(root, "db1", "db_user", true), IsNil)
	u, err = coordinator.AuthenticateDbUser("db1", "db_user", "db_pass")
	c.Assert(err, IsNil)
	c.Assert(u.IsDbAdmin("db1"), Equals, true)

	// can list db users
	dbUsers, err := coordinator.ListDbUsers(root, "db1")
	c.Assert(err, IsNil)
	c.Assert(dbUsers, DeepEquals, []string{"db_user"})

	// can delete cluster admins and db users
	c.Assert(coordinator.DeleteDbUser(root, "db1", "db_user"), IsNil)
	c.Assert(coordinator.DeleteClusterAdminUser(root, "another_cluster_admin"), IsNil)
}

func (self *CoordinatorSuite) TestDbAdminOperations(c *C) {
	servers := startAndVerifyCluster(1, c)
	defer clean(servers)

	coordinator := NewCoordinatorImpl(nil, servers[0], servers[0].clusterConfig)

	time.Sleep(REPLICATION_LAG)

	// create a db user
	root, err := coordinator.AuthenticateClusterAdmin("root", "root")
	c.Assert(err, IsNil)
	c.Assert(root.IsClusterAdmin(), Equals, true)
	c.Assert(coordinator.CreateDbUser(root, "db1", "db_user"), IsNil)
	c.Assert(coordinator.ChangeDbUserPassword(root, "db1", "db_user", "db_pass"), IsNil)
	c.Assert(coordinator.SetDbAdmin(root, "db1", "db_user", true), IsNil)
	dbUser, err := coordinator.AuthenticateDbUser("db1", "db_user", "db_pass")
	c.Assert(err, IsNil)

	// Cannot create or delete other cluster admin
	c.Assert(coordinator.CreateClusterAdminUser(dbUser, "another_cluster_admin"), NotNil)
	c.Assert(coordinator.DeleteClusterAdminUser(dbUser, "root"), NotNil)

	// cannot get cluster admin
	_, err = coordinator.ListClusterAdmins(dbUser)
	c.Assert(err, NotNil)

	// can create db users
	c.Assert(coordinator.CreateDbUser(dbUser, "db1", "db_user2"), IsNil)
	c.Assert(coordinator.ChangeDbUserPassword(dbUser, "db1", "db_user2", "db_pass"), IsNil)
	u, err := coordinator.AuthenticateDbUser("db1", "db_user2", "db_pass")
	c.Assert(err, IsNil)
	c.Assert(u.IsClusterAdmin(), Equals, false)
	c.Assert(u.IsDbAdmin("db1"), Equals, false)

	// can get db users
	admins, err := coordinator.ListDbUsers(dbUser, "db1")
	c.Assert(err, IsNil)
	c.Assert(admins, DeepEquals, []string{"db_user", "db_user2"})

	// cannot create db users for a different db
	c.Assert(coordinator.CreateDbUser(dbUser, "db2", "db_user"), NotNil)

	// cannot get db users for a different db
	_, err = coordinator.ListDbUsers(dbUser, "db2")
	c.Assert(err, NotNil)

	// can make db users db admins
	c.Assert(coordinator.SetDbAdmin(dbUser, "db1", "db_user2", true), IsNil)
	u, err = coordinator.AuthenticateDbUser("db1", "db_user2", "db_pass")
	c.Assert(err, IsNil)
	c.Assert(u.IsDbAdmin("db1"), Equals, true)

	// can delete db users
	c.Assert(coordinator.DeleteDbUser(dbUser, "db1", "db_user2"), IsNil)
}

func (self *CoordinatorSuite) TestDbUserOperations(c *C) {
	servers := startAndVerifyCluster(1, c)
	defer clean(servers)

	coordinator := NewCoordinatorImpl(nil, servers[0], servers[0].clusterConfig)

	time.Sleep(REPLICATION_LAG)

	// create a db user
	root, err := coordinator.AuthenticateClusterAdmin("root", "root")
	c.Assert(err, IsNil)
	c.Assert(root.IsClusterAdmin(), Equals, true)
	c.Assert(coordinator.CreateDbUser(root, "db1", "db_user"), IsNil)
	c.Assert(coordinator.ChangeDbUserPassword(root, "db1", "db_user", "db_pass"), IsNil)
	dbUser, err := coordinator.AuthenticateDbUser("db1", "db_user", "db_pass")
	c.Assert(err, IsNil)

	// Cannot create other cluster admin
	c.Assert(coordinator.CreateClusterAdminUser(dbUser, "another_cluster_admin"), NotNil)

	// can create db users
	c.Assert(coordinator.CreateDbUser(dbUser, "db1", "db_user2"), NotNil)

	// cannot make itself an admin
	c.Assert(coordinator.SetDbAdmin(dbUser, "db1", "db_user", true), NotNil)

	// cannot create db users for a different db
	c.Assert(coordinator.CreateDbUser(dbUser, "db2", "db_user"), NotNil)

	// can change its own password
	c.Assert(coordinator.ChangeDbUserPassword(dbUser, "db1", "db_user", "new_password"), IsNil)
	dbUser, err = coordinator.AuthenticateDbUser("db1", "db_user", "db_pass")
	c.Assert(err, NotNil)
	dbUser, err = coordinator.AuthenticateDbUser("db1", "db_user", "new_password")
	c.Assert(err, IsNil)
}

func (self *CoordinatorSuite) TestUserDataReplication(c *C) {
	servers := startAndVerifyCluster(3, c)
	defer clean(servers)

	coordinators := make([]*CoordinatorImpl, 0, len(servers))
	for _, server := range servers {
		coordinators = append(coordinators, NewCoordinatorImpl(nil, server, server.clusterConfig))
	}

	// root must exist on all three nodes
	var root User
	var err error
	for _, coordinator := range coordinators {
		root, err = coordinator.AuthenticateClusterAdmin("root", "root")
		c.Assert(err, IsNil)
		c.Assert(root.IsClusterAdmin(), Equals, true)
	}

	c.Assert(coordinators[0].CreateClusterAdminUser(root, "admin"), IsNil)
	c.Assert(coordinators[0].ChangeClusterAdminPassword(root, "admin", "admin"), IsNil)
	time.Sleep(REPLICATION_LAG)
	for _, coordinator := range coordinators {
		u, err := coordinator.AuthenticateClusterAdmin("admin", "admin")
		c.Assert(err, IsNil)
		c.Assert(u.IsClusterAdmin(), Equals, true)
	}
}

func (self *CoordinatorSuite) createDatabases(servers []*RaftServer, c *C) {
	err := servers[0].CreateDatabase("db1")
	c.Assert(err, IsNil)
	err = servers[1].CreateDatabase("db2")
	c.Assert(err, IsNil)
	err = servers[2].CreateDatabase("db3")
	c.Assert(err, IsNil)
}

func (self *CoordinatorSuite) TestCanCreateDatabaseWithName(c *C) {
	servers := startAndVerifyCluster(3, c)
	defer clean(servers)

	self.createDatabases(servers, c)

	time.Sleep(REPLICATION_LAG)

	for i := 0; i < 3; i++ {
		databases := servers[i].clusterConfig.GetDatabases()
		c.Assert(databases, DeepEquals, []string{"db1", "db2", "db3"})
	}

	err := servers[0].CreateDatabase("db3")
	c.Assert(err, ErrorMatches, ".*db3 exists.*")
	err = servers[2].CreateDatabase("db3")
	c.Assert(err, ErrorMatches, ".*db3 exists.*")
}

func (self *CoordinatorSuite) TestCanDropDatabaseWithName(c *C) {
	servers := startAndVerifyCluster(3, c)
	defer clean(servers)

	self.createDatabases(servers, c)

	err := servers[0].DropDatabase("db1")
	c.Assert(err, IsNil)
	err = servers[1].DropDatabase("db2")
	c.Assert(err, IsNil)
	err = servers[2].DropDatabase("db3")
	c.Assert(err, IsNil)

	time.Sleep(REPLICATION_LAG)

	for i := 0; i < 3; i++ {
		databases := servers[i].clusterConfig.GetDatabases()
		c.Assert(databases, HasLen, 0)
	}

	err = servers[0].DropDatabase("db3")
	c.Assert(err, ErrorMatches, ".*db3 doesn't exist.*")
	err = servers[2].DropDatabase("db3")
	c.Assert(err, ErrorMatches, ".*db3 doesn't exist.*")
}

func (self *CoordinatorSuite) TestWillSetTimestampsAndSequenceNumbersForPointsWithout(c *C) {
	datastoreMock := &DatastoreMock{}
	coordinator := NewCoordinatorImpl(datastoreMock, nil, nil)
	mock := `
  {
    "points": [
      {
        "values": [
          {
            "int64_value": 3
          }
        ],
        "sequence_number": 1,
        "timestamp": 23423
      }
    ],
    "name": "foo",
    "fields": ["value"]
  }`
	series := stringToSeries(mock, c)
	user := &MockUser{}
	coordinator.WriteSeriesData(user, "foo", series)
	c.Assert(datastoreMock.Series, DeepEquals, series)
	mock = `{
    "points": [{"values": [{"int64_value": 3}]}],
    "name": "foo",
    "fields": ["value"]
  }`
	series = stringToSeries(mock, c)
	beforeTime := CurrentTime()
	coordinator.WriteSeriesData(user, "foo", series)
	afterTime := CurrentTime()
	c.Assert(datastoreMock.Series, Not(DeepEquals), stringToSeries(mock, c))
	c.Assert(*datastoreMock.Series.Points[0].SequenceNumber, Equals, uint32(1))
	t := *datastoreMock.Series.Points[0].Timestamp
	c.Assert(t, InRange, beforeTime, afterTime)
}

func (self *CoordinatorSuite) TestCheckReadAccess(c *C) {
	datastoreMock := &DatastoreMock{}
	coordinator := NewCoordinatorImpl(datastoreMock, nil, nil)
	mock := `{
    "points": [
      {
        "values": [
          {
            "int64_value": 3
          }
        ],
        "sequence_number": 1,
        "timestamp": 23423
      }
    ],
    "name": "foo",
    "fields": ["value"]
  }`
	series := stringToSeries(mock, c)
	user := &MockUser{
		dbCannotWrite: map[string]bool{"foo": true},
	}
	err := coordinator.WriteSeriesData(user, "foo", series)
	c.Assert(err, ErrorMatches, ".*Insufficient permission.*")
	c.Assert(datastoreMock.Series, IsNil)
}
