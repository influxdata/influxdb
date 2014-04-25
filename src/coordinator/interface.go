package coordinator

import (
	"cluster"
	"common"
	"net"
	"protocol"
)

type Coordinator interface {
	// Assumption about the returned data:
	//   1. For any given time series, the points returned are in order
	//   2. If the query involves more than one time series, there is no
	//      guarantee on the order in whic they are returned
	//   3. Data is filtered, i.e. where clause should be assumed to hold true
	//      for all the data points that are returned
	//   4. The end of a time series is signaled by returning a series with no data points
	//   5. TODO: Aggregation on the nodes
	WriteSeriesData(user common.User, db string, series []*protocol.Series) error
	DropDatabase(user common.User, db string) error
	CreateDatabase(user common.User, db string, replicationFactor uint8) error
	ForceCompaction(user common.User) error
	ListDatabases(user common.User) ([]*cluster.Database, error)
	DeleteContinuousQuery(user common.User, db string, id uint32) error
	CreateContinuousQuery(user common.User, db string, query string) error
	ListContinuousQueries(user common.User, db string) ([]*protocol.Series, error)

	// v2 clustering, based on sharding instead of the circular hash ring
	RunQuery(user common.User, db, query string, seriesWriter SeriesWriter) error
}

type ClusterConsensus interface {
	CreateDatabase(name string, replicationFactor uint8) error
	DropDatabase(name string) error
	CreateContinuousQuery(db string, query string) error
	DeleteContinuousQuery(db string, id uint32) error
	SaveClusterAdminUser(u *cluster.ClusterAdmin) error
	SaveDbUser(user *cluster.DbUser) error
	ChangeDbUserPassword(db, username string, hash []byte) error
	ChangeDbUserPermissions(db, username, readPermissions, writePermissions string) error

	// an insert index of -1 will append to the end of the ring
	AddServer(server *cluster.ClusterServer, insertIndex int) error
	// only servers that are in a Potential state can be moved around in the ring
	MovePotentialServer(server *cluster.ClusterServer, insertIndex int) error
	/*
		Activate tells the cluster to start sending writes to this node.
		The node will also make requests to the other servers to backfill any
		  data they should have
		Once the new node updates it state to "Running" the other servers will
		  delete all of the data that they no longer have to keep from the ring
	*/
	ActivateServer(server *cluster.ClusterServer) error

	// Efficient method to have a potential server take the place of a running (or downed)
	// server. The replacement must have a state of "Potential" for this to work.
	ReplaceServer(oldServer *cluster.ClusterServer, replacement *cluster.ClusterServer) error

	AssignCoordinator(coordinator *CoordinatorImpl) error

	// When a cluster is turned on for the first time.
	CreateRootUser() error

	ForceLogCompaction() error
}

type RequestHandler interface {
	HandleRequest(request *protocol.Request, conn net.Conn) error
}
