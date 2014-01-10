package coordinator

import (
	"common"
	"net"
	"parser"
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
	DistributeQuery(user common.User, db string, query *parser.SelectQuery, localOnly bool, yield func(*protocol.Series) error) error
	WriteSeriesData(user common.User, db string, series *protocol.Series) error
	DeleteSeriesData(user common.User, db string, query *parser.DeleteQuery, localOnly bool) error
	DropDatabase(user common.User, db string) error
	DropSeries(user common.User, db, series string) error
	CreateDatabase(user common.User, db string, replicationFactor uint8) error
	ListDatabases(user common.User) ([]*Database, error)
	ListSeries(user common.User, database string) ([]*protocol.Series, error)
	ReplicateWrite(request *protocol.Request) error
	ReplicateDelete(request *protocol.Request) error
	ReplayReplication(request *protocol.Request, replicationFactor *uint8, owningServerId *uint32, lastSeenSequenceNumber *uint64)
	GetLastSequenceNumber(replicationFactor uint8, ownerServerId, originatingServerId uint32) (uint64, error)
}

type UserManager interface {
	// Returns the user for the given db and that has the given
	// credentials, falling back to cluster admins
	AuthenticateDbUser(db, username, password string) (common.User, error)
	// Returns the cluster admin with the given credentials
	AuthenticateClusterAdmin(username, password string) (common.User, error)
	// Create a cluster admin user, it's an error if requester isn't a cluster admin
	CreateClusterAdminUser(request common.User, username string) error
	// Delete a cluster admin. Same restrictions as CreateClusterAdminUser
	DeleteClusterAdminUser(requester common.User, username string) error
	// Change cluster admin's password. It's an error if requester isn't a cluster admin
	ChangeClusterAdminPassword(requester common.User, username, password string) error
	// list cluster admins. only a cluster admin can list the other cluster admins
	ListClusterAdmins(requester common.User) ([]string, error)
	// Create a db user, it's an error if requester isn't a db admin or cluster admin
	CreateDbUser(request common.User, db, username string) error
	// Delete a db user. Same restrictions apply as in CreateDbUser
	DeleteDbUser(requester common.User, db, username string) error
	// Change db user's password. It's an error if requester isn't a cluster admin or db admin
	ChangeDbUserPassword(requester common.User, db, username, password string) error
	// list cluster admins. only a cluster admin or the db admin can list the db users
	ListDbUsers(requester common.User, db string) ([]string, error)
	// make user a db admin for 'db'. It's an error if the requester
	// isn't a db admin or cluster admin or if user isn't a db user
	// for the given db
	SetDbAdmin(requester common.User, db, username string, isAdmin bool) error
}

type ClusterConsensus interface {
	CreateDatabase(name string, replicationFactor uint8) error
	DropDatabase(name string) error
	SaveClusterAdminUser(u *clusterAdmin) error
	SaveDbUser(user *dbUser) error
	ChangeDbUserPassword(db, username string, hash []byte) error

	// an insert index of -1 will append to the end of the ring
	AddServer(server *ClusterServer, insertIndex int) error
	// only servers that are in a Potential state can be moved around in the ring
	MovePotentialServer(server *ClusterServer, insertIndex int) error
	/*
		Activate tells the cluster to start sending writes to this node.
		The node will also make requests to the other servers to backfill any
		  data they should have
		Once the new node updates it state to "Running" the other servers will
		  delete all of the data that they no longer have to keep from the ring
	*/
	ActivateServer(server *ClusterServer) error
	// Efficient method to have a potential server take the place of a running (or downed)
	// server. The replacement must have a state of "Potential" for this to work.
	ReplaceServer(oldServer *ClusterServer, replacement *ClusterServer) error

	// When a cluster is turned on for the first time.
	CreateRootUser() error
}

type RequestHandler interface {
	HandleRequest(request *protocol.Request, conn net.Conn) error
}
