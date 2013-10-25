package coordinator

import (
	"common"
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
	DistributeQuery(user common.User, db string, query *parser.Query, yield func(*protocol.Series) error) error
	WriteSeriesData(user common.User, db string, series *protocol.Series) error
	DropDatabase(user common.User, db string) error
	CreateDatabase(user common.User, db string) error
	ListDatabases(user common.User) ([]string, error)
}

type UserManager interface {
	// Returns the user for the given db and that has the given
	// credentials, falling back to cluster admins
	AuthenticateDbUser(db, username, password string) (common.User, error)
	// Returns the cluster admin with the given credentials
	AuthenticateClusterAdmin(username, password string) (common.User, error)
	// Create a cluster admin user, it's an error if requester isn't a cluster admin
	CreateClusterAdminUser(request common.User, username string) error
	// Delete a cluster admin. Same restricutions as CreateClusterAdminUser
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
