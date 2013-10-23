package coordinator

import (
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
	DistributeQuery(db string, query *parser.Query, yield func(*protocol.Series) error) error
	WriteSeriesData(db string, series *protocol.Series) error
	CreateDatabase(db, initialApiKey, requestingApiKey string) error
}

type UserManager interface {
	// Returns the user for the given db and that has the given
	// credentials, falling back to cluster admins
	AuthenticateDbUser(db, username, password string) (User, error)
	// Returns the cluster admin with the given credentials
	AuthenticateClusterAdmin(username, password string) (User, error)
	// Create a cluster admin user, it's an error if requester isn't a cluster admin
	CreateClusterAdminUser(request User, username string) error
	// Delete a cluster admin. Same restricutions as CreateClusterAdminUser
	DeleteClusterAdminUser(requester User, username string) error
	// Change cluster admin's password. It's an error if requester isn't a cluster admin
	ChangeClusterAdminPassword(requester User, username, password string) error
	// Create a db user, it's an error if requester isn't a db admin or cluster admin
	CreateDbUser(request User, db, username string) error
	// Delete a db user. Same restrictions apply as in CreateDbUser
	DeleteDbUser(requester User, db, username string) error
	// Change db user's password. It's an error if requester isn't a cluster admin or db admin
	ChangeDbUserPassword(requester User, db, username, password string) error
	// make user a db admin for 'db'. It's an error if the requester
	// isn't a db admin or cluster admin or if user isn't a db user
	// for the given db
	SetDbAdmin(requester User, db, username string, isAdmin bool) error
}
