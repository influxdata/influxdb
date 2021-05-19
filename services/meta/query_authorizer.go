package meta

import (
	"fmt"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

// QueryAuthorizer determines whether a user is authorized to execute a given query.
type QueryAuthorizer struct {
	Client *Client
}

// NewQueryAuthorizer returns a new instance of QueryAuthorizer.
func NewQueryAuthorizer(c *Client) *QueryAuthorizer {
	return &QueryAuthorizer{
		Client: c,
	}
}

// AuthorizeQuery authorizes u to execute q on database.
// Database can be "" for queries that do not require a database.
// If no user is provided it will return an error unless the query's first statement is to create
// a root user.
func (a *QueryAuthorizer) AuthorizeQuery(u User, q *influxql.Query, database string) (query.FineAuthorizer, error) {
	// Special case if no users exist.
	if n := a.Client.UserCount(); n == 0 {
		// Ensure there is at least one statement.
		if len(q.Statements) > 0 {
			// First statement in the query must create a user with admin privilege.
			cu, ok := q.Statements[0].(*influxql.CreateUserStatement)
			if ok && cu.Admin {
				return query.OpenAuthorizer, nil
			}
		}
		return nil, &ErrAuthorize{
			Query:    q,
			Database: database,
			Message:  "create admin user first or disable authentication",
		}
	}

	if u == nil {
		return nil, &ErrAuthorize{
			Query:    q,
			Database: database,
			Message:  "no user provided",
		}
	}

	// There is only one OSS implementation of the User interface, and the OSS QueryAuthorizer only works
	// with the OSS UserInfo. There is a similar tight coupling between the Enterprise QueryAuthorizer and
	// Enterprise UserInfo in closed-source code.
	switch user := u.(type) {
	case *UserInfo:
		// Admin privilege allows the user to execute all statements.
		if user.Admin {
			return query.OpenAuthorizer, nil
		}

		// Check each statement in the query.
		for _, stmt := range q.Statements {
			// Get the privileges required to execute the statement.
			privs, err := stmt.RequiredPrivileges()
			if err != nil {
				return nil, err
			}

			// Make sure the user has the privileges required to execute
			// each statement.
			for _, p := range privs {
				if p.Admin {
					// Admin privilege already checked so statement requiring admin
					// privilege cannot be run.
					return nil, &ErrAuthorize{
						Query:    q,
						User:     user.Name,
						Database: database,
						Message:  fmt.Sprintf("statement '%s', requires admin privilege", stmt),
					}
				}

				// Use the db name specified by the statement or the db
				// name passed by the caller if one wasn't specified by
				// the statement.
				db := p.Name
				if db == "" {
					db = database
				}
				if !user.AuthorizeDatabase(p.Privilege, db) {
					return nil, &ErrAuthorize{
						Query:    q,
						User:     user.Name,
						Database: database,
						Message:  fmt.Sprintf("statement '%s', requires %s on %s", stmt, p.Privilege.String(), db),
					}
				}
			}
		}
		return query.OpenAuthorizer, nil
	default:
	}
	return nil, &ErrAuthorize{
		Query:    q,
		User:     u.ID(),
		Database: database,
		Message:  fmt.Sprintf("Invalid OSS user type %T", u),
	}
}

func (a *QueryAuthorizer) AuthorizeDatabase(u User, priv influxql.Privilege, database string) error {
	if u == nil {
		return &ErrAuthorize{
			Database: database,
			Message:  "no user provided",
		}
	}

	switch user := u.(type) {
	case *UserInfo:
		if !user.AuthorizeDatabase(priv, database) {
			return &ErrAuthorize{
				Database: database,
				Message:  fmt.Sprintf("user %q, requires %s for database %q", u.ID(), priv.String(), database),
			}
		}
		return nil
	default:
	}
	return &ErrAuthorize{
		Database: database,
		User:     u.ID(),
		Message:  fmt.Sprintf("Internal error - incorrect oss user type %T", u),
	}
}

// ErrAuthorize represents an authorization error.
type ErrAuthorize struct {
	Query    *influxql.Query
	User     string
	Database string
	Message  string
}

// Error returns the text of the error.
func (e ErrAuthorize) Error() string {
	if e.User == "" {
		return fmt.Sprint(e.Message)
	}
	return fmt.Sprintf("%s not authorized to execute %s", e.User, e.Message)
}
