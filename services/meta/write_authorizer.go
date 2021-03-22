package meta

import (
	"fmt"

	"github.com/influxdata/influxql"
)

// WriteAuthorizer determines whether a user is authorized to write to a given database.
type WriteAuthorizer struct {
	Client *Client
}

// NewWriteAuthorizer returns a new instance of WriteAuthorizer.
func NewWriteAuthorizer(c *Client) *WriteAuthorizer {
	return &WriteAuthorizer{Client: c}
}

// AuthorizeWrite returns nil if the user has permission to write to the database.
func (a WriteAuthorizer) AuthorizeWrite(username, database string) error {
	u, err := a.Client.User(username)
	if err != nil || u == nil {
		return &ErrAuthorize{
			Database: database,
			Message:  fmt.Sprintf("%s not authorized to write to %s", username, database),
		}
	}
	// There is only one OSS implementation of the User interface, and the OSS WriteAuthorizer only works
	// with the OSS UserInfo. There is a similar tight coupling between the Enterprise WriteAuthorizer and
	// Enterprise UserInfo in closed-source code.
	switch user := u.(type) {
	case *UserInfo:
		if !user.AuthorizeDatabase(influxql.WritePrivilege, database) {
			return &ErrAuthorize{
				Database: database,
				Message:  fmt.Sprintf("%s not authorized to write to %s", username, database),
			}
		}
	default:
		return &ErrAuthorize{
			Database: database,
			Message:  fmt.Sprintf("Internal error - wrong type %T for oss user", u),
		}
	}
	return nil
}
