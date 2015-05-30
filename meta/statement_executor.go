package meta

import (
	"fmt"

	"github.com/influxdb/influxdb/influxql"
)

// StatementExecutor translates InfluxQL queries to meta store methods.
type StatementExecutor struct {
	Store interface {
		Nodes() ([]NodeInfo, error)

		Database(name string) (*DatabaseInfo, error)
		Databases() ([]DatabaseInfo, error)
		CreateDatabase(name string) (*DatabaseInfo, error)
		DropDatabase(name string) error

		DefaultRetentionPolicy(database string) (*RetentionPolicyInfo, error)
		CreateRetentionPolicy(database string, rpi *RetentionPolicyInfo) (*RetentionPolicyInfo, error)
		UpdateRetentionPolicy(database, name string, rpu *RetentionPolicyUpdate) error
		SetDefaultRetentionPolicy(database, name string) error
		DropRetentionPolicy(database, name string) error

		Users() ([]UserInfo, error)
		CreateUser(name, password string, admin bool) (*UserInfo, error)
		UpdateUser(name, password string) error
		DropUser(name string) error
		SetPrivilege(username, database string, p influxql.Privilege) error

		CreateContinuousQuery(database, name, query string) error
		DropContinuousQuery(database, name string) error
	}
}

// ExecuteStatement executes stmt against the meta store as user.
func (e *StatementExecutor) ExecuteStatement(stmt influxql.Statement, user *UserInfo) *influxql.Result {
	switch stmt := stmt.(type) {
	case *influxql.CreateDatabaseStatement:
		return e.executeCreateDatabaseStatement(stmt, user)
	case *influxql.DropDatabaseStatement:
		return e.executeDropDatabaseStatement(stmt, user)
	case *influxql.ShowDatabasesStatement:
		return e.executeShowDatabasesStatement(stmt, user)
	case *influxql.ShowServersStatement:
		return e.executeShowServersStatement(stmt, user)
	case *influxql.CreateUserStatement:
		return e.executeCreateUserStatement(stmt, user)
	case *influxql.SetPasswordUserStatement:
		return e.executeSetPasswordUserStatement(stmt, user)
	case *influxql.DropUserStatement:
		return e.executeDropUserStatement(stmt, user)
	case *influxql.ShowUsersStatement:
		return e.executeShowUsersStatement(stmt, user)
	case *influxql.GrantStatement:
		return e.executeGrantStatement(stmt, user)
	case *influxql.RevokeStatement:
		return e.executeRevokeStatement(stmt, user)
	case *influxql.CreateRetentionPolicyStatement:
		return e.executeCreateRetentionPolicyStatement(stmt, user)
	case *influxql.AlterRetentionPolicyStatement:
		return e.executeAlterRetentionPolicyStatement(stmt, user)
	case *influxql.DropRetentionPolicyStatement:
		return e.executeDropRetentionPolicyStatement(stmt, user)
	case *influxql.ShowRetentionPoliciesStatement:
		return e.executeShowRetentionPoliciesStatement(stmt, user)
	case *influxql.CreateContinuousQueryStatement:
		return e.executeCreateContinuousQueryStatement(stmt, user)
	case *influxql.DropContinuousQueryStatement:
		return e.executeDropContinuousQueryStatement(stmt, user)
	case *influxql.ShowContinuousQueriesStatement:
		return e.executeShowContinuousQueriesStatement(stmt, user)
	default:
		panic(fmt.Sprintf("unsupported statement type: %T", stmt))
	}
}

func (e *StatementExecutor) executeCreateDatabaseStatement(q *influxql.CreateDatabaseStatement, user *UserInfo) *influxql.Result {
	_, err := e.Store.CreateDatabase(q.Name)
	return &influxql.Result{Err: err}
}

func (e *StatementExecutor) executeDropDatabaseStatement(q *influxql.DropDatabaseStatement, user *UserInfo) *influxql.Result {
	return &influxql.Result{Err: e.Store.DropDatabase(q.Name)}
}

func (e *StatementExecutor) executeShowDatabasesStatement(q *influxql.ShowDatabasesStatement, user *UserInfo) *influxql.Result {
	dis, err := e.Store.Databases()
	if err != nil {
		return &influxql.Result{Err: err}
	}

	row := &influxql.Row{Name: "databases", Columns: []string{"name"}}
	for _, di := range dis {
		row.Values = append(row.Values, []interface{}{di.Name})
	}
	return &influxql.Result{Series: []*influxql.Row{row}}
}

func (e *StatementExecutor) executeShowServersStatement(q *influxql.ShowServersStatement, user *UserInfo) *influxql.Result {
	nis, err := e.Store.Nodes()
	if err != nil {
		return &influxql.Result{Err: err}
	}

	row := &influxql.Row{Columns: []string{"id", "url"}}
	for _, ni := range nis {
		row.Values = append(row.Values, []interface{}{ni.ID, "http://" + ni.Host})
	}
	return &influxql.Result{Series: []*influxql.Row{row}}
}

func (e *StatementExecutor) executeCreateUserStatement(q *influxql.CreateUserStatement, user *UserInfo) *influxql.Result {
	admin := false
	if q.Privilege != nil {
		admin = (*q.Privilege == influxql.AllPrivileges)
	}

	_, err := e.Store.CreateUser(q.Name, q.Password, admin)
	return &influxql.Result{Err: err}
}

func (e *StatementExecutor) executeSetPasswordUserStatement(q *influxql.SetPasswordUserStatement, user *UserInfo) *influxql.Result {
	return &influxql.Result{Err: e.Store.UpdateUser(q.Name, q.Password)}
}

func (e *StatementExecutor) executeDropUserStatement(q *influxql.DropUserStatement, user *UserInfo) *influxql.Result {
	return &influxql.Result{Err: e.Store.DropUser(q.Name)}
}

func (e *StatementExecutor) executeShowUsersStatement(q *influxql.ShowUsersStatement, user *UserInfo) *influxql.Result {
	uis, err := e.Store.Users()
	if err != nil {
		return &influxql.Result{Err: err}
	}

	row := &influxql.Row{Columns: []string{"user", "admin"}}
	for _, user := range uis {
		row.Values = append(row.Values, []interface{}{user.Name, user.Admin})
	}
	return &influxql.Result{Series: []*influxql.Row{row}}
}

func (e *StatementExecutor) executeGrantStatement(stmt *influxql.GrantStatement, user *UserInfo) *influxql.Result {
	return &influxql.Result{Err: e.Store.SetPrivilege(stmt.User, stmt.On, stmt.Privilege)}
}

func (e *StatementExecutor) executeRevokeStatement(stmt *influxql.RevokeStatement, user *UserInfo) *influxql.Result {
	return &influxql.Result{Err: e.Store.SetPrivilege(stmt.User, stmt.On, influxql.NoPrivileges)}
}

func (e *StatementExecutor) executeCreateRetentionPolicyStatement(stmt *influxql.CreateRetentionPolicyStatement, user *UserInfo) *influxql.Result {
	rpi := NewRetentionPolicyInfo(stmt.Name)
	rpi.Duration = stmt.Duration
	rpi.ReplicaN = stmt.Replication

	// Create new retention policy.
	_, err := e.Store.CreateRetentionPolicy(stmt.Database, rpi)
	if err != nil {
		return &influxql.Result{Err: err}
	}

	// If requested, set new policy as the default.
	if stmt.Default {
		err = e.Store.SetDefaultRetentionPolicy(stmt.Database, stmt.Name)
	}

	return &influxql.Result{Err: err}
}

func (e *StatementExecutor) executeAlterRetentionPolicyStatement(stmt *influxql.AlterRetentionPolicyStatement, user *UserInfo) *influxql.Result {
	rpu := &RetentionPolicyUpdate{
		Duration: stmt.Duration,
		ReplicaN: stmt.Replication,
	}

	// Update the retention policy.
	err := e.Store.UpdateRetentionPolicy(stmt.Database, stmt.Name, rpu)
	if err != nil {
		return &influxql.Result{Err: err}
	}

	// If requested, set as default retention policy.
	if stmt.Default {
		err = e.Store.SetDefaultRetentionPolicy(stmt.Database, stmt.Name)
	}

	return &influxql.Result{Err: err}
}

func (e *StatementExecutor) executeDropRetentionPolicyStatement(q *influxql.DropRetentionPolicyStatement, user *UserInfo) *influxql.Result {
	return &influxql.Result{Err: e.Store.DropRetentionPolicy(q.Database, q.Name)}
}

func (e *StatementExecutor) executeShowRetentionPoliciesStatement(q *influxql.ShowRetentionPoliciesStatement, user *UserInfo) *influxql.Result {
	di, err := e.Store.Database(q.Database)
	if err != nil {
		return &influxql.Result{Err: err}
	} else if di == nil {
		return &influxql.Result{Err: ErrDatabaseNotFound}
	}

	row := &influxql.Row{Columns: []string{"name", "duration", "replicaN", "default"}}
	for _, rpi := range di.RetentionPolicies {
		row.Values = append(row.Values, []interface{}{rpi.Name, rpi.Duration.String(), rpi.ReplicaN, di.DefaultRetentionPolicy == rpi.Name})
	}
	return &influxql.Result{Series: []*influxql.Row{row}}
}

func (e *StatementExecutor) executeCreateContinuousQueryStatement(q *influxql.CreateContinuousQueryStatement, user *UserInfo) *influxql.Result {
	return &influxql.Result{
		Err: e.Store.CreateContinuousQuery(q.Database, q.Name, q.Source.String()),
	}
}

func (e *StatementExecutor) executeDropContinuousQueryStatement(q *influxql.DropContinuousQueryStatement, user *UserInfo) *influxql.Result {
	return &influxql.Result{
		Err: e.Store.DropContinuousQuery(q.Database, q.Name),
	}
}

func (e *StatementExecutor) executeShowContinuousQueriesStatement(stmt *influxql.ShowContinuousQueriesStatement, user *UserInfo) *influxql.Result {
	dis, err := e.Store.Databases()
	if err != nil {
		return &influxql.Result{Err: err}
	}

	rows := []*influxql.Row{}
	for _, di := range dis {
		row := &influxql.Row{Columns: []string{"name", "query"}, Name: di.Name}
		for _, cqi := range di.ContinuousQueries {
			row.Values = append(row.Values, []interface{}{cqi.Name, cqi.Query})
		}
		rows = append(rows, row)
	}
	return &influxql.Result{Series: rows}
}
