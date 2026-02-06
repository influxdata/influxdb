package meta

import (
	"testing"

	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/require"
)

// newTestClient returns a *Client with the given users pre-populated,
// bypassing bcrypt and disk I/O.
func newTestClient(users []UserInfo) *Client {
	c := NewClient(NewConfig(), nil)
	c.cacheData.Users = users
	return c
}

func TestQueryAuthorizer_AuthorizeQuery_ShowStats(t *testing.T) {
	adminUser := UserInfo{Name: "admin", Admin: true}
	regularUser := UserInfo{Name: "user1", Admin: false}

	q, err := influxql.ParseQuery("SHOW STATS")
	require.NoError(t, err)

	t.Run("admin user is authorized", func(t *testing.T) {
		a := NewQueryAuthorizer(newTestClient([]UserInfo{adminUser, regularUser}))
		require.NoError(t, a.AuthorizeQuery(&adminUser, q, ""))
	})

	t.Run("non-admin user is denied", func(t *testing.T) {
		a := NewQueryAuthorizer(newTestClient([]UserInfo{adminUser, regularUser}))
		err := a.AuthorizeQuery(&regularUser, q, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires admin privilege")
	})

	t.Run("nil user is denied", func(t *testing.T) {
		a := NewQueryAuthorizer(newTestClient([]UserInfo{adminUser}))
		err := a.AuthorizeQuery(nil, q, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "no user provided")
	})

	t.Run("SHOW STATS FOR module also requires admin", func(t *testing.T) {
		qm, err := influxql.ParseQuery("SHOW STATS FOR 'userquerybytes'")
		require.NoError(t, err)

		a := NewQueryAuthorizer(newTestClient([]UserInfo{adminUser, regularUser}))
		require.NoError(t, a.AuthorizeQuery(&adminUser, qm, ""))

		err = a.AuthorizeQuery(&regularUser, qm, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires admin privilege")
	})
}
