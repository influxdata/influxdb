package main

import (
	"testing"

	"github.com/influxdata/influxdb"
	influxHTTP "github.com/influxdata/influxdb/http"
)

var userFindUsage = `
Usage:
  influx user find [flags]

Flags:
  -h, --help          help for find
  -i, --id string     The user ID
  -n, --name string   The user name
`
var userCreateUsage = `
Usage:
  influx user create [flags]

Flags:
  -h, --help          help for create
  -n, --name string   The user name (required)
`

var userDeleteUsage = `
Usage:
  influx user delete [flags]

Flags:
  -h, --help        help for delete
  -i, --id string   The user ID (required)
`

var userUpdateUsage = `
Usage:
  influx user update [flags]

Flags:
  -h, --help          help for update
  -i, --id string     The user ID (required)
  -n, --name string   The user name
`
var userResp1 = &influxHTTP.UserResponse{
	User: influxdb.User{
		ID:   oneID,
		Name: "user1",
	},
}

var userResp2 = &influxHTTP.UserResponse{
	User: influxdb.User{
		ID:   twoID,
		Name: "user2",
	},
}

func filterUserResp(filter *influxdb.UserFilter, src influxHTTP.UsersResponse) influxHTTP.UsersResponse {
	result := new(influxHTTP.UsersResponse)
	for _, u := range src.Users {
		ok := true
		if filter.ID != nil && u.ID != *filter.ID {
			ok = false
		}
		if filter.Name != nil && u.Name != *filter.Name {
			ok = false
		}
		if ok {
			result.Users = append(result.Users, u)
		}
	}
	return *result
}

func TestUserCreate(t *testing.T) {
	cases := []struct {
		name string
		args []string
		resp interface{}
		want string
	}{
		{
			name: "name required",
			args: []string{},
			want: `Error: required flag(s) "name" not set` + userCreateUsage + globalUsage + exit1,
		},
		{
			name: "user creation",
			args: []string{"-n", "user1"},
			resp: userResp1,
			want: `ID			Name
020f755c3c082000	user1
`,
		},
	}
	for _, c := range cases {
		m := map[string]methodResponse{
			"/api/v2/setup": methodResponse{
				"GET": setupResponse{},
			},
			"/api/v2/users": methodResponse{
				"POST": c.resp,
			},
		}
		args := append([]string{"user", "create"}, c.args...)
		run(t, m, args, c.name, c.want)
	}
}

func TestUserDelete(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "id required",
			args: []string{},
			want: `Error: required flag(s) "id" not set` + userDeleteUsage + globalUsage + exit1,
		},
		{
			name: "regular delete",
			args: []string{"-i", oneID.String()},
			want: `ID			Name	Deleted
020f755c3c082000	user1	true
`,
		},
	}
	for _, c := range cases {
		m := map[string]methodResponse{
			"/api/v2/setup": methodResponse{
				"GET": setupResponse{},
			},
			"/api/v2/users/" + oneID.String(): methodResponse{
				"DELETE": nil,
				"GET":    userResp1,
			},
		}
		args := append([]string{"user", "delete"}, c.args...)
		run(t, m, args, c.name, c.want)
	}
}

func TestUserUpdate(t *testing.T) {
	cases := []struct {
		name string
		args []string
		resp *influxHTTP.UserResponse
		want string
	}{
		{
			name: "id required",
			args: []string{},
			want: `Error: required flag(s) "id" not set` + userUpdateUsage + globalUsage + exit1,
		},
		{
			name: "bad id",
			args: []string{"-i", "badid"},
			want: `Error: <invalid> id must have a length of 16 bytes.` + userUpdateUsage + globalUsage + exit1,
		},
		{
			name: "user update",
			args: []string{"-i", oneID.String(), "-n", "userS"},
			resp: &influxHTTP.UserResponse{
				User: influxdb.User{
					ID:   oneID,
					Name: "userS",
				},
			},
			want: `ID			Name
020f755c3c082000	userS
`,
		},
	}
	for _, c := range cases {
		var targetID string
		if c.resp != nil {
			targetID = c.resp.ID.String()
		}
		m := map[string]methodResponse{
			"/api/v2/setup": methodResponse{
				"GET": setupResponse{},
			},
			"/api/v2/users/" + targetID: methodResponse{
				"PATCH": c.resp,
			},
		}
		args := append([]string{"user", "update"}, c.args...)
		run(t, m, args, c.name, c.want)
	}
}

func TestUsersFind(t *testing.T) {
	usersResp := influxHTTP.UsersResponse{
		Users: []*influxHTTP.UserResponse{
			userResp1, userResp2,
		},
	}
	cases := []struct {
		name string
		args []string
		resp interface{}
		want string
	}{
		{
			name: "empty result",
			args: []string{},
			resp: influxHTTP.UsersResponse{},
			want: `ID	Name
`,
		},
		{
			name: "mutiple results",
			args: []string{},
			resp: usersResp,
			want: `ID			Name
020f755c3c082000	user1
020f755c3c082001	user2
`,
		},
		{
			name: "id filter",
			args: []string{"-i", twoID.String()},
			resp: filterUserResp(&influxdb.UserFilter{
				ID: &twoID,
			}, usersResp),
			want: `ID			Name
020f755c3c082001	user2
`,
		},
		{
			name: "name filter",
			args: []string{"-n", userResp1.Name},
			resp: filterUserResp(&influxdb.UserFilter{
				Name: &userResp1.Name,
			}, usersResp),
			want: `ID			Name
020f755c3c082000	user1
`,
		},
		{
			name: "bad id filter",
			args: []string{"-i", "lalala"},
			resp: filterUserResp(&influxdb.UserFilter{
				Name: &userResp1.Name,
			}, usersResp),
			want: "Error: <invalid> id must have a length of 16 bytes." + userFindUsage + globalUsage + exit1,
		},
	}
	for _, c := range cases {
		m := map[string]methodResponse{
			"/api/v2/setup": methodResponse{
				"GET": setupResponse{},
			},
			"/api/v2/users": methodResponse{
				"GET": c.resp,
			},
		}
		args := append([]string{"user", "find"}, c.args...)
		run(t, m, args, c.name, c.want)
	}
}
