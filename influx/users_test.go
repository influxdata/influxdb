package influx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/log"
)

func TestClient_userPermissions(t *testing.T) {
	t.Parallel()
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name       string
		showGrants []byte
		status     int
		args       args
		want       chronograf.Permissions
		wantErr    bool
	}{
		{
			name:       "Check all grants",
			showGrants: []byte(`{"results":[{"series":[{"columns":["database","privilege"],"values":[["mydb","ALL PRIVILEGES"]]}]}]}`),
			status:     http.StatusOK,
			args: args{
				ctx:  context.Background(),
				name: "docbrown",
			},
			want: chronograf.Permissions{
				chronograf.Permission{
					Scope:   "database",
					Name:    "mydb",
					Allowed: []string{"WRITE", "READ"},
				},
			},
		},
		{
			name:   "Permission Denied",
			status: http.StatusUnauthorized,
			args: args{
				ctx:  context.Background(),
				name: "docbrown",
			},
			wantErr: true,
		},
		{
			name:       "bad JSON",
			showGrants: []byte(`{"results":[{"series":"adffdadf"}]}`),
			status:     http.StatusOK,
			args: args{
				ctx:  context.Background(),
				name: "docbrown",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if path := r.URL.Path; path != "/query" {
				t.Error("Expected the path to contain `/query` but was", path)
			}
			rw.WriteHeader(tt.status)
			rw.Write(tt.showGrants)
		}))
		u, _ := url.Parse(ts.URL)
		c := &Client{
			URL:    u,
			Logger: log.New(log.DebugLevel),
		}
		defer ts.Close()

		got, err := c.userPermissions(tt.args.ctx, tt.args.name)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.userPermissions() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. Client.userPermissions() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestClient_Add(t *testing.T) {
	t.Parallel()
	type args struct {
		ctx context.Context
		u   *chronograf.User
	}
	tests := []struct {
		name        string
		args        args
		status      int
		want        *chronograf.User
		wantQueries []string
		wantErr     bool
	}{
		{
			name:   "Create User",
			status: http.StatusOK,
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:   "docbrown",
					Passwd: "Dont Need Roads",
				},
			},
			wantQueries: []string{
				`CREATE USER "docbrown" WITH PASSWORD 'Dont Need Roads'`,
				`SHOW USERS`,
				`SHOW GRANTS FOR "docbrown"`,
			},
			want: &chronograf.User{
				Name: "docbrown",
				Permissions: chronograf.Permissions{
					chronograf.Permission{
						Scope: chronograf.AllScope,
						Allowed: chronograf.Allowances{
							"ALL",
						},
					},
				},
			},
		},
		{
			name:   "Create User with permissions",
			status: http.StatusOK,
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:   "docbrown",
					Passwd: "Dont Need Roads",
					Permissions: chronograf.Permissions{
						chronograf.Permission{
							Scope: chronograf.AllScope,
							Allowed: chronograf.Allowances{
								"ALL",
							},
						},
					},
				},
			},
			wantQueries: []string{
				`CREATE USER "docbrown" WITH PASSWORD 'Dont Need Roads'`,
				`GRANT ALL PRIVILEGES TO "docbrown"`,
				`SHOW USERS`,
				`SHOW GRANTS FOR "docbrown"`,
			},
			want: &chronograf.User{
				Name: "docbrown",
				Permissions: chronograf.Permissions{
					chronograf.Permission{
						Scope: chronograf.AllScope,
						Allowed: chronograf.Allowances{
							"ALL",
						},
					},
				},
			},
		},
		{
			name:   "Permission Denied",
			status: http.StatusUnauthorized,
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:   "docbrown",
					Passwd: "Dont Need Roads",
				},
			},
			wantQueries: []string{`CREATE USER "docbrown" WITH PASSWORD 'Dont Need Roads'`},
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		queries := []string{}
		ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if path := r.URL.Path; path != "/query" {
				t.Error("Expected the path to contain `/query` but was", path)
			}
			queries = append(queries, r.URL.Query().Get("q"))
			rw.WriteHeader(tt.status)
			rw.Write([]byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",true],["reader",false]]}]}]}`))
		}))
		u, _ := url.Parse(ts.URL)
		c := &Client{
			URL:    u,
			Logger: log.New(log.DebugLevel),
		}
		defer ts.Close()
		got, err := c.Add(tt.args.ctx, tt.args.u)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if len(tt.wantQueries) != len(queries) {
			t.Errorf("%q. Client.Add() queries = %v, want %v", tt.name, queries, tt.wantQueries)
			continue
		}
		for i := range tt.wantQueries {
			if tt.wantQueries[i] != queries[i] {
				t.Errorf("%q. Client.Add() query = %v, want %v", tt.name, queries[i], tt.wantQueries[i])
			}
		}

		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. Client.Add() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestClient_Delete(t *testing.T) {
	type args struct {
		ctx context.Context
		u   *chronograf.User
	}
	tests := []struct {
		name     string
		status   int
		dropUser []byte
		args     args
		wantErr  bool
	}{
		{
			name:     "Drop User",
			dropUser: []byte(`{"results":[{"series":[{"columns":["database","privilege"],"values":[["mydb","ALL PRIVILEGES"]]}]}]}`),
			status:   http.StatusOK,
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "docbrown",
				},
			},
		},
		{
			name:     "No such user",
			dropUser: []byte(`{"results":[{"error":"user not found"}]}`),
			status:   http.StatusOK,
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "docbrown",
				},
			},
			wantErr: true,
		},
		{
			name:     "Bad InfluxQL",
			dropUser: []byte(`{"error":"error parsing query: found doody, expected ; at line 1, char 17"}`),
			status:   http.StatusBadRequest,
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "docbrown",
				},
			},
			wantErr: true,
		},
		{
			name:     "Bad JSON",
			dropUser: []byte(`{"results":[{"error":breakhere}]}`),
			status:   http.StatusOK,
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "docbrown",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if path := r.URL.Path; path != "/query" {
				t.Error("Expected the path to contain `/query` but was", path)
			}
			rw.WriteHeader(tt.status)
			rw.Write(tt.dropUser)
		}))
		u, _ := url.Parse(ts.URL)
		c := &Client{
			URL:    u,
			Logger: log.New(log.DebugLevel),
		}
		defer ts.Close()

		if err := c.Delete(tt.args.ctx, tt.args.u); (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.Delete() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestClient_Get(t *testing.T) {
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name         string
		args         args
		statusUsers  int
		showUsers    []byte
		statusGrants int
		showGrants   []byte
		want         *chronograf.User
		wantErr      bool
	}{
		{
			name:         "Get User",
			statusUsers:  http.StatusOK,
			showUsers:    []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",true],["reader",false]]}]}]}`),
			statusGrants: http.StatusOK,
			showGrants:   []byte(`{"results":[{"series":[{"columns":["database","privilege"],"values":[["mydb","ALL PRIVILEGES"]]}]}]}`),
			args: args{
				ctx:  context.Background(),
				name: "docbrown",
			},
			want: &chronograf.User{
				Name: "docbrown",
				Permissions: chronograf.Permissions{
					chronograf.Permission{
						Scope:   "all",
						Allowed: []string{"ALL"},
					},
					chronograf.Permission{
						Scope:   "database",
						Name:    "mydb",
						Allowed: []string{"WRITE", "READ"},
					},
				},
			},
		},
		{
			name:        "Fail show users",
			statusUsers: http.StatusBadRequest,
			showUsers:   []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",true],["reader",false]]}]}]}`),
			args: args{
				ctx:  context.Background(),
				name: "docbrown",
			},
			wantErr: true,
		},
		{
			name:         "Fail show grants",
			statusUsers:  http.StatusOK,
			showUsers:    []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",true],["reader",false]]}]}]}`),
			statusGrants: http.StatusBadRequest,
			showGrants:   []byte(`{"results":[{"series":[{"columns":["database","privilege"],"values":[["mydb","ALL PRIVILEGES"]]}]}]}`),
			args: args{
				ctx:  context.Background(),
				name: "docbrown",
			},
			wantErr: true,
		},
		{
			name:        "Fail no such user",
			statusUsers: http.StatusOK,
			showUsers:   []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true]]}]}]}`),
			args: args{
				ctx:  context.Background(),
				name: "docbrown",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if path := r.URL.Path; path != "/query" {
				t.Error("Expected the path to contain `/query` but was", path)
			}
			query := r.URL.Query().Get("q")
			if strings.Contains(query, "GRANTS") {
				rw.WriteHeader(tt.statusGrants)
				rw.Write(tt.showGrants)
			} else if strings.Contains(query, "USERS") {
				rw.WriteHeader(tt.statusUsers)
				rw.Write(tt.showUsers)
			}
		}))
		u, _ := url.Parse(ts.URL)
		c := &Client{
			URL:    u,
			Logger: log.New(log.DebugLevel),
		}
		defer ts.Close()
		got, err := c.Get(tt.args.ctx, chronograf.UserQuery{Name: &tt.args.name})
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. Client.Get() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestClient_grantPermission(t *testing.T) {
	type args struct {
		ctx      context.Context
		username string
		perm     chronograf.Permission
	}
	tests := []struct {
		name      string
		args      args
		status    int
		results   []byte
		wantQuery string
		wantErr   bool
	}{
		{
			name:    "simple grants",
			status:  http.StatusOK,
			results: []byte(`{"results":[]}`),
			args: args{
				ctx:      context.Background(),
				username: "docbrown",
				perm: chronograf.Permission{
					Scope:   "database",
					Name:    "mydb",
					Allowed: []string{"WRITE", "READ"},
				},
			},
			wantQuery: `GRANT ALL ON "mydb" TO "docbrown"`,
		},
		{
			name:    "bad grants",
			status:  http.StatusOK,
			results: []byte(`{"results":[]}`),
			args: args{
				ctx:      context.Background(),
				username: "docbrown",
				perm: chronograf.Permission{
					Scope:   "database",
					Name:    "mydb",
					Allowed: []string{"howdy"},
				},
			},
			wantQuery: ``,
		},
		{
			name:    "no grants",
			status:  http.StatusOK,
			results: []byte(`{"results":[]}`),
			args: args{
				ctx:      context.Background(),
				username: "docbrown",
				perm: chronograf.Permission{
					Scope:   "database",
					Name:    "mydb",
					Allowed: []string{},
				},
			},
			wantQuery: ``,
		},
	}
	for _, tt := range tests {
		query := ""
		ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if path := r.URL.Path; path != "/query" {
				t.Error("Expected the path to contain `/query` but was", path)
			}
			query = r.URL.Query().Get("q")
			rw.WriteHeader(tt.status)
			rw.Write(tt.results)
		}))
		u, _ := url.Parse(ts.URL)
		c := &Client{
			URL:    u,
			Logger: log.New(log.DebugLevel),
		}
		defer ts.Close()
		if err := c.grantPermission(tt.args.ctx, tt.args.username, tt.args.perm); (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.grantPermission() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
		if query != tt.wantQuery {
			t.Errorf("%q. Client.grantPermission() = %v, want %v", tt.name, query, tt.wantQuery)
		}
	}
}

func TestClient_revokePermission(t *testing.T) {
	type args struct {
		ctx      context.Context
		username string
		perm     chronograf.Permission
	}
	tests := []struct {
		name      string
		args      args
		status    int
		results   []byte
		wantQuery string
		wantErr   bool
	}{
		{
			name:    "simple revoke",
			status:  http.StatusOK,
			results: []byte(`{"results":[]}`),
			args: args{
				ctx:      context.Background(),
				username: "docbrown",
				perm: chronograf.Permission{
					Scope:   "database",
					Name:    "mydb",
					Allowed: []string{"WRITE", "READ"},
				},
			},
			wantQuery: `REVOKE ALL ON "mydb" FROM "docbrown"`,
		},
		{
			name:    "bad revoke",
			status:  http.StatusOK,
			results: []byte(`{"results":[]}`),
			args: args{
				ctx:      context.Background(),
				username: "docbrown",
				perm: chronograf.Permission{
					Scope:   "database",
					Name:    "mydb",
					Allowed: []string{"howdy"},
				},
			},
			wantQuery: ``,
		},
		{
			name:    "no permissions",
			status:  http.StatusOK,
			results: []byte(`{"results":[]}`),
			args: args{
				ctx:      context.Background(),
				username: "docbrown",
				perm: chronograf.Permission{
					Scope:   "database",
					Name:    "mydb",
					Allowed: []string{},
				},
			},
			wantQuery: `REVOKE ALL PRIVILEGES ON "mydb" FROM "docbrown"`,
		},
	}
	for _, tt := range tests {
		query := ""
		ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if path := r.URL.Path; path != "/query" {
				t.Error("Expected the path to contain `/query` but was", path)
			}
			query = r.URL.Query().Get("q")
			rw.WriteHeader(tt.status)
			rw.Write(tt.results)
		}))
		u, _ := url.Parse(ts.URL)
		c := &Client{
			URL:    u,
			Logger: log.New(log.DebugLevel),
		}
		defer ts.Close()
		if err := c.revokePermission(tt.args.ctx, tt.args.username, tt.args.perm); (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.revokePermission() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
		if query != tt.wantQuery {
			t.Errorf("%q. Client.revokePermission() = %v, want %v", tt.name, query, tt.wantQuery)
		}
	}
}

func TestClient_All(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name         string
		args         args
		statusUsers  int
		showUsers    []byte
		statusGrants int
		showGrants   []byte
		want         []chronograf.User
		wantErr      bool
	}{
		{
			name:         "All Users",
			statusUsers:  http.StatusOK,
			showUsers:    []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",true],["reader",false]]}]}]}`),
			statusGrants: http.StatusOK,
			showGrants:   []byte(`{"results":[{"series":[{"columns":["database","privilege"],"values":[["mydb","ALL PRIVILEGES"]]}]}]}`),
			args: args{
				ctx: context.Background(),
			},
			want: []chronograf.User{
				{
					Name: "admin",
					Permissions: chronograf.Permissions{
						chronograf.Permission{
							Scope:   "all",
							Allowed: []string{"ALL"},
						},
						chronograf.Permission{
							Scope:   "database",
							Name:    "mydb",
							Allowed: []string{"WRITE", "READ"},
						},
					},
				},
				{
					Name: "docbrown",
					Permissions: chronograf.Permissions{
						chronograf.Permission{
							Scope:   "all",
							Allowed: []string{"ALL"},
						},
						chronograf.Permission{
							Scope:   "database",
							Name:    "mydb",
							Allowed: []string{"WRITE", "READ"},
						},
					},
				},
				{
					Name: "reader",
					Permissions: chronograf.Permissions{
						chronograf.Permission{
							Scope:   "database",
							Name:    "mydb",
							Allowed: []string{"WRITE", "READ"},
						},
					},
				},
			},
		},
		{
			name:        "Unauthorized",
			statusUsers: http.StatusUnauthorized,
			showUsers:   []byte(`{}`),
			args: args{
				ctx: context.Background(),
			},
			wantErr: true,
		},
		{
			name:         "Permission error",
			statusUsers:  http.StatusOK,
			showUsers:    []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",true],["reader",false]]}]}]}`),
			statusGrants: http.StatusBadRequest,
			showGrants:   []byte(`{}`),
			args: args{
				ctx: context.Background(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if path := r.URL.Path; path != "/query" {
				t.Error("Expected the path to contain `/query` but was", path)
			}
			query := r.URL.Query().Get("q")
			if strings.Contains(query, "GRANTS") {
				rw.WriteHeader(tt.statusGrants)
				rw.Write(tt.showGrants)
			} else if strings.Contains(query, "USERS") {
				rw.WriteHeader(tt.statusUsers)
				rw.Write(tt.showUsers)
			}
		}))
		u, _ := url.Parse(ts.URL)
		c := &Client{
			URL:    u,
			Logger: log.New(log.DebugLevel),
		}
		defer ts.Close()
		got, err := c.All(tt.args.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. Client.All() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestClient_Update(t *testing.T) {
	type args struct {
		ctx context.Context
		u   *chronograf.User
	}
	tests := []struct {
		name           string
		statusUsers    int
		showUsers      []byte
		statusGrants   int
		showGrants     []byte
		statusRevoke   int
		revoke         []byte
		statusGrant    int
		grant          []byte
		statusPassword int
		password       []byte
		args           args
		want           []string
		wantErr        bool
	}{
		{
			name:           "Change Password",
			statusPassword: http.StatusOK,
			password:       []byte(`{"results":[]}`),
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:   "docbrown",
					Passwd: "hunter2",
				},
			},
			want: []string{
				`SET PASSWORD for "docbrown" = 'hunter2'`,
			},
		},
		{
			name:         "Grant all permissions",
			statusUsers:  http.StatusOK,
			showUsers:    []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",true],["reader",false]]}]}]}`),
			statusGrants: http.StatusOK,
			showGrants:   []byte(`{"results":[{"series":[{"columns":["database","privilege"],"values":[["mydb","ALL PRIVILEGES"]]}]}]}`),
			statusRevoke: http.StatusOK,
			revoke:       []byte(`{"results":[]}`),
			statusGrant:  http.StatusOK,
			grant:        []byte(`{"results":[]}`),
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "docbrown",
					Permissions: chronograf.Permissions{
						{
							Scope:   "all",
							Allowed: []string{"all"},
						},
						{
							Scope:   "database",
							Name:    "mydb",
							Allowed: []string{"WRITE", "READ"},
						},
					},
				},
			},
			want: []string{
				`SHOW USERS`,
				`SHOW GRANTS FOR "docbrown"`,
				`GRANT ALL PRIVILEGES TO "docbrown"`,
				`GRANT ALL ON "mydb" TO "docbrown"`,
			},
		},
		{
			name:         "Revoke all permissions",
			statusUsers:  http.StatusOK,
			showUsers:    []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",true],["reader",false]]}]}]}`),
			statusGrants: http.StatusOK,
			showGrants:   []byte(`{"results":[{"series":[{"columns":["database","privilege"],"values":[["mydb","ALL PRIVILEGES"]]}]}]}`),
			statusRevoke: http.StatusOK,
			revoke:       []byte(`{"results":[]}`),
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "docbrown",
				},
			},
			want: []string{
				`SHOW USERS`,
				`SHOW GRANTS FOR "docbrown"`,
				`REVOKE ALL PRIVILEGES FROM "docbrown"`,
				`REVOKE ALL ON "mydb" FROM "docbrown"`,
			},
		},
		{
			name:         "Grant all permissions",
			statusUsers:  http.StatusOK,
			showUsers:    []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",true],["reader",false]]}]}]}`),
			statusGrants: http.StatusOK,
			showGrants:   []byte(`{"results":[{"series":[{"columns":["database","privilege"],"values":[["mydb","ALL PRIVILEGES"]]}]}]}`),
			statusRevoke: http.StatusOK,
			revoke:       []byte(`{"results":[]}`),
			statusGrant:  http.StatusOK,
			grant:        []byte(`{"results":[]}`),
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "docbrown",
					Permissions: chronograf.Permissions{
						{
							Scope:   "all",
							Allowed: []string{"all"},
						},
						{
							Scope:   "database",
							Name:    "mydb",
							Allowed: []string{"WRITE", "READ"},
						},
					},
				},
			},
			want: []string{
				`SHOW USERS`,
				`SHOW GRANTS FOR "docbrown"`,
				`GRANT ALL PRIVILEGES TO "docbrown"`,
				`GRANT ALL ON "mydb" TO "docbrown"`,
			},
		},
		{
			name:         "Revoke some add some",
			statusUsers:  http.StatusOK,
			showUsers:    []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",true],["reader",false]]}]}]}`),
			statusGrants: http.StatusOK,
			showGrants:   []byte(`{"results":[{"series":[{"columns":["database","privilege"],"values":[["mydb","ALL PRIVILEGES"]]}]}]}`),
			statusRevoke: http.StatusOK,
			revoke:       []byte(`{"results":[]}`),
			statusGrant:  http.StatusOK,
			grant:        []byte(`{"results":[]}`),
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "docbrown",
					Permissions: chronograf.Permissions{
						{
							Scope:   "all",
							Allowed: []string{},
						},
						{
							Scope:   "database",
							Name:    "mydb",
							Allowed: []string{"WRITE"},
						},
						{
							Scope:   "database",
							Name:    "newdb",
							Allowed: []string{"WRITE", "READ"},
						},
					},
				},
			},
			want: []string{
				`SHOW USERS`,
				`SHOW GRANTS FOR "docbrown"`,
				`GRANT WRITE ON "mydb" TO "docbrown"`,
				`GRANT ALL ON "newdb" TO "docbrown"`,
				`REVOKE ALL PRIVILEGES FROM "docbrown"`,
			},
		},
		{
			name:         "Revoke some",
			statusUsers:  http.StatusOK,
			showUsers:    []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",false],["reader",false]]}]}]}`),
			statusGrants: http.StatusOK,
			showGrants:   []byte(`{"results":[]}`),
			statusRevoke: http.StatusOK,
			revoke:       []byte(`{"results":[]}`),
			statusGrant:  http.StatusOK,
			grant:        []byte(`{"results":[]}`),
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "docbrown",
					Permissions: chronograf.Permissions{
						{
							Scope:   "all",
							Allowed: []string{"ALL"},
						},
					},
				},
			},
			want: []string{
				`SHOW USERS`,
				`SHOW GRANTS FOR "docbrown"`,
				`GRANT ALL PRIVILEGES TO "docbrown"`,
			},
		},
		{
			name:         "Fail users",
			statusUsers:  http.StatusBadRequest,
			showUsers:    []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",true],["reader",false]]}]}]}`),
			statusGrants: http.StatusOK,
			showGrants:   []byte(`{"results":[{"series":[{"columns":["database","privilege"],"values":[["mydb","ALL PRIVILEGES"]]}]}]}`),
			statusRevoke: http.StatusOK,
			revoke:       []byte(`{"results":[]}`),
			statusGrant:  http.StatusOK,
			grant:        []byte(`{"results":[]}`),
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "docbrown",
				},
			},
			wantErr: true,
			want: []string{
				`SHOW USERS`,
			},
		},
		{
			name:         "fail grants",
			statusUsers:  http.StatusOK,
			showUsers:    []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",true],["reader",false]]}]}]}`),
			statusGrants: http.StatusOK,
			showGrants:   []byte(`{"results":[{"series":[{"columns":["database","privilege"],"values":[["mydb","ALL PRIVILEGES"]]}]}]}`),
			statusRevoke: http.StatusOK,
			revoke:       []byte(`{"results":[]}`),
			statusGrant:  http.StatusBadRequest,
			grant:        []byte(`{"results":[]}`),
			wantErr:      true,
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "docbrown",
					Permissions: chronograf.Permissions{
						{
							Scope:   "all",
							Allowed: []string{},
						},
						{
							Scope:   "database",
							Name:    "mydb",
							Allowed: []string{"WRITE"},
						},
						{
							Scope:   "database",
							Name:    "newdb",
							Allowed: []string{"WRITE", "READ"},
						},
					},
				},
			},
			want: []string{
				`SHOW USERS`,
				`SHOW GRANTS FOR "docbrown"`,
				`GRANT WRITE ON "mydb" TO "docbrown"`,
			},
		},
		{
			name:         "fail revoke",
			statusUsers:  http.StatusOK,
			showUsers:    []byte(`{"results":[{"series":[{"columns":["user","admin"],"values":[["admin",true],["docbrown",true],["reader",false]]}]}]}`),
			statusGrants: http.StatusOK,
			showGrants:   []byte(`{"results":[{"series":[{"columns":["database","privilege"],"values":[["mydb","ALL PRIVILEGES"]]}]}]}`),
			statusRevoke: http.StatusBadRequest,
			revoke:       []byte(`{"results":[]}`),
			statusGrant:  http.StatusOK,
			grant:        []byte(`{"results":[]}`),
			wantErr:      true,
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "docbrown",
					Permissions: chronograf.Permissions{
						{
							Scope:   "all",
							Allowed: []string{},
						},
						{
							Scope:   "database",
							Name:    "mydb",
							Allowed: []string{"WRITE"},
						},
						{
							Scope:   "database",
							Name:    "newdb",
							Allowed: []string{"WRITE", "READ"},
						},
					},
				},
			},
			want: []string{
				`SHOW USERS`,
				`SHOW GRANTS FOR "docbrown"`,
				`GRANT WRITE ON "mydb" TO "docbrown"`,
				`GRANT ALL ON "newdb" TO "docbrown"`,
				`REVOKE ALL PRIVILEGES FROM "docbrown"`,
			},
		},
	}
	for _, tt := range tests {
		queries := []string{}
		ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if path := r.URL.Path; path != "/query" {
				t.Error("Expected the path to contain `/query` but was", path)
			}
			query := r.URL.Query().Get("q")
			if strings.Contains(query, "GRANTS") {
				rw.WriteHeader(tt.statusGrants)
				rw.Write(tt.showGrants)
			} else if strings.Contains(query, "USERS") {
				rw.WriteHeader(tt.statusUsers)
				rw.Write(tt.showUsers)
			} else if strings.Contains(query, "REVOKE") {
				rw.WriteHeader(tt.statusRevoke)
				rw.Write(tt.revoke)
			} else if strings.Contains(query, "GRANT") {
				rw.WriteHeader(tt.statusGrant)
				rw.Write(tt.grant)
			} else if strings.Contains(query, "PASSWORD") {
				rw.WriteHeader(tt.statusPassword)
				rw.Write(tt.password)
			}
			queries = append(queries, query)
		}))
		u, _ := url.Parse(ts.URL)
		c := &Client{
			URL:    u,
			Logger: log.New(log.DebugLevel),
		}
		defer ts.Close()
		if err := c.Update(tt.args.ctx, tt.args.u); (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
		if !reflect.DeepEqual(queries, tt.want) {
			t.Errorf("%q. Client.Update() = %v, want %v", tt.name, queries, tt.want)
		}
	}
}

/*



 */
