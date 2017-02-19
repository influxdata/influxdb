package influx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
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
	type args struct {
		ctx context.Context
		u   *chronograf.User
	}
	tests := []struct {
		name      string
		args      args
		status    int
		want      *chronograf.User
		wantQuery string
		wantErr   bool
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
			wantQuery: `CREATE USER "docbrown" WITH PASSWORD 'Dont Need Roads'`,
			want: &chronograf.User{
				Name:   "docbrown",
				Passwd: "Dont Need Roads",
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
			wantQuery: `CREATE USER "docbrown" WITH PASSWORD 'Dont Need Roads'`,
			wantErr:   true,
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
			rw.Write([]byte(`{"results":[{}]}`))
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
		if tt.wantQuery != query {
			t.Errorf("%q. Client.Add() query = %v, want %v", tt.name, query, tt.wantQuery)
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. Client.Add() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
