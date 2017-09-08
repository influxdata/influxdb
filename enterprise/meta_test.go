package enterprise

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
)

func TestMetaClient_ShowCluster(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	tests := []struct {
		name    string
		fields  fields
		want    *Cluster
		wantErr bool
	}{
		{
			name: "Successful Show Cluster",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"data":[{"id":2,"version":"1.1.0-c1.1.0","tcpAddr":"data-1.twinpinesmall.net:8088","httpAddr":"data-1.twinpinesmall.net:8086","httpScheme":"https","status":"joined"}],"meta":[{"id":1,"addr":"meta-0.twinpinesmall.net:8091","httpScheme":"http","tcpAddr":"meta-0.twinpinesmall.net:8089","version":"1.1.0-c1.1.0"}]}`),
					nil,
					nil,
				),
			},
			want: &Cluster{
				DataNodes: []DataNode{
					{
						ID:         2,
						TCPAddr:    "data-1.twinpinesmall.net:8088",
						HTTPAddr:   "data-1.twinpinesmall.net:8086",
						HTTPScheme: "https",
						Status:     "joined",
					},
				},
				MetaNodes: []Node{
					{
						ID:         1,
						Addr:       "meta-0.twinpinesmall.net:8091",
						HTTPScheme: "http",
						TCPAddr:    "meta-0.twinpinesmall.net:8089",
					},
				},
			},
		},
		{
			name: "Failed Show Cluster",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusBadGateway,
					nil,
					nil,
					fmt.Errorf("time circuits on. Flux Capacitor... fluxxing"),
				),
			},
			wantErr: true,
		},
		{
			name: "Bad JSON from Show Cluster",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{data}`),
					nil,
					nil,
				),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		got, err := m.ShowCluster(context.Background())
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.ShowCluster() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. MetaClient.ShowCluster() = %v, want %v", tt.name, got, tt.want)
		}
		if tt.wantErr {
			continue
		}
		reqs := tt.fields.client.(*MockClient).Requests
		if len(reqs) != 1 {
			t.Errorf("%q. MetaClient.ShowCluster() expected 1 but got %d", tt.name, len(reqs))
			continue
		}
		req := reqs[0]
		if req.Method != "GET" {
			t.Errorf("%q. MetaClient.ShowCluster() expected GET method", tt.name)
		}
		if req.URL.Path != "/show-cluster" {
			t.Errorf("%q. MetaClient.ShowCluster() expected /show-cluster path but got %s", tt.name, req.URL.Path)
		}
	}
}

func TestMetaClient_Users(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	type args struct {
		ctx  context.Context
		name *string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Users
		wantErr bool
	}{
		{
			name: "Successful Show users",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"users":[{"name":"admin","hash":"1234","permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: nil,
			},
			want: &Users{
				Users: []User{
					{
						Name: "admin",
						Permissions: map[string][]string{
							"": []string{
								"ViewAdmin", "ViewChronograf",
							},
						},
					},
				},
			},
		},
		{
			name: "Successful Show users single user",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"users":[{"name":"admin","hash":"1234","permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: &[]string{"admin"}[0],
			},
			want: &Users{
				Users: []User{
					{
						Name: "admin",
						Permissions: map[string][]string{
							"": []string{
								"ViewAdmin", "ViewChronograf",
							},
						},
					},
				},
			},
		},
		{
			name: "Failure Show users",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"users":[{"name":"admin","hash":"1234","permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					fmt.Errorf("time circuits on. Flux Capacitor... fluxxing"),
				),
			},
			args: args{
				ctx:  context.Background(),
				name: nil,
			},
			wantErr: true,
		},
		{
			name: "Bad JSON from Show users",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{foo}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: nil,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		got, err := m.Users(tt.args.ctx, tt.args.name)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.Users() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. MetaClient.Users() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestMetaClient_User(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *User
		wantErr bool
	}{
		{
			name: "Successful Show users",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"users":[{"name":"admin","hash":"1234","permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: "admin",
			},
			want: &User{
				Name: "admin",
				Permissions: map[string][]string{
					"": []string{
						"ViewAdmin", "ViewChronograf",
					},
				},
			},
		},
		{
			name: "No such user",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusNotFound,
					[]byte(`{"error":"user not found"}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: "unknown",
			},
			wantErr: true,
		},
		{
			name: "Bad JSON",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusNotFound,
					[]byte(`{BAD}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx: context.Background(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		got, err := m.User(tt.args.ctx, tt.args.name)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.User() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. MetaClient.User() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestMetaClient_CreateUser(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	type args struct {
		ctx    context.Context
		name   string
		passwd string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Successful Create User",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					nil,
					nil,
					nil,
				),
			},
			args: args{
				ctx:    context.Background(),
				name:   "admin",
				passwd: "hunter2",
			},
			want: `{"action":"create","user":{"name":"admin","password":"hunter2"}}`,
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		if err := m.CreateUser(tt.args.ctx, tt.args.name, tt.args.passwd); (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.CreateUser() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
		if tt.wantErr {
			continue
		}
		reqs := tt.fields.client.(*MockClient).Requests
		if len(reqs) != 1 {
			t.Errorf("%q. MetaClient.CreateUser() expected 1 but got %d", tt.name, len(reqs))
			continue
		}
		req := reqs[0]
		if req.Method != "POST" {
			t.Errorf("%q. MetaClient.CreateUser() expected POST method", tt.name)
		}
		if req.URL.Path != "/user" {
			t.Errorf("%q. MetaClient.CreateUser() expected /user path but got %s", tt.name, req.URL.Path)
		}
		got, _ := ioutil.ReadAll(req.Body)
		if string(got) != tt.want {
			t.Errorf("%q. MetaClient.CreateUser() = %v, want %v", tt.name, string(got), tt.want)
		}
	}
}

func TestMetaClient_ChangePassword(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	type args struct {
		ctx    context.Context
		name   string
		passwd string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Successful Change Password",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					nil,
					nil,
					nil,
				),
			},
			args: args{
				ctx:    context.Background(),
				name:   "admin",
				passwd: "hunter2",
			},
			want: `{"action":"change-password","user":{"name":"admin","password":"hunter2"}}`,
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		if err := m.ChangePassword(tt.args.ctx, tt.args.name, tt.args.passwd); (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.ChangePassword() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}

		if tt.wantErr {
			continue
		}
		reqs := tt.fields.client.(*MockClient).Requests
		if len(reqs) != 1 {
			t.Errorf("%q. MetaClient.ChangePassword() expected 1 but got %d", tt.name, len(reqs))
			continue
		}
		req := reqs[0]
		if req.Method != "POST" {
			t.Errorf("%q. MetaClient.ChangePassword() expected POST method", tt.name)
		}
		if req.URL.Path != "/user" {
			t.Errorf("%q. MetaClient.ChangePassword() expected /user path but got %s", tt.name, req.URL.Path)
		}
		got, _ := ioutil.ReadAll(req.Body)
		if string(got) != tt.want {
			t.Errorf("%q. MetaClient.ChangePassword() = %v, want %v", tt.name, string(got), tt.want)
		}
	}
}

func TestMetaClient_DeleteUser(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Successful delete User",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					nil,
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: "admin",
			},
			want: `{"action":"delete","user":{"name":"admin"}}`,
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		if err := m.DeleteUser(tt.args.ctx, tt.args.name); (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.DeleteUser() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
		if tt.wantErr {
			continue
		}
		reqs := tt.fields.client.(*MockClient).Requests
		if len(reqs) != 1 {
			t.Errorf("%q. MetaClient.DeleteUser() expected 1 but got %d", tt.name, len(reqs))
			continue
		}
		req := reqs[0]
		if req.Method != "POST" {
			t.Errorf("%q. MetaClient.DeleteUser() expected POST method", tt.name)
		}
		if req.URL.Path != "/user" {
			t.Errorf("%q. MetaClient.DeleteUser() expected /user path but got %s", tt.name, req.URL.Path)
		}
		got, _ := ioutil.ReadAll(req.Body)
		if string(got) != tt.want {
			t.Errorf("%q. MetaClient.DeleteUser() = %v, want %v", tt.name, string(got), tt.want)
		}
	}
}

func TestMetaClient_SetUserPerms(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	type args struct {
		ctx   context.Context
		name  string
		perms Permissions
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantRm  string
		wantAdd string
		wantErr bool
	}{
		{
			name: "Successful set permissions User",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"users":[{"name":"admin","hash":"1234","permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: "admin",
			},
			wantRm: `{"action":"remove-permissions","user":{"name":"admin","permissions":{"":["ViewAdmin","ViewChronograf"]}}}`,
		},
		{
			name: "Successful set permissions User",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"users":[{"name":"admin","hash":"1234","permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: "admin",
				perms: Permissions{
					"telegraf": []string{
						"ReadData",
					},
				},
			},
			wantRm:  `{"action":"remove-permissions","user":{"name":"admin","permissions":{"":["ViewAdmin","ViewChronograf"]}}}`,
			wantAdd: `{"action":"add-permissions","user":{"name":"admin","permissions":{"telegraf":["ReadData"]}}}`,
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		if err := m.SetUserPerms(tt.args.ctx, tt.args.name, tt.args.perms); (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.SetUserPerms() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
		if tt.wantErr {
			continue
		}
		reqs := tt.fields.client.(*MockClient).Requests
		if len(reqs) < 2 {
			t.Errorf("%q. MetaClient.SetUserPerms() expected 2 but got %d", tt.name, len(reqs))
			continue
		}

		usr := reqs[0]
		if usr.Method != "GET" {
			t.Errorf("%q. MetaClient.SetUserPerms() expected GET method", tt.name)
		}
		if usr.URL.Path != "/user" {
			t.Errorf("%q. MetaClient.SetUserPerms() expected /user path but got %s", tt.name, usr.URL.Path)
		}

		prm := reqs[1]
		if prm.Method != "POST" {
			t.Errorf("%q. MetaClient.SetUserPerms() expected GET method", tt.name)
		}
		if prm.URL.Path != "/user" {
			t.Errorf("%q. MetaClient.SetUserPerms() expected /user path but got %s", tt.name, prm.URL.Path)
		}

		got, _ := ioutil.ReadAll(prm.Body)
		if string(got) != tt.wantRm {
			t.Errorf("%q. MetaClient.SetUserPerms() = %v, want %v", tt.name, string(got), tt.wantRm)
		}
		if tt.wantAdd != "" {
			prm := reqs[2]
			if prm.Method != "POST" {
				t.Errorf("%q. MetaClient.SetUserPerms() expected GET method", tt.name)
			}
			if prm.URL.Path != "/user" {
				t.Errorf("%q. MetaClient.SetUserPerms() expected /user path but got %s", tt.name, prm.URL.Path)
			}

			got, _ := ioutil.ReadAll(prm.Body)
			if string(got) != tt.wantAdd {
				t.Errorf("%q. MetaClient.SetUserPerms() = %v, want %v", tt.name, string(got), tt.wantAdd)
			}
		}
	}
}

func TestMetaClient_Roles(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	type args struct {
		ctx  context.Context
		name *string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Roles
		wantErr bool
	}{
		{
			name: "Successful Show role",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"roles":[{"name":"admin","users":["marty"],"permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: nil,
			},
			want: &Roles{
				Roles: []Role{
					{
						Name: "admin",
						Permissions: map[string][]string{
							"": []string{
								"ViewAdmin", "ViewChronograf",
							},
						},
						Users: []string{"marty"},
					},
				},
			},
		},
		{
			name: "Successful Show role single role",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"roles":[{"name":"admin","users":["marty"],"permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: &[]string{"admin"}[0],
			},
			want: &Roles{
				Roles: []Role{
					{
						Name: "admin",
						Permissions: map[string][]string{
							"": []string{
								"ViewAdmin", "ViewChronograf",
							},
						},
						Users: []string{"marty"},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		got, err := m.Roles(tt.args.ctx, tt.args.name)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.Roles() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. MetaClient.Roles() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestMetaClient_Role(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Role
		wantErr bool
	}{
		{
			name: "Successful Show role",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"roles":[{"name":"admin","users":["marty"],"permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: "admin",
			},
			want: &Role{
				Name: "admin",
				Permissions: map[string][]string{
					"": []string{
						"ViewAdmin", "ViewChronograf",
					},
				},
				Users: []string{"marty"},
			},
		},
		{
			name: "No such role",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusNotFound,
					[]byte(`{"error":"user not found"}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: "unknown",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		got, err := m.Role(tt.args.ctx, tt.args.name)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.Role() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. MetaClient.Role() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestMetaClient_UserRoles(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	type args struct {
		ctx  context.Context
		name *string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]Roles
		wantErr bool
	}{
		{
			name: "Successful Show all roles",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"roles":[{"name":"timetravelers","users":["marty","docbrown"],"permissions":{"":["ViewAdmin","ViewChronograf"]}},{"name":"mcfly","users":["marty","george"],"permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: nil,
			},
			want: map[string]Roles{
				"marty": Roles{
					Roles: []Role{
						{
							Name: "timetravelers",
							Permissions: map[string][]string{
								"": []string{
									"ViewAdmin", "ViewChronograf",
								},
							},
							Users: []string{"marty", "docbrown"},
						},
						{
							Name: "mcfly",
							Permissions: map[string][]string{
								"": []string{
									"ViewAdmin", "ViewChronograf",
								},
							},
							Users: []string{"marty", "george"},
						},
					},
				},
				"docbrown": Roles{
					Roles: []Role{
						{
							Name: "timetravelers",
							Permissions: map[string][]string{
								"": []string{
									"ViewAdmin", "ViewChronograf",
								},
							},
							Users: []string{"marty", "docbrown"},
						},
					},
				},
				"george": Roles{
					Roles: []Role{
						{
							Name: "mcfly",
							Permissions: map[string][]string{
								"": []string{
									"ViewAdmin", "ViewChronograf",
								},
							},
							Users: []string{"marty", "george"},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		got, err := m.UserRoles(tt.args.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.UserRoles() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. MetaClient.UserRoles() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestMetaClient_CreateRole(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Successful Create Role",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					nil,
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: "admin",
			},
			want: `{"action":"create","role":{"name":"admin"}}`,
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		if err := m.CreateRole(tt.args.ctx, tt.args.name); (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.CreateRole() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
		reqs := tt.fields.client.(*MockClient).Requests
		if len(reqs) != 1 {
			t.Errorf("%q. MetaClient.CreateRole() expected 1 but got %d", tt.name, len(reqs))
			continue
		}
		req := reqs[0]
		if req.Method != "POST" {
			t.Errorf("%q. MetaClient.CreateRole() expected POST method", tt.name)
		}
		if req.URL.Path != "/role" {
			t.Errorf("%q. MetaClient.CreateRole() expected /role path but got %s", tt.name, req.URL.Path)
		}
		got, _ := ioutil.ReadAll(req.Body)
		if string(got) != tt.want {
			t.Errorf("%q. MetaClient.CreateRole() = %v, want %v", tt.name, string(got), tt.want)
		}
	}
}

func TestMetaClient_DeleteRole(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Successful delete role",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					nil,
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: "admin",
			},
			want: `{"action":"delete","role":{"name":"admin"}}`,
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		if err := m.DeleteRole(tt.args.ctx, tt.args.name); (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.DeleteRole() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
		if tt.wantErr {
			continue
		}
		reqs := tt.fields.client.(*MockClient).Requests
		if len(reqs) != 1 {
			t.Errorf("%q. MetaClient.DeleteRole() expected 1 but got %d", tt.name, len(reqs))
			continue
		}
		req := reqs[0]
		if req.Method != "POST" {
			t.Errorf("%q. MetaClient.DeleDeleteRoleteUser() expected POST method", tt.name)
		}
		if req.URL.Path != "/role" {
			t.Errorf("%q. MetaClient.DeleteRole() expected /role path but got %s", tt.name, req.URL.Path)
		}
		got, _ := ioutil.ReadAll(req.Body)
		if string(got) != tt.want {
			t.Errorf("%q. MetaClient.DeleteRole() = %v, want %v", tt.name, string(got), tt.want)
		}
	}
}

func TestMetaClient_SetRolePerms(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	type args struct {
		ctx   context.Context
		name  string
		perms Permissions
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantRm  string
		wantAdd string
		wantErr bool
	}{
		{
			name: "Successful set permissions role",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"roles":[{"name":"admin","users":["marty"],"permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: "admin",
			},
			wantRm: `{"action":"remove-permissions","role":{"name":"admin","permissions":{"":["ViewAdmin","ViewChronograf"]},"users":["marty"]}}`,
		},
		{
			name: "Successful set single permissions role",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"roles":[{"name":"admin","users":["marty"],"permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: "admin",
				perms: Permissions{
					"telegraf": []string{
						"ReadData",
					},
				},
			},
			wantRm:  `{"action":"remove-permissions","role":{"name":"admin","permissions":{"":["ViewAdmin","ViewChronograf"]},"users":["marty"]}}`,
			wantAdd: `{"action":"add-permissions","role":{"name":"admin","permissions":{"telegraf":["ReadData"]}}}`,
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		if err := m.SetRolePerms(tt.args.ctx, tt.args.name, tt.args.perms); (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.SetRolePerms() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
		if tt.wantErr {
			continue
		}
		reqs := tt.fields.client.(*MockClient).Requests
		if len(reqs) < 2 {
			t.Errorf("%q. MetaClient.SetRolePerms() expected 2 but got %d", tt.name, len(reqs))
			continue
		}

		usr := reqs[0]
		if usr.Method != "GET" {
			t.Errorf("%q. MetaClient.SetRolePerms() expected GET method", tt.name)
		}
		if usr.URL.Path != "/role" {
			t.Errorf("%q. MetaClient.SetRolePerms() expected /user path but got %s", tt.name, usr.URL.Path)
		}

		prm := reqs[1]
		if prm.Method != "POST" {
			t.Errorf("%q. MetaClient.SetRolePerms() expected GET method", tt.name)
		}
		if prm.URL.Path != "/role" {
			t.Errorf("%q. MetaClient.SetRolePerms() expected /role path but got %s", tt.name, prm.URL.Path)
		}

		got, _ := ioutil.ReadAll(prm.Body)
		if string(got) != tt.wantRm {
			t.Errorf("%q. MetaClient.SetRolePerms() = %v, want %v", tt.name, string(got), tt.wantRm)
		}
		if tt.wantAdd != "" {
			prm := reqs[2]
			if prm.Method != "POST" {
				t.Errorf("%q. MetaClient.SetRolePerms() expected GET method", tt.name)
			}
			if prm.URL.Path != "/role" {
				t.Errorf("%q. MetaClient.SetRolePerms() expected /role path but got %s", tt.name, prm.URL.Path)
			}

			got, _ := ioutil.ReadAll(prm.Body)
			if string(got) != tt.wantAdd {
				t.Errorf("%q. MetaClient.SetRolePerms() = %v, want %v", tt.name, string(got), tt.wantAdd)
			}
		}
	}
}

func TestMetaClient_SetRoleUsers(t *testing.T) {
	type fields struct {
		URL    *url.URL
		client interface {
			Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error)
		}
	}
	type args struct {
		ctx   context.Context
		name  string
		users []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wants   []string
		wantErr bool
	}{
		{
			name: "Successful set users role (remove user from role)",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"roles":[{"name":"admin","users":["marty"],"permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:  context.Background(),
				name: "admin",
			},
			wants: []string{`{"action":"remove-users","role":{"name":"admin","users":["marty"]}}`},
		},
		{
			name: "Successful set single user role",
			fields: fields{
				URL: &url.URL{
					Host:   "twinpinesmall.net:8091",
					Scheme: "https",
				},
				client: NewMockClient(
					http.StatusOK,
					[]byte(`{"roles":[{"name":"admin","users":[],"permissions":{"":["ViewAdmin","ViewChronograf"]}}]}`),
					nil,
					nil,
				),
			},
			args: args{
				ctx:   context.Background(),
				name:  "admin",
				users: []string{"marty"},
			},
			wants: []string{
				`{"action":"add-users","role":{"name":"admin","users":["marty"]}}`,
			},
		},
	}
	for _, tt := range tests {
		m := &MetaClient{
			URL:    tt.fields.URL,
			client: tt.fields.client,
		}
		if err := m.SetRoleUsers(tt.args.ctx, tt.args.name, tt.args.users); (err != nil) != tt.wantErr {
			t.Errorf("%q. MetaClient.SetRoleUsers() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}

		if tt.wantErr {
			continue
		}
		reqs := tt.fields.client.(*MockClient).Requests
		if len(reqs) != len(tt.wants)+1 {
			t.Errorf("%q. MetaClient.SetRoleUsers() expected %d but got %d", tt.name, len(tt.wants)+1, len(reqs))
			continue
		}

		usr := reqs[0]
		if usr.Method != "GET" {
			t.Errorf("%q. MetaClient.SetRoleUsers() expected GET method", tt.name)
		}
		if usr.URL.Path != "/role" {
			t.Errorf("%q. MetaClient.SetRoleUsers() expected /user path but got %s", tt.name, usr.URL.Path)
		}
		for i := range tt.wants {
			prm := reqs[i+1]
			if prm.Method != "POST" {
				t.Errorf("%q. MetaClient.SetRoleUsers() expected GET method", tt.name)
			}
			if prm.URL.Path != "/role" {
				t.Errorf("%q. MetaClient.SetRoleUsers() expected /role path but got %s", tt.name, prm.URL.Path)
			}

			got, _ := ioutil.ReadAll(prm.Body)
			if string(got) != tt.wants[i] {
				t.Errorf("%q. MetaClient.SetRoleUsers() = %v, want %v", tt.name, string(got), tt.wants[i])
			}
		}
	}
}

type MockClient struct {
	Code      int // HTTP Status code
	Body      []byte
	HeaderMap http.Header
	Err       error

	Requests []*http.Request
}

func NewMockClient(code int, body []byte, headers http.Header, err error) *MockClient {
	return &MockClient{
		Code:      code,
		Body:      body,
		HeaderMap: headers,
		Err:       err,
		Requests:  make([]*http.Request, 0),
	}
}

func (c *MockClient) Do(URL *url.URL, path, method string, params map[string]string, body io.Reader) (*http.Response, error) {
	if c == nil {
		return nil, fmt.Errorf("NIL MockClient")
	}
	if URL == nil {
		return nil, fmt.Errorf("NIL url")
	}
	if c.Err != nil {
		return nil, c.Err
	}

	// Record the request in the mock client
	p := url.Values{}
	for k, v := range params {
		p.Add(k, v)
	}

	URL.Path = path
	URL.RawQuery = p.Encode()

	req, err := http.NewRequest(method, URL.String(), body)
	if err != nil {
		return nil, err
	}
	c.Requests = append(c.Requests, req)

	return &http.Response{
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		StatusCode: c.Code,
		Status:     http.StatusText(c.Code),
		Header:     c.HeaderMap,
		Body:       ioutil.NopCloser(bytes.NewReader(c.Body)),
	}, nil
}

func Test_AuthedCheckRedirect_Do(t *testing.T) {
	var ts2URL string
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := http.Header{
			"Referer":         []string{ts2URL},
			"Accept-Encoding": []string{"gzip"},
			"Authorization":   []string{"hunter2"},
		}
		for k, v := range want {
			if !reflect.DeepEqual(r.Header[k], v) {
				t.Errorf("Request.Header = %#v; want %#v", r.Header[k], v)
			}
		}
		if t.Failed() {
			w.Header().Set("Result", "got errors")
		} else {
			w.Header().Set("Result", "ok")
		}
	}))
	defer ts1.Close()

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, ts1.URL, http.StatusFound)
	}))
	defer ts2.Close()
	ts2URL = ts2.URL

	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	d := &defaultClient{}
	c := &http.Client{
		Transport:     tr,
		CheckRedirect: d.AuthedCheckRedirect,
	}

	req, _ := http.NewRequest("GET", ts2.URL, nil)
	req.Header.Add("Cookie", "foo=bar")
	req.Header.Add("Authorization", "hunter2")
	req.Header.Add("Howdy", "doody")
	req.Header.Set("User-Agent", "Darth Vader, an extraterrestrial from the Planet Vulcan")

	res, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatal(res.Status)
	}

	if got := res.Header.Get("Result"); got != "ok" {
		t.Errorf("result = %q; want ok", got)
	}
}
