package enterprise_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/chronograf/enterprise"
	"github.com/influxdata/platform/chronograf/influx"
	"github.com/influxdata/platform/chronograf/log"
)

func Test_Enterprise_FetchesDataNodes(t *testing.T) {
	t.Parallel()
	showClustersCalled := false
	ctrl := &mockCtrl{
		showCluster: func(ctx context.Context) (*enterprise.Cluster, error) {
			showClustersCalled = true
			return &enterprise.Cluster{}, nil
		},
	}

	cl := &enterprise.Client{
		Ctrl: ctrl,
	}

	bg := context.Background()
	err := cl.Connect(bg, &chronograf.Source{})

	if err != nil {
		t.Fatal("Unexpected error while creating enterprise client. err:", err)
	}

	if showClustersCalled != true {
		t.Fatal("Expected request to meta node but none was issued")
	}
}

func Test_Enterprise_IssuesQueries(t *testing.T) {
	t.Parallel()

	called := false
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		called = true
		if r.URL.Path != "/query" {
			t.Fatal("Expected request to '/query' but was", r.URL.Path)
		}
		rw.Write([]byte(`{}`))
	}))
	defer ts.Close()

	cl := &enterprise.Client{
		Ctrl:   NewMockControlClient(ts.URL),
		Logger: log.New(log.DebugLevel),
	}

	err := cl.Connect(context.Background(), &chronograf.Source{})
	if err != nil {
		t.Fatal("Unexpected error initializing client: err:", err)
	}

	_, err = cl.Query(context.Background(), chronograf.Query{Command: "show shards", DB: "_internal", RP: "autogen"})

	if err != nil {
		t.Fatal("Unexpected error while querying data node: err:", err)
	}

	if called == false {
		t.Fatal("Expected request to data node but none was received")
	}
}

func Test_Enterprise_AdvancesDataNodes(t *testing.T) {
	m1 := NewMockTimeSeries("http://host-1.example.com:8086")
	m2 := NewMockTimeSeries("http://host-2.example.com:8086")
	cl, err := enterprise.NewClientWithTimeSeries(
		log.New(log.DebugLevel),
		"http://meta.example.com:8091",
		&influx.BasicAuth{
			Username: "marty",
			Password: "thelake",
		},
		false,
		false,
		chronograf.TimeSeries(m1),
		chronograf.TimeSeries(m2))
	if err != nil {
		t.Error("Unexpected error while initializing client: err:", err)
	}

	err = cl.Connect(context.Background(), &chronograf.Source{})
	if err != nil {
		t.Error("Unexpected error while initializing client: err:", err)
	}

	_, err = cl.Query(context.Background(), chronograf.Query{Command: "show shards", DB: "_internal", RP: "autogen"})
	if err != nil {
		t.Fatal("Unexpected error while issuing query: err:", err)
	}

	_, err = cl.Query(context.Background(), chronograf.Query{Command: "show shards", DB: "_internal", RP: "autogen"})
	if err != nil {
		t.Fatal("Unexpected error while issuing query: err:", err)
	}

	if m1.QueryCtr != 1 || m2.QueryCtr != 1 {
		t.Fatalf("Expected m1.Query to be called once but was %d. Expected m2.Query to be called once but was %d\n", m1.QueryCtr, m2.QueryCtr)
	}
}

func Test_Enterprise_NewClientWithURL(t *testing.T) {
	t.Parallel()

	urls := []struct {
		name               string
		url                string
		username           string
		password           string
		tls                bool
		insecureSkipVerify bool
		wantErr            bool
	}{
		{
			name: "no tls should have no error",
			url:  "http://localhost:8086",
		},
		{
			name: "tls sholuld have no error",
			url:  "https://localhost:8086",
		},
		{
			name:     "no tls but with basic auth",
			url:      "http://localhost:8086",
			username: "username",
			password: "password",
		},
		{
			name: "tls request but url is not tls should not error",
			url:  "http://localhost:8086",
			tls:  true,
		},
		{
			name:               "https with tls and with insecureSkipVerify should not error",
			url:                "https://localhost:8086",
			tls:                true,
			insecureSkipVerify: true,
		},
		{
			name: "URL does not require http or https",
			url:  "localhost:8086",
		},
		{
			name: "URL with TLS request should not error",
			url:  "localhost:8086",
			tls:  true,
		},
		{
			name:    "invalid URL causes error",
			url:     ":http",
			wantErr: true,
		},
	}

	for _, testURL := range urls {
		_, err := enterprise.NewClientWithURL(
			testURL.url,
			&influx.BasicAuth{
				Username: testURL.username,
				Password: testURL.password,
			},
			testURL.tls,
			testURL.insecureSkipVerify,
			log.New(log.DebugLevel))
		if err != nil && !testURL.wantErr {
			t.Errorf("Unexpected error creating Client with URL %s and TLS preference %t. err: %s", testURL.url, testURL.tls, err.Error())
		} else if err == nil && testURL.wantErr {
			t.Errorf("Expected error creating Client with URL %s and TLS preference %t", testURL.url, testURL.tls)
		}
	}
}

func Test_Enterprise_ComplainsIfNotOpened(t *testing.T) {
	m1 := NewMockTimeSeries("http://host-1.example.com:8086")
	cl, err := enterprise.NewClientWithTimeSeries(
		log.New(log.DebugLevel),
		"http://meta.example.com:8091",
		&influx.BasicAuth{
			Username: "docbrown",
			Password: "1.21 gigawatts",
		},
		false, false, chronograf.TimeSeries(m1))
	if err != nil {
		t.Error("Expected ErrUnitialized, but was this err:", err)
	}
	_, err = cl.Query(context.Background(), chronograf.Query{Command: "show shards", DB: "_internal", RP: "autogen"})
	if err != chronograf.ErrUninitialized {
		t.Error("Expected ErrUnitialized, but was this err:", err)
	}
}

func TestClient_Permissions(t *testing.T) {
	tests := []struct {
		name string

		want chronograf.Permissions
	}{
		{
			name: "All possible enterprise permissions",
			want: chronograf.Permissions{
				{
					Scope: chronograf.AllScope,
					Allowed: chronograf.Allowances{
						"NoPermissions",
						"ViewAdmin",
						"ViewChronograf",
						"CreateDatabase",
						"CreateUserAndRole",
						"AddRemoveNode",
						"DropDatabase",
						"DropData",
						"ReadData",
						"WriteData",
						"Rebalance",
						"ManageShard",
						"ManageContinuousQuery",
						"ManageQuery",
						"ManageSubscription",
						"Monitor",
						"CopyShard",
						"KapacitorAPI",
						"KapacitorConfigAPI",
					},
				},
				{
					Scope: chronograf.DBScope,
					Allowed: chronograf.Allowances{
						"NoPermissions",
						"ViewAdmin",
						"ViewChronograf",
						"CreateDatabase",
						"CreateUserAndRole",
						"AddRemoveNode",
						"DropDatabase",
						"DropData",
						"ReadData",
						"WriteData",
						"Rebalance",
						"ManageShard",
						"ManageContinuousQuery",
						"ManageQuery",
						"ManageSubscription",
						"Monitor",
						"CopyShard",
						"KapacitorAPI",
						"KapacitorConfigAPI",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		c := &enterprise.Client{}
		if got := c.Permissions(context.Background()); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. Client.Permissions() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
