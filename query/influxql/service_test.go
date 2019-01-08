package influxql_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/query/influxql"
)

func TestService(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the parameters were passed correctly.
		if want, got := "POST", r.Method; want != got {
			t.Errorf("unexpected method -want/+got\n\t- %q\n\t+ %q", want, got)
		}
		if want, got := "SHOW DATABASES", r.FormValue("q"); want != got {
			t.Errorf("unexpected query -want/+got\n\t- %q\n\t+ %q", want, got)
		}
		if want, got := "db0", r.FormValue("db"); want != got {
			t.Errorf("unexpected database -want/+got\n\t- %q\n\t+ %q", want, got)
		}
		if want, got := "rp0", r.FormValue("rp"); want != got {
			t.Errorf("unexpected retention policy -want/+got\n\t- %q\n\t+ %q", want, got)
		}
		user, pass, ok := r.BasicAuth()
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if want, got := "me", user; want != got {
			t.Errorf("unexpected username -want/+got\n\t- %q\n\t+ %q", want, got)
		}
		if want, got := "secretpassword", pass; want != got {
			t.Errorf("unexpected password -want/+got\n\t- %q\n\t+ %q", want, got)
		}
		io.WriteString(w, `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["db0"]]}]}]}`)
	}))
	defer server.Close()

	service := &influxql.Service{
		Endpoints: map[string]influxql.Endpoint{
			"myserver": {
				URL:      server.URL,
				Username: "me",
				Password: "secretpassword",
			},
		},
	}
	req := &query.Request{Compiler: &influxql.Compiler{
		Cluster: "myserver",
		DB:      "db0",
		RP:      "rp0",
		Query:   "SHOW DATABASES",
	}}

	results, err := service.Query(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer results.Release()
	_, err = service.QueryRawJSON(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}
