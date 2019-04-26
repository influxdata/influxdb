package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/query/mock"
	"go.uber.org/zap"
)

func TestProxyQueryService_Query(t *testing.T) {
	id, err := influxdb.IDFromString("deadbeefbeefdead")
	if err != nil {
		t.Fatalf("error creating org ID: %v", err)
	}

	h := NewProxyQueryHandler("test")
	h.CompilerMappings = make(flux.CompilerMappings)
	h.DialectMappings = make(flux.DialectMappings)
	h.Logger = zap.NewNop()
	if err := lang.AddCompilerMappings(h.CompilerMappings); err != nil {
		t.Fatalf("error adding compiler mappings: %v", err)
	}
	if err := csv.AddDialectMappings(h.DialectMappings); err != nil {
		t.Fatalf("error adding dialect mappings: %v", err)
	}
	h.ProxyQueryService = &mock.ProxyQueryService{
		QueryF: func(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
			if _, err := io.WriteString(w, "boo"); err != nil {
				return flux.Statistics{}, err
			}
			return flux.Statistics{
				TotalDuration: 777,
			}, nil
		},
	}

	ts := httptest.NewServer(h)
	defer ts.Close()

	svc := ProxyQueryService{
		Addr: ts.URL,
	}

	var w bytes.Buffer
	req := query.ProxyRequest{
		Request: query.Request{
			Authorization: &influxdb.Authorization{
				ID:     *id,
				OrgID:  *id,
				UserID: *id,
			},
			OrganizationID: *id,
			Compiler: lang.FluxCompiler{
				Query: "buckets()",
			},
		},
		Dialect: csv.Dialect{},
	}
	stats, err := svc.Query(context.Background(), &w, &req)
	if err != nil {
		t.Fatalf("call to ProxyQueryService.Query failed: %v", err.Error())
	}

	if w.String() != "boo" {
		t.Errorf(`unexpected return: -want/+got: -"boo"/+"%v"`, w.String())
	}

	if diff := cmp.Diff(flux.Statistics{TotalDuration: 777}, stats); diff != "" {
		t.Errorf("Query returned unexpected stats -want/+got: %v", diff)
	}
}

// Certain error cases must be encoded as influxdb.Error so they can be properly decoded clientside.
func TestProxyQueryHandler_Errors(t *testing.T) {
	h := NewProxyQueryHandler("test")
	h.CompilerMappings = make(flux.CompilerMappings)
	h.DialectMappings = make(flux.DialectMappings)
	h.Logger = zap.NewNop()
	if err := lang.AddCompilerMappings(h.CompilerMappings); err != nil {
		t.Fatalf("error adding compiler mappings: %v", err)
	}
	if err := csv.AddDialectMappings(h.DialectMappings); err != nil {
		t.Fatalf("error adding dialect mappings: %v", err)
	}
	h.ProxyQueryService = &mock.ProxyQueryService{
		QueryF: func(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
			return flux.Statistics{}, errors.New("some query error")
		},
	}

	t.Run("bad JSON request", func(t *testing.T) {
		ts := httptest.NewServer(h)
		defer ts.Close()

		resp, err := http.Post(ts.URL+"/queryproxysvc", "application/json", strings.NewReader("oops"))
		if err != nil {
			t.Fatal(err)
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected bad request status, got %d", resp.StatusCode)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		var ierr influxdb.Error
		if err := json.Unmarshal(body, &ierr); err != nil {
			t.Logf("failed to json unmarshal into influxdb.error: %q", body)
			t.Fatal(err)
		}

		if !strings.Contains(ierr.Msg, "decode request body") {
			t.Fatalf("expected error to mention decoding, got %s", ierr.Msg)
		}
	})

	t.Run("syntatically valid JSON request, but no authorization", func(t *testing.T) {
		ts := httptest.NewServer(h)
		defer ts.Close()

		req := query.ProxyRequest{
			Request: query.Request{
				OrganizationID: 2,
				Compiler: lang.FluxCompiler{
					Query: "buckets()",
				},
			},
			Dialect: csv.Dialect{},
		}
		js, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := http.Post(ts.URL+"/queryproxysvc", "application/json", bytes.NewReader(js))
		if err != nil {
			t.Fatal(err)
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected unauthorized status, got %d", resp.StatusCode)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		var ierr influxdb.Error
		if err := json.Unmarshal(body, &ierr); err != nil {
			t.Logf("failed to json unmarshal into influxdb.error: %q", body)
			t.Fatal(err)
		}

		if !strings.Contains(ierr.Msg, "authorization is missing") {
			t.Fatalf("expected error to mention missing authorization, got %s", ierr.Msg)
		}
	})

	t.Run("valid request but query has an error", func(t *testing.T) {
		ts := httptest.NewServer(h)
		defer ts.Close()

		req := query.ProxyRequest{
			Request: query.Request{
				Authorization: &influxdb.Authorization{
					ID:     1,
					OrgID:  2,
					UserID: 3,
				},
				OrganizationID: 2,
				Compiler: lang.FluxCompiler{
					Query: "buckets()",
				},
			},
			Dialect: csv.Dialect{},
		}
		js, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := http.Post(ts.URL+"/queryproxysvc", "application/json", bytes.NewReader(js))
		if err != nil {
			t.Fatal(err)
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusInternalServerError {
			t.Errorf("expected internal error status, got %d", resp.StatusCode)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		var ierr influxdb.Error
		if err := json.Unmarshal(body, &ierr); err != nil {
			t.Logf("failed to json unmarshal into influxdb.error: %q", body)
			t.Fatal(err)
		}

		if !strings.Contains(ierr.Msg, "failed to execute query") {
			t.Fatalf("expected error to mention failure to execute query, got %s", ierr.Msg)
		}
		if ierr.Err.Error() != "some query error" {
			t.Fatalf("expected wrapped error to be the query service error, got %s", ierr.Err.Error())
		}
	})
}
