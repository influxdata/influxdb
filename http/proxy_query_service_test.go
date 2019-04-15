package http

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
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
