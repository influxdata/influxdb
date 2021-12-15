package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/query"
)

type SourceProxyQueryService struct {
	Addr               string
	InsecureSkipVerify bool
	platform.SourceFields
}

func (s *SourceProxyQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
	switch req.Request.Compiler.CompilerType() {
	case lang.FluxCompilerType:
		return s.queryFlux(ctx, w, req)
	}
	return flux.Statistics{}, fmt.Errorf("compiler type not supported")
}

func (s *SourceProxyQueryService) queryFlux(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	u, err := NewURL(s.Addr, "/api/v2/query")
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(req); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	hreq.Header.Set("Authorization", fmt.Sprintf("Token %s", s.Token))
	hreq.Header.Set("Content-Type", "application/json")
	hreq = hreq.WithContext(ctx)

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	defer resp.Body.Close()
	if err := CheckError(resp); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	if _, err = io.Copy(w, resp.Body); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	return flux.Statistics{}, nil
}

func (s *SourceProxyQueryService) Check(context.Context) check.Response {
	return QueryHealthCheck(s.Addr, s.InsecureSkipVerify)
}
