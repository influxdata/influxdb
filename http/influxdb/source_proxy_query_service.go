package influxdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	platform "github.com/influxdata/influxdb/v2"
	platformhttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/check"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/query"
)

type SourceProxyQueryService struct {
	InsecureSkipVerify bool
	URL                string
	OrganizationID     platform2.ID
	platform.SourceFields
	platform.V1SourceFields
}

func (s *SourceProxyQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
	switch req.Request.Compiler.CompilerType() {
	case lang.FluxCompilerType:
		return s.fluxQuery(ctx, w, req)
	}

	return flux.Statistics{}, fmt.Errorf("compiler type not supported")
}

func (s *SourceProxyQueryService) fluxQuery(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	request := struct {
		Query   string       `json:"query"`
		Type    string       `json:"type"`
		Dialect flux.Dialect `json:"dialect"`
	}{}

	switch c := req.Request.Compiler.(type) {
	case lang.FluxCompiler:
		request.Query = c.Query
		request.Type = lang.FluxCompilerType
	default:
		return flux.Statistics{}, tracing.LogError(span, fmt.Errorf("compiler type not supported: %s", c.CompilerType()))
	}

	request.Dialect = req.Dialect
	if request.Dialect == nil {
		request.Dialect = &csv.Dialect{
			ResultEncoderConfig: csv.ResultEncoderConfig{
				Annotations: nil,
				NoHeader:    false,
				Delimiter:   ',',
			},
		}
	}

	u, err := newURL(s.URL, "/api/v2/query")
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	qp := u.Query()
	qp.Set("organizationID", req.Request.OrganizationID.String())
	u.RawQuery = qp.Encode()

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(request); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	hreq.Header.Set("Authorization", s.Token)
	hreq.Header.Set("Content-Type", "application/json")
	hreq = hreq.WithContext(ctx)

	hc := newTraceClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	defer resp.Body.Close()
	if err := platformhttp.CheckError(resp); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	if _, err = io.Copy(w, resp.Body); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	return flux.Statistics{}, nil
}

func (s *SourceProxyQueryService) Check(context.Context) check.Response {
	return platformhttp.QueryHealthCheck(s.URL, s.InsecureSkipVerify)
}
