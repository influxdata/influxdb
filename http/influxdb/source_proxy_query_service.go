package influxdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/influxdata/platform"
	platformhttp "github.com/influxdata/platform/http"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/csv"
	"github.com/influxdata/platform/query/influxql"
)

type SourceProxyQueryService struct {
	InsecureSkipVerify bool
	URL                string
	OrganizationID     platform.ID
	platform.SourceFields
	platform.V1SourceFields
}

func (s *SourceProxyQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (int64, error) {
	switch req.Request.Compiler.CompilerType() {
	case influxql.CompilerType:
		return s.influxQuery(ctx, w, req)
	case query.FluxCompilerType:
		return s.fluxQuery(ctx, w, req)
	}

	return 0, fmt.Errorf("compiler type not supported")
}

func (s *SourceProxyQueryService) fluxQuery(ctx context.Context, w io.Writer, req *query.ProxyRequest) (int64, error) {
	if len(s.FluxURL) == 0 {
		return 0, fmt.Errorf("fluxURL from source cannot be empty if the compiler type is flux")
	}

	request := struct {
		Spec    *query.Spec   `json:"spec"`
		Query   string        `json:"query"`
		Type    string        `json:"type"`
		Dialect query.Dialect `json:"dialect"`
	}{}

	switch c := req.Request.Compiler.(type) {
	case query.FluxCompiler:
		request.Query = c.Query
		request.Type = query.FluxCompilerType
	case query.SpecCompiler:
		request.Spec = c.Spec
		request.Type = query.SpecCompilerType
	default:
		return 0, fmt.Errorf("compiler type not supported: %s", c.CompilerType())
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

	u, err := newURL(s.FluxURL, "/query")
	if err != nil {
		return 0, err
	}

	qp := u.Query()
	qp.Set("organizationID", req.Request.OrganizationID.String())
	u.RawQuery = qp.Encode()

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(request); err != nil {
		return 0, err
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return 0, err
	}
	hreq.Header.Set("Authorization", s.Token)
	hreq.Header.Set("Content-Type", "application/json")
	hreq = hreq.WithContext(ctx)

	hc := newTraceClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if err := platformhttp.CheckError(resp); err != nil {
		return 0, err
	}
	return io.Copy(w, resp.Body)
}

func (s *SourceProxyQueryService) influxQuery(ctx context.Context, w io.Writer, req *query.ProxyRequest) (int64, error) {
	if len(s.URL) == 0 {
		return 0, fmt.Errorf("URL from source cannot be empty if the compiler type is influxql")
	}

	u, err := newURL(s.URL, "/query")
	if err != nil {
		return 0, err
	}

	hreq, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return 0, err
	}

	// TODO(fntlnz): configure authentication methods username/password and stuff
	hreq = hreq.WithContext(ctx)

	params := hreq.URL.Query()
	compiler, ok := req.Request.Compiler.(*influxql.Compiler)
	if !ok {
		return 0, fmt.Errorf("passed compiler is not of type 'influxql'")
	}
	params.Set("q", compiler.Query)
	params.Set("db", compiler.DB)
	params.Set("rp", compiler.RP)

	hreq.URL.RawQuery = params.Encode()

	hc := newTraceClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if err := platformhttp.CheckError(resp); err != nil {
		return 0, err
	}
	return io.Copy(w, resp.Body)
}
