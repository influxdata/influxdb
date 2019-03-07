package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/query/influxql"
)

type SourceProxyQueryService struct {
	Addr               string
	InsecureSkipVerify bool
	platform.SourceFields
}

func (s *SourceProxyQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
	switch req.Request.Compiler.CompilerType() {
	case influxql.CompilerType:
		return s.queryInfluxQL(ctx, w, req)
	case lang.FluxCompilerType:
		return s.queryFlux(ctx, w, req)
	}
	return flux.Statistics{}, fmt.Errorf("compiler type not supported")
}

func (s *SourceProxyQueryService) queryFlux(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
	u, err := newURL(s.Addr, "/api/v2/query")
	if err != nil {
		return flux.Statistics{}, err
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(req); err != nil {
		return flux.Statistics{}, err
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return flux.Statistics{}, err
	}
	hreq.Header.Set("Authorization", fmt.Sprintf("Token %s", s.Token))
	hreq.Header.Set("Content-Type", "application/json")
	hreq = hreq.WithContext(ctx)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return flux.Statistics{}, err
	}
	defer resp.Body.Close()
	if err := CheckError(resp); err != nil {
		return flux.Statistics{}, err
	}

	if _, err = io.Copy(w, resp.Body); err != nil {
		return flux.Statistics{}, err
	}

	return flux.Statistics{}, nil
}

func (s *SourceProxyQueryService) queryInfluxQL(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
	compiler, ok := req.Request.Compiler.(*influxql.Compiler)

	if !ok {
		return flux.Statistics{}, fmt.Errorf("compiler is not of type 'influxql'")
	}

	u, err := newURL(s.Addr, "/query")
	if err != nil {
		return flux.Statistics{}, err
	}

	body := url.Values{}
	body.Add("db", compiler.DB)
	body.Add("org", compiler.Cluster)
	body.Add("q", compiler.Query)
	body.Add("rp", compiler.RP)
	hreq, err := http.NewRequest("POST", u.String(), strings.NewReader(body.Encode()))
	if err != nil {
		return flux.Statistics{}, err
	}
	hreq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	hreq.Header.Set("Authorization", fmt.Sprintf("Token %s", s.Token))
	hreq = hreq.WithContext(ctx)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return flux.Statistics{}, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return flux.Statistics{}, err
	}

	if _, err = io.Copy(w, resp.Body); err != nil {
		return flux.Statistics{}, err
	}

	return flux.Statistics{}, nil
}
