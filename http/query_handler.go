package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/platform"
	pcontext "github.com/influxdata/platform/context"
	"github.com/influxdata/platform/kit/errors"
	"github.com/influxdata/platform/query"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	fluxPath = "/api/v2/query"
)

// FluxHandler implements handling flux queries.
type FluxHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	AuthorizationService platform.AuthorizationService
	OrganizationService  platform.OrganizationService
	ProxyQueryService    query.ProxyQueryService
}

// NewFluxHandler returns a new handler at /api/v2/query for flux queries.
func NewFluxHandler() *FluxHandler {
	h := &FluxHandler{
		Router: httprouter.New(),
		Logger: zap.NewNop(),
	}

	h.HandlerFunc("POST", fluxPath, h.handlePostQuery)
	return h
}

func (h *FluxHandler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	a, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	auth, err := h.AuthorizationService.FindAuthorizationByID(ctx, a.Identifier())
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if !auth.IsActive() {
		EncodeError(ctx, errors.Forbiddenf("insufficient permissions for query"), w)
		return
	}

	req, err := decodeProxyQueryRequest(ctx, r, auth, h.OrganizationService)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	hd, ok := req.Dialect.(HTTPDialect)
	if !ok {
		EncodeError(ctx, fmt.Errorf("unsupported dialect over HTTP %T", req.Dialect), w)
		return
	}
	hd.SetHeaders(w)

	n, err := h.ProxyQueryService.Query(ctx, w, req)
	if err != nil {
		if n == 0 {
			// Only record the error headers IFF nothing has been written to w.
			EncodeError(ctx, err, w)
			return
		}
		h.Logger.Info("Error writing response to client",
			zap.String("handler", "flux"),
			zap.Error(err),
		)
	}
}

// PrometheusCollectors satisifies the prom.PrometheusCollector interface.
func (h *FluxHandler) PrometheusCollectors() []prometheus.Collector {
	// TODO: gather and return relevant metrics.
	return nil
}

var _ query.ProxyQueryService = (*FluxService)(nil)

// FluxService connects to Influx via HTTP using tokens to run queries.
type FluxService struct {
	URL                string
	Token              string
	InsecureSkipVerify bool
}

// Query runs a flux query against a influx server and sends the results to the io.Writer.
// Will use the token from the context over the token within the service struct.
func (s *FluxService) Query(ctx context.Context, w io.Writer, r *query.ProxyRequest) (int64, error) {
	u, err := newURL(s.URL, fluxPath)
	if err != nil {
		return 0, err
	}

	qreq, err := QueryRequestFromProxyRequest(r)
	if err != nil {
		return 0, err
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(qreq); err != nil {
		return 0, err
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return 0, err
	}

	tok, err := pcontext.GetToken(ctx)
	if err != nil {
		tok = s.Token
	}
	SetToken(tok, hreq)

	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/csv")
	hreq = hreq.WithContext(ctx)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return 0, err
	}
	return io.Copy(w, resp.Body)
}

var _ query.QueryService = (*FluxQueryService)(nil)

// FluxQueryService implements query.QueryService by making HTTP requests to the /api/v2/query API endpoint.
type FluxQueryService struct {
	URL                string
	Token              string
	InsecureSkipVerify bool
}

// Query runs a flux query against a influx server and decodes the result
func (s *FluxQueryService) Query(ctx context.Context, r *query.Request) (flux.ResultIterator, error) {
	u, err := newURL(s.URL, fluxPath)
	if err != nil {
		return nil, err
	}
	params := url.Values{}
	params.Set(OrgID, r.OrganizationID.String())
	u.RawQuery = params.Encode()

	preq := &query.ProxyRequest{
		Request: *r,
		Dialect: csv.DefaultDialect(),
	}
	qreq, err := QueryRequestFromProxyRequest(preq)
	if err != nil {
		return nil, err
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(qreq); err != nil {
		return nil, err
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return nil, err
	}

	tok, err := pcontext.GetToken(ctx)
	if err != nil {
		tok = s.Token
	}
	SetToken(tok, hreq)

	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/csv")
	hreq = hreq.WithContext(ctx)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return nil, err
	}

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	decoder := csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
	return decoder.Decode(resp.Body)
}
