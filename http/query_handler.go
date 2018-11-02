package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/complete"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/flux/plan"
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

	Now                  func() time.Time
	AuthorizationService platform.AuthorizationService
	OrganizationService  platform.OrganizationService
	ProxyQueryService    query.ProxyQueryService
}

// NewFluxHandler returns a new handler at /api/v2/query for flux queries.
func NewFluxHandler() *FluxHandler {
	h := &FluxHandler{
		Router: httprouter.New(),
		Now:    time.Now,
		Logger: zap.NewNop(),
	}

	h.HandlerFunc("POST", fluxPath, h.handlePostQuery)
	h.HandlerFunc("POST", "/api/v2/query/ast", h.postFluxAST)
	h.HandlerFunc("POST", "/api/v2/query/plan", h.postFluxPlan)
	h.HandlerFunc("POST", "/api/v2/query/spec", h.postFluxSpec)
	h.HandlerFunc("GET", "/api/v2/query/suggestions", h.getFluxSuggestions)
	h.HandlerFunc("GET", "/api/v2/query/suggestions/:name", h.getFluxSuggestion)
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

type langRequest struct {
	Query string `json:"query"`
}

type postFluxASTResponse struct {
	AST *ast.Program `json:"ast"`
}

// postFluxAST returns a flux AST for provided flux string
func (h *FluxHandler) postFluxAST(w http.ResponseWriter, r *http.Request) {
	var request langRequest
	ctx := r.Context()

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		EncodeError(ctx, errors.MalformedDataf("invalid json: %v", err), w)
		return
	}

	ast, err := parser.NewAST(request.Query)
	if err != nil {
		EncodeError(ctx, errors.InvalidDataf("invalid AST: %v", err), w)
		return
	}

	res := postFluxASTResponse{
		AST: ast,
	}

	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type postFluxSpecResponse struct {
	Spec *flux.Spec `json:"spec"`
}

// postFluxSpec returns a flux Spec for provided flux string
func (h *FluxHandler) postFluxSpec(w http.ResponseWriter, r *http.Request) {
	var req langRequest
	ctx := r.Context()

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		EncodeError(ctx, errors.MalformedDataf("invalid json: %v", err), w)
		return
	}

	spec, err := flux.Compile(ctx, req.Query, h.Now())
	if err != nil {
		EncodeError(ctx, errors.InvalidDataf("invalid spec: %v", err), w)
		return
	}

	res := postFluxSpecResponse{
		Spec: spec,
	}

	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type fluxPlan struct {
	Roots     []plan.PlanNode         `json:"roots"`
	Resources flux.ResourceManagement `json:"resources"`
	Now       time.Time               `json:"now"`
}

func newFluxPlan(p *plan.PlanSpec) *fluxPlan {
	res := &fluxPlan{
		Roots:     make([]plan.PlanNode, 0, len(p.Roots)),
		Resources: p.Resources,
		Now:       p.Now,
	}

	for node := range p.Roots {
		res.Roots = append(res.Roots, node)
	}

	return res
}

type postFluxPlanResponse struct {
	Spec     *flux.Spec `json:"spec"`
	Logical  *fluxPlan  `json:"logical"`
	Physical *fluxPlan  `json:"physical"`
}

type postPlanRequest struct {
	Query string     `json:"query,omitempty"`
	Spec  *flux.Spec `json:"spec,omityempty"`
}

// Valid check if the plan request has a query or spec defined, but not both.
func (p *postPlanRequest) Valid() error {
	if p.Query == "" && p.Spec == nil {
		return errors.MalformedDataf("query or spec required")
	}

	if p.Query != "" && p.Spec != nil {
		return errors.MalformedDataf("cannot request both query and spec")
	}
	return nil
}

// postFluxPlan returns a flux plan for provided flux string
func (h *FluxHandler) postFluxPlan(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePostPlanRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	var spec *flux.Spec = req.Spec
	if req.Query != "" {
		spec, err = flux.Compile(ctx, req.Query, h.Now())
		if err != nil {
			EncodeError(ctx, errors.InvalidDataf("invalid spec: %v", err), w)
			return
		}
	}

	logical := plan.NewLogicalPlanner()
	log, err := logical.Plan(spec)
	if err != nil {
		EncodeError(ctx, errors.InvalidDataf("invalid logical plan: %v", err), w)
		return
	}

	physical := plan.NewPhysicalPlanner()
	phys, err := physical.Plan(log)
	if err != nil {
		EncodeError(ctx, errors.InvalidDataf("invalid physical plan: %v", err), w)
		return
	}

	res := postFluxPlanResponse{
		Logical:  newFluxPlan(log),
		Physical: newFluxPlan(phys),
		Spec:     spec,
	}

	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

func decodePostPlanRequest(ctx context.Context, r *http.Request) (*postPlanRequest, error) {
	req := &postPlanRequest{}
	err := json.NewDecoder(r.Body).Decode(req)
	if err != nil {
		return nil, errors.MalformedDataf("invalid json: %v", err)
	}

	return req, req.Valid()
}

// fluxParams contain flux funciton parameters as defined by the semantic graph
type fluxParams map[string]string

// suggestionResponse provides the parameters available for a given Flux function
type suggestionResponse struct {
	Name   string     `json:"name"`
	Params fluxParams `json:"params"`
}

// suggestionsResponse provides a list of available Flux functions
type suggestionsResponse struct {
	Functions []suggestionResponse `json:"funcs"`
}

// getFluxSuggestions returns a list of available Flux functions for the Flux Builder
func (h *FluxHandler) getFluxSuggestions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	completer := complete.DefaultCompleter()
	names := completer.FunctionNames()
	var functions []suggestionResponse
	for _, name := range names {
		suggestion, err := completer.FunctionSuggestion(name)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		filteredParams := make(fluxParams)
		for key, value := range suggestion.Params {
			if key == "table" {
				continue
			}

			filteredParams[key] = value
		}

		functions = append(functions, suggestionResponse{
			Name:   name,
			Params: filteredParams,
		})
	}
	res := suggestionsResponse{Functions: functions}

	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

// getFluxSuggestion returns the function parameters for the requested function
func (h *FluxHandler) getFluxSuggestion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := httprouter.ParamsFromContext(ctx).ByName("name")
	completer := complete.DefaultCompleter()

	suggestion, err := completer.FunctionSuggestion(name)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	res := suggestionResponse{Name: name, Params: suggestion.Params}
	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		EncodeError(ctx, err, w)
		return
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
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// Query runs a flux query against a influx server and sends the results to the io.Writer.
// Will use the token from the context over the token within the service struct.
func (s *FluxService) Query(ctx context.Context, w io.Writer, r *query.ProxyRequest) (int64, error) {
	u, err := newURL(s.Addr, fluxPath)
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

	SetToken(s.Token, hreq)

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
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// Query runs a flux query against a influx server and decodes the result
func (s *FluxQueryService) Query(ctx context.Context, r *query.Request) (flux.ResultIterator, error) {
	u, err := newURL(s.Addr, fluxPath)
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

	SetToken(s.Token, hreq)

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
