package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"go.uber.org/zap"
)

const (
	prefixVariables = "/api/v2/variables"
)

// VariableBackend is all services and associated parameters required to construct
// the VariableHandler.
type VariableBackend struct {
	errors.HTTPErrorHandler
	log             *zap.Logger
	VariableService influxdb.VariableService
	LabelService    influxdb.LabelService
}

// NewVariableBackend creates a backend used by the variable handler.
func NewVariableBackend(log *zap.Logger, b *APIBackend) *VariableBackend {
	return &VariableBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,
		VariableService:  b.VariableService,
		LabelService:     b.LabelService,
	}
}

// VariableHandler is the handler for the variable service
type VariableHandler struct {
	*httprouter.Router

	errors.HTTPErrorHandler
	log *zap.Logger

	VariableService influxdb.VariableService
	LabelService    influxdb.LabelService
}

// NewVariableHandler creates a new VariableHandler
func NewVariableHandler(log *zap.Logger, b *VariableBackend) *VariableHandler {
	h := &VariableHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		VariableService: b.VariableService,
		LabelService:    b.LabelService,
	}

	entityPath := fmt.Sprintf("%s/:id", prefixVariables)
	entityLabelsPath := fmt.Sprintf("%s/labels", entityPath)
	entityLabelsIDPath := fmt.Sprintf("%s/:lid", entityLabelsPath)

	h.HandlerFunc("GET", prefixVariables, h.handleGetVariables)
	h.HandlerFunc("POST", prefixVariables, h.handlePostVariable)
	h.HandlerFunc("GET", entityPath, h.handleGetVariable)
	h.HandlerFunc("PATCH", entityPath, h.handlePatchVariable)
	h.HandlerFunc("PUT", entityPath, h.handlePutVariable)
	h.HandlerFunc("DELETE", entityPath, h.handleDeleteVariable)

	labelBackend := &LabelBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              b.log.With(zap.String("handler", "label")),
		LabelService:     b.LabelService,
		ResourceType:     influxdb.VariablesResourceType,
	}
	h.HandlerFunc("GET", entityLabelsPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", entityLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", entityLabelsIDPath, newDeleteLabelHandler(labelBackend))

	return h
}

type getVariablesResponse struct {
	Variables []variableResponse    `json:"variables"`
	Links     *influxdb.PagingLinks `json:"links"`
}

func (r getVariablesResponse) Toinfluxdb() []*influxdb.Variable {
	variables := make([]*influxdb.Variable, len(r.Variables))
	for i := range r.Variables {
		variables[i] = r.Variables[i].Variable
	}
	return variables
}

func newGetVariablesResponse(ctx context.Context, variables []*influxdb.Variable, f influxdb.VariableFilter, opts influxdb.FindOptions, labelService influxdb.LabelService) getVariablesResponse {
	num := len(variables)
	resp := getVariablesResponse{
		Variables: make([]variableResponse, 0, num),
		Links:     influxdb.NewPagingLinks(prefixVariables, opts, f, num),
	}

	for _, variable := range variables {
		labels, _ := labelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: variable.ID, ResourceType: influxdb.VariablesResourceType})
		resp.Variables = append(resp.Variables, newVariableResponse(variable, labels))
	}

	return resp
}

type getVariablesRequest struct {
	filter influxdb.VariableFilter
	opts   influxdb.FindOptions
}

func decodeGetVariablesRequest(ctx context.Context, r *http.Request) (*getVariablesRequest, error) {
	opts, err := influxdb.DecodeFindOptions(r)
	if err != nil {
		return nil, err
	}

	req := &getVariablesRequest{
		opts: *opts,
	}
	qp := r.URL.Query()
	if orgID := qp.Get("orgID"); orgID != "" {
		id, err := platform.IDFromString(orgID)
		if err != nil {
			return nil, err
		}
		req.filter.OrganizationID = id
	}

	if org := qp.Get("org"); org != "" {
		req.filter.Organization = &org
	}

	return req, nil
}

func (h *VariableHandler) handleGetVariables(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetVariablesRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	variables, err := h.VariableService.FindVariables(ctx, req.filter, req.opts)
	if err != nil {
		h.HandleHTTPError(ctx, &errors.Error{
			Code: errors.EInternal,
			Msg:  "could not read variables",
			Err:  err,
		}, w)
		return
	}
	h.log.Debug("Variables retrieved", zap.String("vars", fmt.Sprint(variables)))
	err = encodeResponse(ctx, w, http.StatusOK, newGetVariablesResponse(ctx, variables, req.filter, req.opts, h.LabelService))
	if err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

func requestVariableID(ctx context.Context) (platform.ID, error) {
	params := httprouter.ParamsFromContext(ctx)
	urlID := params.ByName("id")
	if urlID == "" {
		return platform.InvalidID(), &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		}
	}

	id, err := platform.IDFromString(urlID)
	if err != nil {
		return platform.InvalidID(), err
	}

	return *id, nil
}

func (h *VariableHandler) handleGetVariable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, err := requestVariableID(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	variable, err := h.VariableService.FindVariableByID(ctx, id)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: variable.ID, ResourceType: influxdb.VariablesResourceType})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Variable retrieved", zap.String("var", fmt.Sprint(variable)))
	err = encodeResponse(ctx, w, http.StatusOK, newVariableResponse(variable, labels))
	if err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type variableLinks struct {
	Self   string `json:"self"`
	Labels string `json:"labels"`
	Org    string `json:"org"`
}

type variableResponse struct {
	*influxdb.Variable
	Labels []influxdb.Label `json:"labels"`
	Links  variableLinks    `json:"links"`
}

func newVariableResponse(m *influxdb.Variable, labels []*influxdb.Label) variableResponse {
	res := variableResponse{
		Variable: m,
		Labels:   []influxdb.Label{},
		Links: variableLinks{
			Self:   fmt.Sprintf("/api/v2/variables/%s", m.ID),
			Labels: fmt.Sprintf("/api/v2/variables/%s/labels", m.ID),
			Org:    fmt.Sprintf("/api/v2/orgs/%s", m.OrganizationID),
		},
	}

	for _, l := range labels {
		res.Labels = append(res.Labels, *l)
	}
	return res
}

func (h *VariableHandler) handlePostVariable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePostVariableRequest(r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	err = h.VariableService.CreateVariable(ctx, req.variable)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Variable created", zap.String("var", fmt.Sprint(req.variable)))
	if err := encodeResponse(ctx, w, http.StatusCreated, newVariableResponse(req.variable, []*influxdb.Label{})); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type postVariableRequest struct {
	variable *influxdb.Variable
}

func (r *postVariableRequest) Valid() error {
	return r.variable.Valid()
}

func decodePostVariableRequest(r *http.Request) (*postVariableRequest, error) {
	m := &influxdb.Variable{}

	err := json.NewDecoder(r.Body).Decode(m)
	if err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  err.Error(),
		}
	}

	req := &postVariableRequest{
		variable: m,
	}

	if err := req.Valid(); err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  err.Error(),
		}
	}

	return req, nil
}

func (h *VariableHandler) handlePatchVariable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePatchVariableRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	variable, err := h.VariableService.UpdateVariable(ctx, req.id, req.variableUpdate)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: variable.ID, ResourceType: influxdb.VariablesResourceType})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Variable updated", zap.String("var", fmt.Sprint(variable)))
	err = encodeResponse(ctx, w, http.StatusOK, newVariableResponse(variable, labels))
	if err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type patchVariableRequest struct {
	id             platform.ID
	variableUpdate *influxdb.VariableUpdate
}

func (r *patchVariableRequest) Valid() error {
	return r.variableUpdate.Valid()
}

func decodePatchVariableRequest(ctx context.Context, r *http.Request) (*patchVariableRequest, error) {
	u := &influxdb.VariableUpdate{}

	err := json.NewDecoder(r.Body).Decode(u)
	if err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  err.Error(),
		}
	}

	id, err := requestVariableID(ctx)
	if err != nil {
		return nil, err
	}

	req := &patchVariableRequest{
		id:             id,
		variableUpdate: u,
	}

	if err := req.Valid(); err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  err.Error(),
		}
	}

	return req, nil
}

func (h *VariableHandler) handlePutVariable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePutVariableRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	v := req.variable
	v.ID = req.id

	err = h.VariableService.ReplaceVariable(ctx, v)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: v.ID, ResourceType: influxdb.VariablesResourceType})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Variable replaced", zap.String("var", fmt.Sprint(req.variable)))
	err = encodeResponse(ctx, w, http.StatusOK, newVariableResponse(req.variable, labels))
	if err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type putVariableRequest struct {
	id       platform.ID
	variable *influxdb.Variable
}

func (r *putVariableRequest) Valid() error {
	return r.variable.Valid()
}

func decodePutVariableRequest(ctx context.Context, r *http.Request) (*putVariableRequest, error) {
	m := &influxdb.Variable{}

	err := json.NewDecoder(r.Body).Decode(m)
	if err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		}
	}

	id, err := requestVariableID(ctx)
	if err != nil {
		return nil, err
	}

	req := &putVariableRequest{
		variable: m,
		id:       id,
	}

	if err := req.Valid(); err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		}
	}

	return req, nil
}

func (h *VariableHandler) handleDeleteVariable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, err := requestVariableID(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	err = h.VariableService.DeleteVariable(ctx, id)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Variable deleted", zap.String("variableID", fmt.Sprint(id)))
	w.WriteHeader(http.StatusNoContent)
}

// VariableService is a variable service over HTTP to the influxdb server
type VariableService struct {
	Client *httpc.Client
}

// FindVariableByID finds a single variable from the store by its ID
func (s *VariableService) FindVariableByID(ctx context.Context, id platform.ID) (*influxdb.Variable, error) {
	var mr variableResponse
	err := s.Client.
		Get(prefixVariables, id.String()).
		DecodeJSON(&mr).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return mr.Variable, nil
}

// FindVariables returns a list of variables that match filter.
// Additional options provide pagination & sorting.
func (s *VariableService) FindVariables(ctx context.Context, filter influxdb.VariableFilter, opts ...influxdb.FindOptions) ([]*influxdb.Variable, error) {
	params := influxdb.FindOptionParams(opts...)
	if filter.OrganizationID != nil {
		params = append(params, [2]string{"orgID", filter.OrganizationID.String()})
	}
	if filter.Organization != nil {
		params = append(params, [2]string{"org", *filter.Organization})
	}
	if filter.ID != nil {
		params = append(params, [2]string{"id", filter.ID.String()})
	}

	var ms getVariablesResponse
	err := s.Client.
		Get(prefixVariables).
		QueryParams(params...).
		DecodeJSON(&ms).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return ms.Toinfluxdb(), nil
}

// CreateVariable creates a new variable and assigns it an influxdb.ID
func (s *VariableService) CreateVariable(ctx context.Context, m *influxdb.Variable) error {
	m.Name = strings.TrimSpace(m.Name)
	if err := m.Valid(); err != nil {
		return &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		}
	}

	return s.Client.
		PostJSON(m, prefixVariables).
		DecodeJSON(m).
		Do(ctx)
}

// UpdateVariable updates a single variable with a changeset
func (s *VariableService) UpdateVariable(ctx context.Context, id platform.ID, update *influxdb.VariableUpdate) (*influxdb.Variable, error) {
	var m influxdb.Variable
	err := s.Client.
		PatchJSON(update, prefixVariables, id.String()).
		DecodeJSON(&m).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

// ReplaceVariable replaces a single variable
func (s *VariableService) ReplaceVariable(ctx context.Context, variable *influxdb.Variable) error {
	return s.Client.
		PutJSON(variable, prefixVariables, variable.ID.String()).
		DecodeJSON(variable).
		Do(ctx)
}

// DeleteVariable removes a variable from the store
func (s *VariableService) DeleteVariable(ctx context.Context, id platform.ID) error {
	return s.Client.
		Delete(prefixVariables, id.String()).
		Do(ctx)
}
