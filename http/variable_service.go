package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	platform "github.com/influxdata/influxdb"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

const (
	variablePath = "/api/v2/variables"
)

// VariableBackend is all services and associated parameters required to construct
// the VariableHandler.
type VariableBackend struct {
	Logger          *zap.Logger
	VariableService platform.VariableService
}

func NewVariableBackend(b *APIBackend) *VariableBackend {
	return &VariableBackend{
		Logger:          b.Logger.With(zap.String("handler", "variable")),
		VariableService: b.VariableService,
	}
}

// VariableHandler is the handler for the variable service
type VariableHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	VariableService platform.VariableService
}

// NewVariableHandler creates a new VariableHandler
func NewVariableHandler(b *VariableBackend) *VariableHandler {
	h := &VariableHandler{
		Router: NewRouter(),
		Logger: b.Logger,

		VariableService: b.VariableService,
	}

	entityPath := fmt.Sprintf("%s/:id", variablePath)

	h.HandlerFunc("GET", variablePath, h.handleGetVariables)
	h.HandlerFunc("POST", variablePath, h.handlePostVariable)
	h.HandlerFunc("GET", entityPath, h.handleGetVariable)
	h.HandlerFunc("PATCH", entityPath, h.handlePatchVariable)
	h.HandlerFunc("PUT", entityPath, h.handlePutVariable)
	h.HandlerFunc("DELETE", entityPath, h.handleDeleteVariable)

	return h
}

type getVariablesResponse struct {
	Variables []variableResponse    `json:"variables"`
	Links     *platform.PagingLinks `json:"links"`
}

func (r getVariablesResponse) ToPlatform() []*platform.Variable {
	variables := make([]*platform.Variable, len(r.Variables))
	for i := range r.Variables {
		variables[i] = r.Variables[i].Variable
	}
	return variables
}

func newGetVariablesResponse(variables []*platform.Variable, f platform.VariableFilter, opts platform.FindOptions) getVariablesResponse {
	num := len(variables)
	resp := getVariablesResponse{
		Variables: make([]variableResponse, 0, num),
		Links:     newPagingLinks(variablePath, opts, f, num),
	}

	for _, variable := range variables {
		resp.Variables = append(resp.Variables, newVariableResponse(variable))
	}

	return resp
}

type getVariablesRequest struct {
	filter platform.VariableFilter
	opts   platform.FindOptions
}

func decodeGetVariablesRequest(ctx context.Context, r *http.Request) (*getVariablesRequest, error) {
	qp := r.URL.Query()
	req := &getVariablesRequest{}

	opts, err := decodeFindOptions(ctx, r)
	if err != nil {
		return nil, err
	}

	req.opts = *opts

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
		EncodeError(ctx, err, w)
		return
	}

	variables, err := h.VariableService.FindVariables(ctx, req.filter, req.opts)
	if err != nil {
		EncodeError(ctx, &platform.Error{
			Code: platform.EInternal,
			Msg:  "could not read variables",
			Err:  err,
		}, w)
		return
	}

	err = encodeResponse(ctx, w, http.StatusOK, newGetVariablesResponse(variables, req.filter, req.opts))
	if err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

func requestVariableID(ctx context.Context) (platform.ID, error) {
	params := httprouter.ParamsFromContext(ctx)
	urlID := params.ByName("id")
	if urlID == "" {
		return platform.InvalidID(), &platform.Error{
			Code: platform.EInvalid,
			Msg:  "url missing id",
		}
	}

	id, err := platform.IDFromString(urlID)
	if err != nil {
		return platform.InvalidID(), &platform.Error{
			Code: platform.EInvalid,
			Msg:  err.Error(),
		}
	}

	return *id, nil
}

func (h *VariableHandler) handleGetVariable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := requestVariableID(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	variable, err := h.VariableService.FindVariableByID(ctx, id)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = encodeResponse(ctx, w, http.StatusOK, newVariableResponse(variable))
	if err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type variableLinks struct {
	Self string `json:"self"`
	Org  string `json:"org"`
}

type variableResponse struct {
	*platform.Variable
	Links variableLinks `json:"links"`
}

func newVariableResponse(m *platform.Variable) variableResponse {
	return variableResponse{
		Variable: m,
		Links: variableLinks{
			Self: fmt.Sprintf("/api/v2/variables/%s", m.ID),
			Org:  fmt.Sprintf("/api/v2/orgs/%s", m.OrganizationID),
		},
	}
}

func (h *VariableHandler) handlePostVariable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostVariableRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = h.VariableService.CreateVariable(ctx, req.variable)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = encodeResponse(ctx, w, http.StatusCreated, newVariableResponse(req.variable))
	if err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type postVariableRequest struct {
	variable *platform.Variable
}

func (r *postVariableRequest) Valid() error {
	return r.variable.Valid()
}

func decodePostVariableRequest(ctx context.Context, r *http.Request) (*postVariableRequest, error) {
	m := &platform.Variable{}

	err := json.NewDecoder(r.Body).Decode(m)
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  err.Error(),
		}
	}

	req := &postVariableRequest{
		variable: m,
	}

	if err := req.Valid(); err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  err.Error(),
		}
	}

	return req, nil
}

func (h *VariableHandler) handlePatchVariable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchVariableRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	variable, err := h.VariableService.UpdateVariable(ctx, req.id, req.variableUpdate)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = encodeResponse(ctx, w, http.StatusOK, newVariableResponse(variable))
	if err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type patchVariableRequest struct {
	id             platform.ID
	variableUpdate *platform.VariableUpdate
}

func (r *patchVariableRequest) Valid() error {
	return r.variableUpdate.Valid()
}

func decodePatchVariableRequest(ctx context.Context, r *http.Request) (*patchVariableRequest, error) {
	u := &platform.VariableUpdate{}

	err := json.NewDecoder(r.Body).Decode(u)
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
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
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  err.Error(),
		}
	}

	return req, nil
}

func (h *VariableHandler) handlePutVariable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePutVariableRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = h.VariableService.ReplaceVariable(ctx, req.variable)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = encodeResponse(ctx, w, http.StatusOK, newVariableResponse(req.variable))
	if err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type putVariableRequest struct {
	variable *platform.Variable
}

func (r *putVariableRequest) Valid() error {
	return r.variable.Valid()
}

func decodePutVariableRequest(ctx context.Context, r *http.Request) (*putVariableRequest, error) {
	m := &platform.Variable{}

	err := json.NewDecoder(r.Body).Decode(m)
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	req := &putVariableRequest{
		variable: m,
	}

	if err := req.Valid(); err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	return req, nil
}

func (h *VariableHandler) handleDeleteVariable(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := requestVariableID(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = h.VariableService.DeleteVariable(ctx, id)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// VariableService is a variable service over HTTP to the influxdb server
type VariableService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// FindVariableByID finds a single variable from the store by its ID
func (s *VariableService) FindVariableByID(ctx context.Context, id platform.ID) (*platform.Variable, error) {
	path := variableIDPath(id)
	url, err := newURL(s.Addr, path)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}

	SetToken(s.Token, req)
	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var mr variableResponse
	if err := json.NewDecoder(resp.Body).Decode(&mr); err != nil {
		return nil, err
	}

	variable := mr.Variable
	return variable, nil
}

// FindVariables returns a list of variables that match filter.
//
// Additional options provide pagination & sorting.
func (s *VariableService) FindVariables(ctx context.Context, filter platform.VariableFilter, opts ...platform.FindOptions) ([]*platform.Variable, error) {
	url, err := newURL(s.Addr, variablePath)
	if err != nil {
		return nil, err
	}

	query := url.Query()
	if filter.OrganizationID != nil {
		query.Add("orgID", filter.OrganizationID.String())
	}
	if filter.Organization != nil {
		query.Add("org", *filter.Organization)
	}
	if filter.ID != nil {
		query.Add("id", filter.ID.String())
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}
	req.URL.RawQuery = query.Encode()
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var ms getVariablesResponse
	if err := json.NewDecoder(resp.Body).Decode(&ms); err != nil {
		return nil, err
	}

	variables := ms.ToPlatform()
	return variables, nil
}

// CreateVariable creates a new variable and assigns it an platform.ID
func (s *VariableService) CreateVariable(ctx context.Context, m *platform.Variable) error {
	if err := m.Valid(); err != nil {
		return &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	url, err := newURL(s.Addr, variablePath)
	if err != nil {
		return err
	}

	octets, err := json.Marshal(m)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return err
	}

	return json.NewDecoder(resp.Body).Decode(m)
}

// UpdateVariable updates a single variable with a changeset
func (s *VariableService) UpdateVariable(ctx context.Context, id platform.ID, update *platform.VariableUpdate) (*platform.Variable, error) {
	u, err := newURL(s.Addr, variableIDPath(id))
	if err != nil {
		return nil, err
	}

	octets, err := json.Marshal(update)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PATCH", u.String(), bytes.NewReader(octets))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var m platform.Variable
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}

	return &m, nil
}

// ReplaceVariable replaces a single variable
func (s *VariableService) ReplaceVariable(ctx context.Context, variable *platform.Variable) error {
	u, err := newURL(s.Addr, variableIDPath(variable.ID))
	if err != nil {
		return err
	}

	octets, err := json.Marshal(variable)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", u.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(&variable); err != nil {
		return err
	}

	return nil
}

// DeleteVariable removes a variable from the store
func (s *VariableService) DeleteVariable(ctx context.Context, id platform.ID) error {
	u, err := newURL(s.Addr, variableIDPath(id))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckError(resp)
}

func variableIDPath(id platform.ID) string {
	return path.Join(variablePath, id.String())
}
