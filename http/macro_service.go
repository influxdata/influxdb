package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	platform "github.com/influxdata/influxdb"
	kerrors "github.com/influxdata/influxdb/kit/errors"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

const (
	macroPath = "/api/v2/macros"
)

// MacroHandler is the handler for the macro service
type MacroHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	MacroService platform.MacroService
}

// NewMacroHandler creates a new MacroHandler
func NewMacroHandler() *MacroHandler {
	h := &MacroHandler{
		Router: NewRouter(),
		Logger: zap.NewNop(),
	}

	entityPath := fmt.Sprintf("%s/:id", macroPath)

	h.HandlerFunc("GET", macroPath, h.handleGetMacros)
	h.HandlerFunc("POST", macroPath, h.handlePostMacro)
	h.HandlerFunc("GET", entityPath, h.handleGetMacro)
	h.HandlerFunc("PATCH", entityPath, h.handlePatchMacro)
	h.HandlerFunc("PUT", entityPath, h.handlePutMacro)
	h.HandlerFunc("DELETE", entityPath, h.handleDeleteMacro)

	return h
}

type getMacrosResponse struct {
	Macros []macroResponse       `json:"macros"`
	Links  *platform.PagingLinks `json:"links"`
}

func (r getMacrosResponse) ToPlatform() []*platform.Macro {
	macros := make([]*platform.Macro, len(r.Macros))
	for i := range r.Macros {
		macros[i] = r.Macros[i].Macro
	}
	return macros
}

func newGetMacrosResponse(macros []*platform.Macro, f platform.MacroFilter, opts platform.FindOptions) getMacrosResponse {
	num := len(macros)
	resp := getMacrosResponse{
		Macros: make([]macroResponse, 0, num),
		Links:  newPagingLinks(macroPath, opts, f, num),
	}

	for _, macro := range macros {
		resp.Macros = append(resp.Macros, newMacroResponse(macro))
	}

	return resp
}

type getMacrosRequest struct {
	filter platform.MacroFilter
	opts   platform.FindOptions
}

func decodeGetMacrosRequest(ctx context.Context, r *http.Request) (*getMacrosRequest, error) {
	qp := r.URL.Query()
	req := &getMacrosRequest{}

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

func (h *MacroHandler) handleGetMacros(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetMacrosRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	macros, err := h.MacroService.FindMacros(ctx, req.filter, req.opts)
	if err != nil {
		EncodeError(ctx, kerrors.InternalErrorf("could not read macros: %v", err), w)
		return
	}

	err = encodeResponse(ctx, w, http.StatusOK, newGetMacrosResponse(macros, req.filter, req.opts))
	if err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

func requestMacroID(ctx context.Context) (platform.ID, error) {
	params := httprouter.ParamsFromContext(ctx)
	urlID := params.ByName("id")
	if urlID == "" {
		return platform.InvalidID(), kerrors.InvalidDataf("url missing id")
	}

	id, err := platform.IDFromString(urlID)
	if err != nil {
		return platform.InvalidID(), &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	return *id, nil
}

func (h *MacroHandler) handleGetMacro(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := requestMacroID(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	macro, err := h.MacroService.FindMacroByID(ctx, id)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = encodeResponse(ctx, w, http.StatusOK, newMacroResponse(macro))
	if err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type macroLinks struct {
	Self string `json:"self"`
	Org  string `json:"org"`
}

type macroResponse struct {
	*platform.Macro
	Links macroLinks `json:"links"`
}

func newMacroResponse(m *platform.Macro) macroResponse {
	return macroResponse{
		Macro: m,
		Links: macroLinks{
			Self: fmt.Sprintf("/api/v2/macros/%s", m.ID),
			Org:  fmt.Sprintf("/api/v2/orgs/%s", m.OrganizationID),
		},
	}
}

func (h *MacroHandler) handlePostMacro(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostMacroRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = h.MacroService.CreateMacro(ctx, req.macro)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = encodeResponse(ctx, w, http.StatusCreated, newMacroResponse(req.macro))
	if err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type postMacroRequest struct {
	macro *platform.Macro
}

func (r *postMacroRequest) Valid() error {
	return r.macro.Valid()
}

func decodePostMacroRequest(ctx context.Context, r *http.Request) (*postMacroRequest, error) {
	m := &platform.Macro{}

	err := json.NewDecoder(r.Body).Decode(m)
	if err != nil {
		return nil, kerrors.MalformedDataf(err.Error())
	}

	req := &postMacroRequest{
		macro: m,
	}

	if err := req.Valid(); err != nil {
		return nil, kerrors.InvalidDataf(err.Error())
	}

	return req, nil
}

func (h *MacroHandler) handlePatchMacro(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchMacroRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	macro, err := h.MacroService.UpdateMacro(ctx, req.id, req.macroUpdate)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = encodeResponse(ctx, w, http.StatusOK, newMacroResponse(macro))
	if err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type patchMacroRequest struct {
	id          platform.ID
	macroUpdate *platform.MacroUpdate
}

func (r *patchMacroRequest) Valid() error {
	return r.macroUpdate.Valid()
}

func decodePatchMacroRequest(ctx context.Context, r *http.Request) (*patchMacroRequest, error) {
	u := &platform.MacroUpdate{}

	err := json.NewDecoder(r.Body).Decode(u)
	if err != nil {
		return nil, kerrors.MalformedDataf(err.Error())
	}

	id, err := requestMacroID(ctx)
	if err != nil {
		return nil, err
	}

	req := &patchMacroRequest{
		id:          id,
		macroUpdate: u,
	}

	if err := req.Valid(); err != nil {
		return nil, kerrors.InvalidDataf(err.Error())
	}

	return req, nil
}

func (h *MacroHandler) handlePutMacro(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePutMacroRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = h.MacroService.ReplaceMacro(ctx, req.macro)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = encodeResponse(ctx, w, http.StatusOK, newMacroResponse(req.macro))
	if err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type putMacroRequest struct {
	macro *platform.Macro
}

func (r *putMacroRequest) Valid() error {
	return r.macro.Valid()
}

func decodePutMacroRequest(ctx context.Context, r *http.Request) (*putMacroRequest, error) {
	m := &platform.Macro{}

	err := json.NewDecoder(r.Body).Decode(m)
	if err != nil {
		return nil, kerrors.MalformedDataf(err.Error())
	}

	req := &putMacroRequest{
		macro: m,
	}

	if err := req.Valid(); err != nil {
		return nil, kerrors.InvalidDataf(err.Error())
	}

	return req, nil
}

func (h *MacroHandler) handleDeleteMacro(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := requestMacroID(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	err = h.MacroService.DeleteMacro(ctx, id)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// MacroService is a macro service over HTTP to the influxdb server
type MacroService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// FindMacroByID finds a single macro from the store by its ID
func (s *MacroService) FindMacroByID(ctx context.Context, id platform.ID) (*platform.Macro, error) {
	path := macroIDPath(id)
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

	if err := CheckError(resp, true); err != nil {
		return nil, err
	}

	var mr macroResponse
	if err := json.NewDecoder(resp.Body).Decode(&mr); err != nil {
		return nil, err
	}

	macro := mr.Macro
	return macro, nil
}

// FindMacros returns a list of macros that match filter.
//
// Additional options provide pagination & sorting.
func (s *MacroService) FindMacros(ctx context.Context, filter platform.MacroFilter, opts ...platform.FindOptions) ([]*platform.Macro, error) {
	url, err := newURL(s.Addr, macroPath)
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

	if err := CheckError(resp, true); err != nil {
		return nil, err
	}

	var ms getMacrosResponse
	if err := json.NewDecoder(resp.Body).Decode(&ms); err != nil {
		return nil, err
	}

	macros := ms.ToPlatform()
	return macros, nil
}

// CreateMacro creates a new macro and assigns it an platform.ID
func (s *MacroService) CreateMacro(ctx context.Context, m *platform.Macro) error {
	if err := m.Valid(); err != nil {
		return kerrors.InvalidDataf(err.Error())
	}

	url, err := newURL(s.Addr, macroPath)
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

	if err := CheckError(resp, true); err != nil {
		return err
	}

	return json.NewDecoder(resp.Body).Decode(m)
}

// UpdateMacro updates a single macro with a changeset
func (s *MacroService) UpdateMacro(ctx context.Context, id platform.ID, update *platform.MacroUpdate) (*platform.Macro, error) {
	u, err := newURL(s.Addr, macroIDPath(id))
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

	if err := CheckError(resp, true); err != nil {
		return nil, err
	}

	var m platform.Macro
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}

	return &m, nil
}

// ReplaceMacro replaces a single macro
func (s *MacroService) ReplaceMacro(ctx context.Context, macro *platform.Macro) error {
	u, err := newURL(s.Addr, macroIDPath(macro.ID))
	if err != nil {
		return err
	}

	octets, err := json.Marshal(macro)
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

	if err := CheckError(resp, true); err != nil {
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(&macro); err != nil {
		return err
	}

	return nil
}

// DeleteMacro removes a macro from the store
func (s *MacroService) DeleteMacro(ctx context.Context, id platform.ID) error {
	u, err := newURL(s.Addr, macroIDPath(id))
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

	return CheckError(resp, true)
}

func macroIDPath(id platform.ID) string {
	return path.Join(macroPath, id.String())
}
