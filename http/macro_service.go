package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
	"github.com/julienschmidt/httprouter"
)

// MacroHandler is the handler for the macro service
type MacroHandler struct {
	*httprouter.Router

	MacroService platform.MacroService
}

// NewMacroHandler creates a new MacroHandler
func NewMacroHandler() *MacroHandler {
	h := &MacroHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("GET", "/v2/macros", h.handleGetMacros)
	h.HandlerFunc("POST", "/v2/macros", h.handlePostMacro)
	h.HandlerFunc("GET", "/v2/macros/:id", h.handleGetMacro)
	h.HandlerFunc("PATCH", "/v2/macros/:id", h.handlePatchMacro)
	h.HandlerFunc("DELETE", "/v2/macros/:id", h.handleDeleteMacro)

	return h
}

type macrosLinks struct {
	Self string `json:"self"`
}

type getMacrosResponse struct {
	Macros []macroResponse `json:"macros"`
	Links  macrosLinks     `json:"links"`
}

func newGetMacrosResponse(macros []*platform.Macro) getMacrosResponse {
	resp := getMacrosResponse{
		Macros: make([]macroResponse, 0, len(macros)),
		Links: macrosLinks{
			Self: "/v2/macros",
		},
	}

	for _, macro := range macros {
		resp.Macros = append(resp.Macros, newMacroResponse(macro))
	}

	return resp
}

func (h *MacroHandler) handleGetMacros(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	macros, err := h.MacroService.FindMacros(ctx)
	if err != nil {
		EncodeError(ctx, kerrors.InternalErrorf("could not read macros: %v", err), w)
		return
	}

	err = encodeResponse(ctx, w, http.StatusOK, newGetMacrosResponse(macros))
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

func requestMacroID(ctx context.Context) (platform.ID, error) {
	params := httprouter.ParamsFromContext(ctx)
	urlID := params.ByName("id")
	if urlID == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	id, err := platform.IDFromString(urlID)
	return *id, err
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
		EncodeError(ctx, err, w)
		return
	}
}

type macroLinks struct {
	Self string `json:"self"`
}

type macroResponse struct {
	*platform.Macro
	Links macroLinks `json:"links"`
}

func newMacroResponse(m *platform.Macro) macroResponse {
	return macroResponse{
		Macro: m,
		Links: macroLinks{
			Self: fmt.Sprintf("/v2/macros/%s", m.ID),
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
		EncodeError(ctx, err, w)
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
		EncodeError(ctx, err, w)
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

func (h *MacroHandler) handleDeleteMacro(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := requestMacroID(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
	}

	err = h.MacroService.DeleteMacro(ctx, id)
	if err != nil {
		EncodeError(ctx, err, w)
	}

	w.WriteHeader(http.StatusNoContent)
}
