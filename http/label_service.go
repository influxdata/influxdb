package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	plat "github.com/influxdata/influxdb"
	kerrors "github.com/influxdata/influxdb/kit/errors"
	"github.com/julienschmidt/httprouter"
)

// LabelHandler represents an HTTP API handler for labels
type LabelHandler struct {
	*httprouter.Router
}

const (
	labelsPath   = "/api/v2/labels"
	labelsIDPath = "/api/v2/labels/:id"
)

// NewLabelHandler returns a new instance of LabelHandler
func NewLabelHandler() *LabelHandler {
	h := &LabelHandler{
		Router: NewRouter(),
	}

	h.HandlerFunc("POST", labelsPath, h.handlePostLabel)
	h.HandlerFunc("GET", labelsPath, h.handleGetLabels)

	// h.HandlerFunc("GET", labelsIDPath, h.handleGetLabel)
	h.HandlerFunc("PATCH", labelsIDPath, h.handlePatchLabel)
	h.HandlerFunc("DELETE", labelsIDPath, h.handleDeleteLabel)

	return h
}

func (h *LabelHandler) handlePostLabel(w http.ResponseWriter, r *http.Request) {

}

func (h *LabelHandler) handleGetLabels(w http.ResponseWriter, r *http.Request) {

}

// func (h *LabelHandler) handleGetLabel(w http.ResponseWriter, r *http.Request) {}

func (h *LabelHandler) handlePatchLabel(w http.ResponseWriter, r *http.Request) {

}

func (h *LabelHandler) handleDeleteLabel(w http.ResponseWriter, r *http.Request) {

}

// LabelService connects to Influx via HTTP using tokens to manage labels
type LabelService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
	BasePath           string
}

type labelResponse struct {
	Links map[string]string `json:"links"`
	Label plat.Label        `json:"label"`
}

func newLabelResponse(l *plat.Label) *labelResponse {
	return &labelResponse{
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/labels/%s", l.ID),
		},
		Label: *l,
	}
}

type labelsResponse struct {
	Links  map[string]string `json:"links"`
	Labels []*plat.Label     `json:"labels"`
}

func newLabelsResponse(ls []*plat.Label) *labelsResponse {
	return &labelsResponse{
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/labels"),
		},
		Labels: ls,
	}
}

// newGetLabelsHandler returns a handler func for a GET to /labels endpoints
func newGetLabelsHandler(s plat.LabelService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		req, err := decodeGetLabelsRequest(ctx, r)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		labels, err := s.FindResourceLabels(ctx, req.filter)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		if err := encodeResponse(ctx, w, http.StatusOK, newLabelsResponse(labels)); err != nil {
			// TODO: this can potentially result in calling w.WriteHeader multiple times, we need to pass a logger in here
			// some how. This isn't as simple as simply passing in a logger to this function since the time that this function
			// is called is distinct from the time that a potential logger is set.
			EncodeError(ctx, err, w)
			return
		}
	}
}

type getLabelsRequest struct {
	filter plat.LabelMappingFilter
}

func decodeGetLabelsRequest(ctx context.Context, r *http.Request) (*getLabelsRequest, error) {
	req := &getLabelsRequest{}

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i plat.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req.filter.ResourceID = i

	return req, nil
}

// newPostLabelHandler returns a handler func for a POST to /labels endpoints
func newPostLabelHandler(s plat.LabelService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		req, err := decodePostLabelRequest(ctx, r)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		if err := req.Mapping.Validate(); err != nil {
			EncodeError(ctx, err, w)
			return
		}

		if err := s.CreateLabelMapping(ctx, &req.Mapping); err != nil {
			EncodeError(ctx, err, w)
			return
		}

		label, err := s.FindLabelByID(ctx, req.Mapping.LabelID)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		if err := encodeResponse(ctx, w, http.StatusCreated, newLabelResponse(label)); err != nil {
			// TODO: this can potentially result in calling w.WriteHeader multiple times, we need to pass a logger in here
			// some how. This isn't as simple as simply passing in a logger to this function since the time that this function
			// is called is distinct from the time that a potential logger is set.
			EncodeError(ctx, err, w)
			return
		}
	}
}

type postLabelRequest struct {
	Mapping plat.LabelMapping
}

func decodePostLabelRequest(ctx context.Context, r *http.Request) (*postLabelRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var rid plat.ID
	if err := rid.DecodeFromString(id); err != nil {
		return nil, err
	}

	mapping := &plat.LabelMapping{}
	if err := json.NewDecoder(r.Body).Decode(mapping); err != nil {
		return nil, err
	}

	mapping.ResourceID = rid

	if err := mapping.Validate(); err != nil {
		return nil, err
	}

	req := &postLabelRequest{
		Mapping: *mapping,
	}

	return req, nil
}

type patchLabelRequest struct {
	label *plat.Label
	upd   plat.LabelUpdate
}

func decodePatchLabelRequest(ctx context.Context, r *http.Request) (*patchLabelRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &plat.Error{
			Code: plat.EInvalid,
			Msg:  "url missing label id",
		}
	}

	name := params.ByName("lid")
	if name == "" {
		return nil, &plat.Error{
			Code: plat.EInvalid,
			Msg:  "label name is missing",
		}
	}

	var rid plat.ID
	if err := rid.DecodeFromString(id); err != nil {
		return nil, err
	}

	upd := &plat.LabelUpdate{}
	if err := json.NewDecoder(r.Body).Decode(upd); err != nil {
		return nil, err
	}

	return &patchLabelRequest{
		label: &plat.Label{ID: rid, Name: name},
		upd:   *upd,
	}, nil
}

// newPatchLabelHandler returns a handler func for a PATCH to /labels endpoints
func newPatchLabelHandler(s plat.LabelService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		req, err := decodePatchLabelRequest(ctx, r)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		label, err := s.UpdateLabel(ctx, req.label, req.upd)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		if err := encodeResponse(ctx, w, http.StatusOK, newLabelResponse(label)); err != nil {
			// TODO: this can potentially result in calling w.WriteHeader multiple times, we need to pass a logger in here
			// some how. This isn't as simple as simply passing in a logger to this function since the time that this function
			// is called is distinct from the time that a potential logger is set.
			EncodeError(ctx, err, w)
			return
		}
	}
}

// newDeleteLabelHandler returns a handler func for a DELETE to /labels endpoints
func newDeleteLabelHandler(s plat.LabelService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		req, err := decodeDeleteLabelRequest(ctx, r)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		mapping := &plat.LabelMapping{
			LabelID:    req.LabelID,
			ResourceID: req.ResourceID,
		}

		if err := s.DeleteLabelMapping(ctx, mapping); err != nil {
			EncodeError(ctx, err, w)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

type deleteLabelRequest struct {
	ResourceID plat.ID
	LabelID    plat.ID
}

func decodeDeleteLabelRequest(ctx context.Context, r *http.Request) (*deleteLabelRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &plat.Error{
			Code: plat.EInvalid,
			Msg:  "url missing resource id",
		}
	}

	var rid plat.ID
	if err := rid.DecodeFromString(id); err != nil {
		return nil, err
	}

	id = params.ByName("lid")
	if id == "" {
		return nil, &plat.Error{
			Code: plat.EInvalid,
			Msg:  "label id is missing",
		}
	}

	var lid plat.ID
	if err := lid.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteLabelRequest{
		LabelID:    lid,
		ResourceID: rid,
	}, nil
}

// FindLabels returns a slice of labels
func (s *LabelService) FindResourceLabels(ctx context.Context, filter plat.LabelMappingFilter) ([]*plat.Label, error) {
	url, err := newURL(s.Addr, resourceIDPath(s.BasePath, filter.ResourceID))
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

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var r labelsResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}

	return r.Labels, nil
}

func (s *LabelService) CreateLabelMapping(ctx context.Context, m *plat.LabelMapping) error {
	if err := m.Validate(); err != nil {
		return err
	}

	url, err := newURL(s.Addr, resourceIDPath(s.BasePath, m.ResourceID))
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

	if err := CheckError(resp); err != nil {
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(m); err != nil {
		return err
	}

	return nil
}

func (s *LabelService) DeleteLabelMapping(ctx context.Context, m *plat.LabelMapping) error {
	url, err := newURL(s.Addr, labelNamePath(s.BasePath, m.ResourceID, m.LabelID))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", url.String(), nil)
	if err != nil {
		return err
	}
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	return CheckError(resp)
}

func labelNamePath(basePath string, resourceID plat.ID, labelID plat.ID) string {
	return path.Join(basePath, resourceID.String(), "labels", labelID.String())
}
