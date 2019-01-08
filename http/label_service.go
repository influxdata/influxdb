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

// TODO: remove "dashboard" from this
func newLabelResponse(l *plat.Label) *labelResponse {
	return &labelResponse{
		Links: map[string]string{
			"resource": fmt.Sprintf("/api/v2/%ss/%s", "dashboard", l.ResourceID),
		},
		Label: *l,
	}
}

type labelsResponse struct {
	Links  map[string]string `json:"links"`
	Labels []*plat.Label     `json:"labels"`
}

func newLabelsResponse(opts plat.FindOptions, f plat.LabelFilter, ls []*plat.Label) *labelsResponse {
	// TODO: Remove "dashboard" from this
	return &labelsResponse{
		Links: map[string]string{
			"resource": fmt.Sprintf("/api/v2/%ss/%s", "dashboard", f.ResourceID),
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

		opts := plat.FindOptions{}
		labels, err := s.FindLabels(ctx, req.filter)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		if err := encodeResponse(ctx, w, http.StatusOK, newLabelsResponse(opts, req.filter, labels)); err != nil {
			// TODO: this can potentially result in calling w.WriteHeader multiple times, we need to pass a logger in here
			// some how. This isn't as simple as simply passing in a logger to this function since the time that this function
			// is called is distinct from the time that a potential logger is set.
			EncodeError(ctx, err, w)
			return
		}
	}
}

type getLabelsRequest struct {
	filter plat.LabelFilter
}

func decodeGetLabelsRequest(ctx context.Context, r *http.Request) (*getLabelsRequest, error) {
	qp := r.URL.Query()
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

	if name := qp.Get("name"); name != "" {
		req.filter.Name = name
	}

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

		if err := req.Label.Validate(); err != nil {
			EncodeError(ctx, err, w)
			return
		}

		if err := s.CreateLabel(ctx, &req.Label); err != nil {
			EncodeError(ctx, err, w)
			return
		}

		if err := encodeResponse(ctx, w, http.StatusCreated, newLabelResponse(&req.Label)); err != nil {
			// TODO: this can potentially result in calling w.WriteHeader multiple times, we need to pass a logger in here
			// some how. This isn't as simple as simply passing in a logger to this function since the time that this function
			// is called is distinct from the time that a potential logger is set.
			EncodeError(ctx, err, w)
			return
		}
	}
}

type postLabelRequest struct {
	Label plat.Label
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

	label := &plat.Label{}
	if err := json.NewDecoder(r.Body).Decode(label); err != nil {
		return nil, err
	}

	label.ResourceID = rid

	if err := label.Validate(); err != nil {
		return nil, err
	}

	req := &postLabelRequest{
		Label: *label,
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
			Msg:  "url missing resource id",
		}
	}

	name := params.ByName("name")
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
		label: &plat.Label{ResourceID: rid, Name: name},
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

		label := plat.Label{
			ResourceID: req.ResourceID,
			Name:       req.Name,
		}

		if err := s.DeleteLabel(ctx, label); err != nil {
			EncodeError(ctx, err, w)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

type deleteLabelRequest struct {
	ResourceID plat.ID
	Name       string
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

	name := params.ByName("name")
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

	return &deleteLabelRequest{
		Name:       name,
		ResourceID: rid,
	}, nil
}

// FindLabels returns a slice of labels
func (s *LabelService) FindLabels(ctx context.Context, filter plat.LabelFilter, opt ...plat.FindOptions) ([]*plat.Label, error) {
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

func (s *LabelService) CreateLabel(ctx context.Context, l *plat.Label) error {
	if err := l.Validate(); err != nil {
		return err
	}

	url, err := newURL(s.Addr, resourceIDPath(s.BasePath, l.ResourceID))
	if err != nil {
		return err
	}

	octets, err := json.Marshal(l)
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

	if err := json.NewDecoder(resp.Body).Decode(l); err != nil {
		return err
	}

	return nil
}

func (s *LabelService) DeleteLabel(ctx context.Context, l plat.Label) error {
	url, err := newURL(s.Addr, labelNamePath(s.BasePath, l.ResourceID, l.Name))
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

func labelNamePath(basePath string, resourceID plat.ID, name string) string {
	return path.Join(basePath, resourceID.String(), "labels", name)
}
