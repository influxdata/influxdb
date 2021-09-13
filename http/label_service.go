package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"go.uber.org/zap"
)

// LabelHandler represents an HTTP API handler for labels
type LabelHandler struct {
	*httprouter.Router
	errors.HTTPErrorHandler
	log *zap.Logger

	LabelService influxdb.LabelService
}

const (
	prefixLabels = "/api/v2/labels"
	labelsIDPath = "/api/v2/labels/:id"
)

// NewLabelHandler returns a new instance of LabelHandler
func NewLabelHandler(log *zap.Logger, s influxdb.LabelService, he errors.HTTPErrorHandler) *LabelHandler {
	h := &LabelHandler{
		Router:           NewRouter(he),
		HTTPErrorHandler: he,
		log:              log,
		LabelService:     s,
	}

	h.HandlerFunc("POST", prefixLabels, h.handlePostLabel)
	h.HandlerFunc("GET", prefixLabels, h.handleGetLabels)

	h.HandlerFunc("GET", labelsIDPath, h.handleGetLabel)
	h.HandlerFunc("PATCH", labelsIDPath, h.handlePatchLabel)
	h.HandlerFunc("DELETE", labelsIDPath, h.handleDeleteLabel)

	return h
}

func (h *LabelHandler) Prefix() string {
	return prefixLabels
}

// handlePostLabel is the HTTP handler for the POST /api/v2/labels route.
func (h *LabelHandler) handlePostLabel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePostLabelRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.LabelService.CreateLabel(ctx, req.Label); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Label created", zap.String("label", fmt.Sprint(req.Label)))
	if err := encodeResponse(ctx, w, http.StatusCreated, newLabelResponse(req.Label)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type postLabelRequest struct {
	Label *influxdb.Label
}

func (b postLabelRequest) Validate() error {
	if b.Label.Name == "" {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "label requires a name",
		}
	}
	if !b.Label.OrgID.Valid() {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "label requires a valid orgID",
		}
	}
	return nil
}

// TODO(jm): ensure that the specified org actually exists
func decodePostLabelRequest(ctx context.Context, r *http.Request) (*postLabelRequest, error) {
	l := &influxdb.Label{}
	if err := json.NewDecoder(r.Body).Decode(l); err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "unable to decode label request",
			Err:  err,
		}
	}

	req := &postLabelRequest{
		Label: l,
	}

	return req, req.Validate()
}

// handleGetLabels is the HTTP handler for the GET /api/v2/labels route.
func (h *LabelHandler) handleGetLabels(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetLabelsRequest(r.URL.Query())
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindLabels(ctx, req.filter)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Labels retrieved", zap.String("labels", fmt.Sprint(labels)))
	err = encodeResponse(ctx, w, http.StatusOK, newLabelsResponse(labels))
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

type getLabelsRequest struct {
	filter influxdb.LabelFilter
}

func decodeGetLabelsRequest(qp url.Values) (*getLabelsRequest, error) {
	req := &getLabelsRequest{
		filter: influxdb.LabelFilter{
			Name: qp.Get("name"),
		},
	}

	if orgID := qp.Get("orgID"); orgID != "" {
		id, err := platform.IDFromString(orgID)
		if err != nil {
			return nil, err
		}
		req.filter.OrgID = id
	}

	return req, nil
}

// handleGetLabel is the HTTP handler for the GET /api/v2/labels/id route.
func (h *LabelHandler) handleGetLabel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetLabelRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	l, err := h.LabelService.FindLabelByID(ctx, req.LabelID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Label retrieved", zap.String("label", fmt.Sprint(l)))
	if err := encodeResponse(ctx, w, http.StatusOK, newLabelResponse(l)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getLabelRequest struct {
	LabelID platform.ID
}

func decodeGetLabelRequest(ctx context.Context, r *http.Request) (*getLabelRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "label id is not valid",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req := &getLabelRequest{
		LabelID: i,
	}

	return req, nil
}

// handleDeleteLabel is the HTTP handler for the DELETE /api/v2/labels/:id route.
func (h *LabelHandler) handleDeleteLabel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeDeleteLabelRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.LabelService.DeleteLabel(ctx, req.LabelID); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Label deleted", zap.String("labelID", fmt.Sprint(req.LabelID)))
	w.WriteHeader(http.StatusNoContent)
}

type deleteLabelRequest struct {
	LabelID platform.ID
}

func decodeDeleteLabelRequest(ctx context.Context, r *http.Request) (*deleteLabelRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req := &deleteLabelRequest{
		LabelID: i,
	}

	return req, nil
}

// handlePatchLabel is the HTTP handler for the PATCH /api/v2/labels route.
func (h *LabelHandler) handlePatchLabel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePatchLabelRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	l, err := h.LabelService.UpdateLabel(ctx, req.LabelID, req.Update)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Label updated", zap.String("label", fmt.Sprint(l)))
	if err := encodeResponse(ctx, w, http.StatusOK, newLabelResponse(l)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type patchLabelRequest struct {
	Update  influxdb.LabelUpdate
	LabelID platform.ID
}

func decodePatchLabelRequest(ctx context.Context, r *http.Request) (*patchLabelRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	upd := &influxdb.LabelUpdate{}
	if err := json.NewDecoder(r.Body).Decode(upd); err != nil {
		return nil, err
	}

	return &patchLabelRequest{
		Update:  *upd,
		LabelID: i,
	}, nil
}

type labelResponse struct {
	Links map[string]string `json:"links"`
	Label influxdb.Label    `json:"label"`
}

func newLabelResponse(l *influxdb.Label) *labelResponse {
	return &labelResponse{
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/labels/%s", l.ID),
		},
		Label: *l,
	}
}

type labelsResponse struct {
	Links  map[string]string `json:"links"`
	Labels []*influxdb.Label `json:"labels"`
}

func newLabelsResponse(ls []*influxdb.Label) *labelsResponse {
	return &labelsResponse{
		Links: map[string]string{
			"self": "/api/v2/labels",
		},
		Labels: ls,
	}
}

// LabelBackend is all services and associated parameters required to construct
// label handlers.
type LabelBackend struct {
	log *zap.Logger
	errors.HTTPErrorHandler
	LabelService influxdb.LabelService
	ResourceType influxdb.ResourceType
}

// newGetLabelsHandler returns a handler func for a GET to /labels endpoints
func newGetLabelsHandler(b *LabelBackend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		req, err := decodeGetLabelMappingsRequest(ctx, b.ResourceType)
		if err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		labels, err := b.LabelService.FindResourceLabels(ctx, req.filter)
		if err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		if err := encodeResponse(ctx, w, http.StatusOK, newLabelsResponse(labels)); err != nil {
			logEncodingError(b.log, r, err)
			return
		}
	}
}

type getLabelMappingsRequest struct {
	filter influxdb.LabelMappingFilter
}

func decodeGetLabelMappingsRequest(ctx context.Context, rt influxdb.ResourceType) (*getLabelMappingsRequest, error) {
	req := &getLabelMappingsRequest{}

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req.filter.ResourceID = i
	req.filter.ResourceType = rt

	return req, nil
}

// newPostLabelHandler returns a handler func for a POST to /labels endpoints
func newPostLabelHandler(b *LabelBackend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		req, err := decodePostLabelMappingRequest(ctx, r, b.ResourceType)
		if err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		if err := req.Mapping.Validate(); err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		if err := b.LabelService.CreateLabelMapping(ctx, &req.Mapping); err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		label, err := b.LabelService.FindLabelByID(ctx, req.Mapping.LabelID)
		if err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		if err := encodeResponse(ctx, w, http.StatusCreated, newLabelResponse(label)); err != nil {
			logEncodingError(b.log, r, err)
			return
		}
	}
}

type postLabelMappingRequest struct {
	Mapping influxdb.LabelMapping
}

func decodePostLabelMappingRequest(ctx context.Context, r *http.Request, rt influxdb.ResourceType) (*postLabelMappingRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		}
	}

	var rid platform.ID
	if err := rid.DecodeFromString(id); err != nil {
		return nil, err
	}

	mapping := &influxdb.LabelMapping{}
	if err := json.NewDecoder(r.Body).Decode(mapping); err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Invalid post label map request",
		}
	}

	mapping.ResourceID = rid
	mapping.ResourceType = rt

	if err := mapping.Validate(); err != nil {
		return nil, err
	}

	req := &postLabelMappingRequest{
		Mapping: *mapping,
	}

	return req, nil
}

// newDeleteLabelHandler returns a handler func for a DELETE to /labels endpoints
func newDeleteLabelHandler(b *LabelBackend) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		req, err := decodeDeleteLabelMappingRequest(ctx, r)
		if err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		mapping := &influxdb.LabelMapping{
			LabelID:      req.LabelID,
			ResourceID:   req.ResourceID,
			ResourceType: b.ResourceType,
		}

		if err := b.LabelService.DeleteLabelMapping(ctx, mapping); err != nil {
			b.HandleHTTPError(ctx, err, w)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

type deleteLabelMappingRequest struct {
	ResourceID platform.ID
	LabelID    platform.ID
}

func decodeDeleteLabelMappingRequest(ctx context.Context, r *http.Request) (*deleteLabelMappingRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing resource id",
		}
	}

	var rid platform.ID
	if err := rid.DecodeFromString(id); err != nil {
		return nil, err
	}

	id = params.ByName("lid")
	if id == "" {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "label id is missing",
		}
	}

	var lid platform.ID
	if err := lid.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteLabelMappingRequest{
		LabelID:    lid,
		ResourceID: rid,
	}, nil
}

func labelIDPath(id platform.ID) string {
	return path.Join(prefixLabels, id.String())
}

// LabelService connects to Influx via HTTP using tokens to manage labels
type LabelService struct {
	Client   *httpc.Client
	OpPrefix string
}

// FindLabelByID returns a single label by ID.
func (s *LabelService) FindLabelByID(ctx context.Context, id platform.ID) (*influxdb.Label, error) {
	var lr labelResponse
	err := s.Client.
		Get(labelIDPath(id)).
		DecodeJSON(&lr).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return &lr.Label, nil
}

// FindLabels is a client for the find labels response from the server.
func (s *LabelService) FindLabels(ctx context.Context, filter influxdb.LabelFilter, opt ...influxdb.FindOptions) ([]*influxdb.Label, error) {
	params := influxdb.FindOptionParams(opt...)
	if filter.OrgID != nil {
		params = append(params, [2]string{"orgID", filter.OrgID.String()})
	}
	if filter.Name != "" {
		params = append(params, [2]string{"name", filter.Name})
	}

	var lr labelsResponse
	err := s.Client.
		Get(prefixLabels).
		QueryParams(params...).
		DecodeJSON(&lr).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return lr.Labels, nil
}

// FindResourceLabels returns a list of labels, derived from a label mapping filter.
func (s *LabelService) FindResourceLabels(ctx context.Context, filter influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
	if err := filter.Valid(); err != nil {
		return nil, err
	}

	var r labelsResponse
	err := s.Client.
		Get(resourceIDPath(filter.ResourceType, filter.ResourceID, "labels")).
		DecodeJSON(&r).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return r.Labels, nil
}

// CreateLabel creates a new label.
func (s *LabelService) CreateLabel(ctx context.Context, l *influxdb.Label) error {
	var lr labelResponse
	err := s.Client.
		PostJSON(l, prefixLabels).
		DecodeJSON(&lr).
		Do(ctx)
	if err != nil {
		return err
	}

	// this is super dirty >_<
	*l = lr.Label
	return nil
}

// UpdateLabel updates a label and returns the updated label.
func (s *LabelService) UpdateLabel(ctx context.Context, id platform.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
	var lr labelResponse
	err := s.Client.
		PatchJSON(upd, labelIDPath(id)).
		DecodeJSON(&lr).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return &lr.Label, nil
}

// DeleteLabel removes a label by ID.
func (s *LabelService) DeleteLabel(ctx context.Context, id platform.ID) error {
	return s.Client.
		Delete(labelIDPath(id)).
		Do(ctx)
}

// CreateLabelMapping will create a labbel mapping
func (s *LabelService) CreateLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	if err := m.Validate(); err != nil {
		return err
	}

	urlPath := resourceIDPath(m.ResourceType, m.ResourceID, "labels")
	return s.Client.
		PostJSON(m, urlPath).
		DecodeJSON(m).
		Do(ctx)
}

func (s *LabelService) DeleteLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	if err := m.Validate(); err != nil {
		return err
	}

	return s.Client.
		Delete(resourceIDMappingPath(m.ResourceType, m.ResourceID, "labels", m.LabelID)).
		Do(ctx)
}

func resourceIDMappingPath(resourceType influxdb.ResourceType, resourceID platform.ID, p string, labelID platform.ID) string {
	return path.Join("/api/v2/", string(resourceType), resourceID.String(), p, labelID.String())
}
