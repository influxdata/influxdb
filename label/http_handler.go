package label

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

type LabelEmbeddedHandler struct {
	chi.Router
	api      *kithttp.API
	log      *zap.Logger
	labelSvc influxdb.LabelService
	rt       influxdb.ResourceType
}

// NewHTTPEmbeddedHandler create a label handler for embedding in other service apis
func NewHTTPEmbeddedHandler(log *zap.Logger, rt influxdb.ResourceType, ls influxdb.LabelService) *LabelEmbeddedHandler {
	h := &LabelEmbeddedHandler{
		api:      kithttp.NewAPI(kithttp.WithLog(log)),
		log:      log,
		labelSvc: ls,
		rt:       rt,
	}

	r := chi.NewRouter()
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		h.api.Err(w, r, &errors.Error{
			Code: errors.ENotFound,
			Msg:  "path not found",
		})
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		h.api.Err(w, r, &errors.Error{
			Code: errors.EMethodNotAllowed,
			Msg:  fmt.Sprintf("allow: %s", w.Header().Get("Allow")),
		})

	})
	r.Use(
		kithttp.SkipOptions,
		middleware.StripSlashes,
		kithttp.SetCORS,
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)

	r.Route("/", func(r chi.Router) {
		r.Post("/", h.handlePostLabelMapping)
		r.Get("/", h.handleFindResourceLabels)
		r.Delete("/{labelID}", h.handleDeleteLabelMapping)
	})

	h.Router = r
	return h
}

// handlePostLabelMapping create a new label mapping for the host service api
func (h *LabelEmbeddedHandler) handlePostLabelMapping(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	embeddedID, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	mapping := &influxdb.LabelMapping{}
	if err := json.NewDecoder(r.Body).Decode(mapping); err != nil {
		h.api.Err(w, r, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Invalid post label map request",
		})
	}

	mapping.ResourceID = *embeddedID
	mapping.ResourceType = h.rt

	if err := mapping.Validate(); err != nil {
		h.api.Err(w, r, err)
		return
	}

	label, err := h.labelSvc.FindLabelByID(ctx, mapping.LabelID)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	if err := h.labelSvc.CreateLabelMapping(ctx, mapping); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusCreated, newLabelResponse(label))
}

// handleFindResourceLabels list labels that reference the host api
func (h *LabelEmbeddedHandler) handleFindResourceLabels(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	embeddedID, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	filter := influxdb.LabelMappingFilter{
		ResourceID:   *embeddedID,
		ResourceType: h.rt,
	}

	labels, err := h.labelSvc.FindResourceLabels(ctx, filter)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, newLabelsResponse(labels))
}

// handleDeleteLabelMapping delete a mapping for this host and label combination
func (h *LabelEmbeddedHandler) handleDeleteLabelMapping(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	embeddedID, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	labelID, err := platform.IDFromString(chi.URLParam(r, "labelID"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	mapping := &influxdb.LabelMapping{
		LabelID:      *labelID,
		ResourceID:   *embeddedID,
		ResourceType: h.rt,
	}

	if err := h.labelSvc.DeleteLabelMapping(ctx, mapping); err != nil {
		h.api.Err(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
