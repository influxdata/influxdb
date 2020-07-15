package label

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	ihttp "github.com/influxdata/influxdb/v2/http"
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

	r := ihttp.NewBaseChiRouter(h.api)
	r.Use(
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

	embeddedID, err := influxdb.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	mapping := &influxdb.LabelMapping{}
	if err := json.NewDecoder(r.Body).Decode(mapping); err != nil {
		h.api.Err(w, r, &influxdb.Error{
			Code: influxdb.EInvalid,
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

	embeddedID, err := influxdb.IDFromString(chi.URLParam(r, "id"))
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

	embeddedID, err := influxdb.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	labelID, err := influxdb.IDFromString(chi.URLParam(r, "labelID"))
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
