package transport

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

func (h *AnnotationHandler) annotationsRouter() http.Handler {
	r := chi.NewRouter()

	r.Post("/", h.handleCreateAnnotations)
	r.Get("/", h.handleGetAnnotations)
	r.Delete("/", h.handleDeleteAnnotations)

	r.Route("/{id}", func(r chi.Router) {
		r.Get("/", h.handleGetAnnotation)
		r.Delete("/", h.handleDeleteAnnotation)
		r.Put("/", h.handleUpdateAnnotation)
	})

	return r
}

func (h *AnnotationHandler) handleCreateAnnotations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	o, err := platform.IDFromString(r.URL.Query().Get("orgID"))
	if err != nil {
		h.api.Err(w, r, errBadOrg)
		return
	}

	c, err := decodeCreateAnnotationsRequest(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	l, err := h.annotationService.CreateAnnotations(ctx, *o, c)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, l)
}

func (h *AnnotationHandler) handleGetAnnotations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	o, err := platform.IDFromString(r.URL.Query().Get("orgID"))
	if err != nil {
		h.api.Err(w, r, errBadOrg)
		return
	}

	f, err := decodeListAnnotationsRequest(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	s, err := h.annotationService.ListAnnotations(ctx, *o, *f)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	l, err := storedAnnotationsToReadAnnotations(s)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, l)
}

func (h *AnnotationHandler) handleDeleteAnnotations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	o, err := platform.IDFromString(r.URL.Query().Get("orgID"))
	if err != nil {
		h.api.Err(w, r, errBadOrg)
		return
	}

	f, err := decodeDeleteAnnotationsRequest(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	if err = h.annotationService.DeleteAnnotations(ctx, *o, *f); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusNoContent, nil)
}

func (h *AnnotationHandler) handleGetAnnotation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, errBadAnnotationId)
		return
	}

	s, err := h.annotationService.GetAnnotation(ctx, *id)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	c, err := storedAnnotationToEvent(s)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, c)
}

func (h *AnnotationHandler) handleDeleteAnnotation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, errBadAnnotationId)
		return
	}

	if err := h.annotationService.DeleteAnnotation(ctx, *id); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusNoContent, nil)
}

func (h *AnnotationHandler) handleUpdateAnnotation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, errBadAnnotationId)
		return
	}

	u, err := decodeUpdateAnnotationRequest(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	a, err := h.annotationService.UpdateAnnotation(ctx, *id, *u)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, a)
}

func decodeCreateAnnotationsRequest(r *http.Request) ([]influxdb.AnnotationCreate, error) {
	cs := []influxdb.AnnotationCreate{}
	if err := json.NewDecoder(r.Body).Decode(&cs); err != nil {
		return nil, err
	}

	for _, c := range cs {
		if err := c.Validate(time.Now); err != nil {
			return nil, err
		}
	}

	return cs, nil
}

func decodeListAnnotationsRequest(r *http.Request) (*influxdb.AnnotationListFilter, error) {
	startTime, endTime, err := tFromReq(r)
	if err != nil {
		return nil, err
	}

	f := &influxdb.AnnotationListFilter{
		StreamIncludes: r.URL.Query()["streamIncludes"],
		BasicFilter: influxdb.BasicFilter{
			EndTime:   endTime,
			StartTime: startTime,
		},
	}
	f.SetStickerIncludes(r.URL.Query())
	if err := f.Validate(time.Now); err != nil {
		return nil, err
	}

	return f, nil
}

func decodeDeleteAnnotationsRequest(r *http.Request) (*influxdb.AnnotationDeleteFilter, error) {
	// Try to get a stream ID from the query params. The stream ID is not required,
	// so if one is not set we can leave streamID as the zero value.
	var streamID platform.ID
	if qid := chi.URLParam(r, "streamID"); qid != "" {
		id, err := platform.IDFromString(qid)
		// if a streamID parameter was provided but is not valid, return an error
		if err != nil {
			return nil, errBadStreamId
		}
		streamID = *id
	}

	startTime, endTime, err := tFromReq(r)
	if err != nil {
		return nil, err
	}

	f := &influxdb.AnnotationDeleteFilter{
		StreamTag: r.URL.Query().Get("stream"),
		StreamID:  streamID,
		EndTime:   endTime,
		StartTime: startTime,
	}
	f.SetStickers(r.URL.Query())
	if err := f.Validate(); err != nil {
		return nil, err
	}

	return f, nil
}

func decodeUpdateAnnotationRequest(r *http.Request) (*influxdb.AnnotationCreate, error) {
	u := &influxdb.AnnotationCreate{}
	if err := json.NewDecoder(r.Body).Decode(u); err != nil {
		return nil, err
	} else if err := u.Validate(time.Now); err != nil {
		return nil, err
	}

	return u, nil
}

func storedAnnotationsToReadAnnotations(s []influxdb.StoredAnnotation) (influxdb.ReadAnnotations, error) {
	r := influxdb.ReadAnnotations{}

	for _, val := range s {
		r[val.StreamTag] = append(r[val.StreamTag], influxdb.ReadAnnotation{
			ID:        val.ID,
			Summary:   val.Summary,
			Message:   val.Message,
			Stickers:  val.Stickers,
			StartTime: val.Lower,
			EndTime:   val.Upper,
		})
	}

	return r, nil
}

func storedAnnotationToEvent(s *influxdb.StoredAnnotation) (*influxdb.AnnotationEvent, error) {
	st, err := tStringToPointer(s.Lower)
	if err != nil {
		return nil, err
	}

	et, err := tStringToPointer(s.Upper)
	if err != nil {
		return nil, err
	}

	return &influxdb.AnnotationEvent{
		ID: s.ID,
		AnnotationCreate: influxdb.AnnotationCreate{
			StreamTag: s.StreamTag,
			Summary:   s.Summary,
			Message:   s.Message,
			Stickers:  s.Stickers,
			EndTime:   et,
			StartTime: st,
		},
	}, nil
}
