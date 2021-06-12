package transport

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

func (h *AnnotationHandler) streamsRouter() http.Handler {
	r := chi.NewRouter()

	r.Put("/", h.handleCreateOrUpdateStream)
	r.Get("/", h.handleGetStreams)
	r.Delete("/", h.handleDeleteStreams)

	r.Route("/{id}", func(r chi.Router) {
		r.Delete("/", h.handleDeleteStream)
		r.Put("/", h.handleUpdateStreamByID)
	})

	return r
}

func (h *AnnotationHandler) handleCreateOrUpdateStream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	o, err := platform.IDFromString(r.URL.Query().Get("orgID"))
	if err != nil {
		h.api.Err(w, r, errBadOrg)
		return
	}

	u, err := decodeCreateOrUpdateStreamRequest(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	s, err := h.annotationService.CreateOrUpdateStream(ctx, *o, *u)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, s)
}

func (h *AnnotationHandler) handleGetStreams(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	o, err := platform.IDFromString(r.URL.Query().Get("orgID"))
	if err != nil {
		h.api.Err(w, r, errBadOrg)
		return
	}

	f := decodeListStreamsRequest(r)

	s, err := h.annotationService.ListStreams(ctx, *o, *f)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, storedStreamsToReadStreams(s))
}

// Delete stream(s) by name, capable of handling a list of names
func (h *AnnotationHandler) handleDeleteStreams(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	o, err := platform.IDFromString(r.URL.Query().Get("orgID"))
	if err != nil {
		h.api.Err(w, r, errBadOrg)
		return
	}

	f, err := decodeDeleteStreamsRequest(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	// delete all of the streams according to the filter. annotations associated with the stream
	// will be deleted by the ON DELETE CASCADE relationship between streams and annotations.
	if err = h.annotationService.DeleteStreams(ctx, *o, *f); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusNoContent, nil)
}

// Delete a single stream by ID
func (h *AnnotationHandler) handleDeleteStream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, errBadAnnotationId)
		return
	}

	// as in the handleDeleteStreams method above, deleting a stream will delete annotations
	// associated with it due to the ON DELETE CASCADE relationship between the two
	if err := h.annotationService.DeleteStreamByID(ctx, *id); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusNoContent, nil)
}

func (h *AnnotationHandler) handleUpdateStreamByID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, errBadAnnotationId)
		return
	}

	u, err := decodeCreateOrUpdateStreamRequest(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	s, err := h.annotationService.UpdateStream(ctx, *id, *u)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, s)
}

func decodeCreateOrUpdateStreamRequest(r *http.Request) (*influxdb.Stream, error) {
	s := influxdb.Stream{}

	if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
		return nil, err
	}

	if err := s.Validate(false); err != nil {
		return nil, err
	}

	return &s, nil
}

func decodeListStreamsRequest(r *http.Request) *influxdb.StreamListFilter {
	f := &influxdb.StreamListFilter{
		StreamIncludes: r.URL.Query()["streamIncludes"],
	}
	return f
}

func decodeDeleteStreamsRequest(r *http.Request) (*influxdb.BasicStream, error) {
	f := &influxdb.BasicStream{
		Names: r.URL.Query()["stream"],
	}

	if !f.IsValid() {
		return nil, errBadStreamName
	}

	return f, nil
}

func storedStreamsToReadStreams(stored []influxdb.StoredStream) []influxdb.ReadStream {
	r := make([]influxdb.ReadStream, 0, len(stored))

	for _, s := range stored {
		r = append(r, influxdb.ReadStream{
			ID:          s.ID,
			Name:        s.Name,
			Description: s.Description,
			CreatedAt:   s.CreatedAt,
			UpdatedAt:   s.UpdatedAt,
		})
	}

	return r
}
