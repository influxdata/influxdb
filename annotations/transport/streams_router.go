package transport

import (
	"net/http"

	"github.com/go-chi/chi"
)

func (h *AnnotationHandler) streamsRouter() http.Handler {
	r := chi.NewRouter()

	r.Put("/", h.handleCreateOrUpdateStreams)
	r.Get("/", h.handleGetStreams)
	r.Delete("/", h.handleDeleteStreams)

	r.Route("/{id}", func(r chi.Router) {
		r.Delete("/", h.handleDeleteStream)
		r.Put("/", h.handleUpdateStream)
	})

	return r
}

func (h *AnnotationHandler) handleCreateOrUpdateStreams(w http.ResponseWriter, r *http.Request) {

	h.api.Respond(w, r, http.StatusOK, nil)
}

func (h *AnnotationHandler) handleGetStreams(w http.ResponseWriter, r *http.Request) {

	h.api.Respond(w, r, http.StatusOK, nil)
}

func (h *AnnotationHandler) handleDeleteStreams(w http.ResponseWriter, r *http.Request) {

	h.api.Respond(w, r, http.StatusOK, nil)
}

func (h *AnnotationHandler) handleDeleteStream(w http.ResponseWriter, r *http.Request) {

	h.api.Respond(w, r, http.StatusOK, nil)
}

func (h *AnnotationHandler) handleUpdateStream(w http.ResponseWriter, r *http.Request) {

	h.api.Respond(w, r, http.StatusOK, nil)
}
