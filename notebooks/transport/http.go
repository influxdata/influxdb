package transport

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	feature "github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/notebooks/service"
	"go.uber.org/zap"
)

const (
	prefixNotebooks = "/api/v2private/notebooks"
)

var (
	errBadOrg = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "invalid or missing org id",
	}

	errBadId = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "notebook id is invalid",
	}
)

// NotebookHandler is the handler for the notebook service
type NotebookHandler struct {
	chi.Router

	log *zap.Logger
	api *kithttp.API

	notebookService service.NotebookService
}

func NewNotebookHandler(
	log *zap.Logger,
	notebookService service.NotebookService,
) *NotebookHandler {
	h := &NotebookHandler{
		log:             log,
		api:             kithttp.NewAPI(kithttp.WithLog(log)),
		notebookService: notebookService,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
		h.mwNotebookFlag, // temporary, remove when feature flag for notebooks is removed
	)

	r.Route("/", func(r chi.Router) {
		r.Get("/", h.handleGetNotebooks)
		r.Post("/", h.handleCreateNotebook)

		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", h.handleGetNotebook)
			r.Delete("/", h.handleDeleteNotebook)
			r.Put("/", h.handleUpdateNotebook)
			r.Patch("/", h.handleUpdateNotebook)
		})
	})

	h.Router = r

	return h
}

func (h *NotebookHandler) Prefix() string {
	return prefixNotebooks
}

func (h *NotebookHandler) mwNotebookFlag(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flags := feature.FlagsFromContext(r.Context())

		if !flags["notebooks"].(bool) || !flags["notebooksApi"].(bool) {
			h.api.Respond(w, r, http.StatusNoContent, nil)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// get a list of all notebooks for an org
func (h *NotebookHandler) handleGetNotebooks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID := r.URL.Query().Get("orgID")
	o, err := platform.IDFromString(orgID)
	if err != nil {
		h.api.Err(w, r, errBadOrg)
		return
	}

	d, err := h.notebookService.ListNotebooks(ctx, service.NotebookListFilter{OrgID: *o})
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, d)
}

// create a single notebook.
func (h *NotebookHandler) handleCreateNotebook(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	b, err := h.decodeNotebookReqBody(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	new, err := h.notebookService.CreateNotebook(ctx, b)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, new)
}

// get a single notebook.
func (h *NotebookHandler) handleGetNotebook(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, errBadId)
		return
	}

	d, err := h.notebookService.GetNotebook(ctx, *id)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, d)
}

// delete a single notebook.
func (h *NotebookHandler) handleDeleteNotebook(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, errBadId)
		return
	}

	if err := h.notebookService.DeleteNotebook(ctx, *id); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusNoContent, nil)
}

// update a single notebook.
func (h *NotebookHandler) handleUpdateNotebook(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, errBadId)
		return
	}

	b, err := h.decodeNotebookReqBody(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	u, err := h.notebookService.UpdateNotebook(ctx, *id, b)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, u)
}

func (h *NotebookHandler) decodeNotebookReqBody(r *http.Request) (*service.NotebookReqBody, error) {
	b := &service.NotebookReqBody{}
	if err := h.api.DecodeJSON(r.Body, b); err != nil {
		return nil, err
	}

	if err := b.Validate(); err != nil {
		return nil, err
	}

	return b, nil
}
