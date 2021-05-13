package transport

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/notebooks/service"
	"go.uber.org/zap"
)

const (
	prefixNotebooks = "/api/v2/notebooks"
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
		r.With(h.mwRequireOrgID).Get("/", h.handleGetNotebooks)
		r.With(h.mwNotebookReqBody).Post("/", h.handleCreateNotebook)

		r.Group(func(r chi.Router) {
			r.Use(
				h.mwRequireNotebookID,
			)
			r.Route("/{id}", func(r chi.Router) {
				r.Get("/", h.handleGetNotebook)
				r.Delete("/", h.handleDeleteNotebook)
				r.With(h.mwNotebookReqBody).Put("/", h.handleUpdateNotebook)
				r.With(h.mwNotebookReqBody).Patch("/", h.handleUpdateNotebook)
			})
		})
	})

	h.Router = r

	return h
}

func (h *NotebookHandler) Prefix() string {
	return prefixNotebooks
}

// get a list of all notebooks for an org
func (h *NotebookHandler) handleGetNotebooks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID := orgIDFromContext(ctx)

	d, err := h.notebookService.ListNotebooks(ctx, service.NotebookListFilter{OrgID: orgID})
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, d)
}

// create a single notebook.
func (h *NotebookHandler) handleCreateNotebook(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	b := notebookReqBodyFromContext(ctx)

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
	id := notebookIDFromContext(ctx)

	d, err := h.notebookService.GetNotebook(ctx, id)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, d)
}

// delete a single notebook.
func (h *NotebookHandler) handleDeleteNotebook(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := notebookIDFromContext(ctx)

	if err := h.notebookService.DeleteNotebook(ctx, id); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, nil)
}

// update a single notebook.
func (h *NotebookHandler) handleUpdateNotebook(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := notebookIDFromContext(ctx)
	b := notebookReqBodyFromContext(ctx)

	u, err := h.notebookService.UpdateNotebook(ctx, id, b)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, u)
}
