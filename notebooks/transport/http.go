package transport

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2/context"
	feature "github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	notebooks "github.com/influxdata/influxdb/v2/notebooks/service"
	"go.uber.org/zap"
)

const (
	prefixNotebooks = "/api/v2/notebooks"
)

// NotebookHandler is the handler for the notebook service
type NotebookHandler struct {
	chi.Router

	api *kithttp.API
	log *zap.Logger
}

func NewNotebookHandler(log *zap.Logger) *NotebookHandler {
	h := &NotebookHandler{
		log: log,
		api: kithttp.NewAPI(kithttp.WithLog(log)),
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
		h.notebookFlag, // temporary, remove when feature flag for notebooks is removed
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

// notebookFlag is middleware for returning no content if the notebooks feature
// flag is set to false. remove this middleware when the feature flag is removed.
func (h *NotebookHandler) notebookFlag(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		flags := feature.FlagsFromContext(r.Context())

		if !flags["notebooks"].(bool) || !flags["notebooksApi"].(bool) {
			h.api.Respond(w, r, http.StatusNoContent, nil)
			return
		}

		next.ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}

// get a list of all notebooks for an org
func (h *NotebookHandler) handleGetNotebooks(w http.ResponseWriter, r *http.Request) {
	// Demo data - respond with a list of notebooks for the requesting org
	orgID, err := orgIDFromReq(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	d := map[string][]notebooks.Notebook{}
	d["flows"] = demoNotebooks(3, *orgID)

	h.api.Respond(w, r, http.StatusOK, d)
}

// create a single notebook
func (h *NotebookHandler) handleCreateNotebook(w http.ResponseWriter, r *http.Request) {
	// Demo data - just return the body from the request with a generated ID
	b := notebooks.Notebook{}
	if err := h.api.DecodeJSON(r.Body, &b); err != nil {
		h.api.Err(w, r, err)
		return
	}
	id, _ := platform.IDFromString(strconv.Itoa(1000000000000000 + 1)) // give it an ID from the getNotebooks list so that the UI doesn't break
	b.ID = *id

	h.api.Respond(w, r, http.StatusOK, b)
}

// get a single notebook
func (h *NotebookHandler) handleGetNotebook(w http.ResponseWriter, r *http.Request) {
	orgID, err := orgIDFromReq(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	notebookID, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	// Demo data - return a notebook for the request org and id
	d := demoNotebook(*orgID, *notebookID)

	h.api.Respond(w, r, http.StatusOK, d)
}

// update a single notebook
func (h *NotebookHandler) handleUpdateNotebook(w http.ResponseWriter, r *http.Request) {
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	// Demo data - just return the body from the request with the id
	b := notebooks.Notebook{}
	if err := h.api.DecodeJSON(r.Body, &b); err != nil {
		h.api.Err(w, r, err)
		return
	}
	b.ID = *id

	fmt.Printf("\n%#v\n", b)

	h.api.Respond(w, r, http.StatusOK, b)
}

// delete a single notebook
func (h *NotebookHandler) handleDeleteNotebook(w http.ResponseWriter, r *http.Request) {
	// for now, just respond with 200 unless there is a problem with the notebook ID
	if _, err := platform.IDFromString(chi.URLParam(r, "id")); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, nil)
}

// this is a placeholder for more complex authorization behavior to come.
// for now, it just returns the first orgID from the user's permission set.
func orgIDFromReq(r *http.Request) (*platform.ID, error) {
	ctx := r.Context()
	a, err := context.GetAuthorizer(ctx)
	if err != nil {
		return nil, err
	}

	p, err := a.PermissionSet()
	if err != nil {
		return nil, err
	}

	for _, pp := range p {
		if pp.Resource.OrgID != nil {
			return pp.Resource.OrgID, nil
		}
	}

	return nil, &errors.Error{
		Code: errors.EInvalid,
		Msg:  "could not find an org",
	}
}
