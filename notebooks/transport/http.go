package transport

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	feature "github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	notebooks "github.com/influxdata/influxdb/v2/notebooks/service"
	"go.uber.org/zap"
)

const (
	prefixNotebooks = "/api/v2private/flows"
	errMissingParam = "url missing %s"
	errInvalidParam = "url %s is invalid"
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

	r.Route("/orgs/{orgID}/flows", func(r chi.Router) {
		r.Get("/", h.handleGetNotebooks)
		r.Post("/", h.handleCreateNotebook)

		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", h.handleGetNotebook)
			r.Patch("/", h.handlePatchNotebook)
			r.Delete("/", h.handleDeleteNotebook)
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

		if !flags["notebooks"].(bool) {
			h.api.Respond(w, r, http.StatusNoContent, nil)
			return
		}

		next.ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}

// get a list of all notebooks for an org
func (h *NotebookHandler) handleGetNotebooks(w http.ResponseWriter, r *http.Request) {
	orgID, err := getIDfromReq(r, "orgID")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	// Demo data - for development purposes.
	d := map[string][]notebooks.Notebook{}
	d["flows"] = demoNotebooks(3, *orgID)

	h.api.Respond(w, r, http.StatusOK, d)
}

// create a single notebook
func (h *NotebookHandler) handleCreateNotebook(w http.ResponseWriter, r *http.Request) {
	orgID, err := getIDfromReq(r, "orgID")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	// Demo data - just return the body from the request with a generated ID
	b := notebooks.Notebook{}
	if err := h.api.DecodeJSON(r.Body, &b); err != nil {
		h.api.Err(w, r, err)
		return
	}
	b.OrgID = *orgID                                                   // this isn't necessary with the demo data, but keeping it here for future
	id, _ := platform.IDFromString(strconv.Itoa(1000000000000000 + 1)) // give it an ID from the getNotebooks list so that the UI doesn't break
	b.ID = *id

	h.api.Respond(w, r, http.StatusOK, b)
}

// get a single notebook
func (h *NotebookHandler) handleGetNotebook(w http.ResponseWriter, r *http.Request) {
	orgID, err := getIDfromReq(r, "orgID")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	notebookID, err := getIDfromReq(r, "id")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	// Demo data - for development purposes.
	d := demoNotebook(*orgID, *notebookID)

	h.api.Respond(w, r, http.StatusOK, d)
}

// update a single notebook
func (h *NotebookHandler) handlePatchNotebook(w http.ResponseWriter, r *http.Request) {
	orgID, err := getIDfromReq(r, "orgID")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	id, err := getIDfromReq(r, "id")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	// Demo data - just return the body from the request with a generated ID
	b := notebooks.Notebook{}
	if err := h.api.DecodeJSON(r.Body, &b); err != nil {
		h.api.Err(w, r, err)
		return
	}
	b.OrgID = *orgID // this isn't necessary with the demo data, but keeping it here for future
	b.ID = *id       // ditto

	h.api.Respond(w, r, http.StatusOK, b)
}

// delete a single notebook
// for now, just respond with 200 unless there is a problem with the orgID or notebook ID
func (h *NotebookHandler) handleDeleteNotebook(w http.ResponseWriter, r *http.Request) {
	_, err := getIDfromReq(r, "orgID")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	_, err = getIDfromReq(r, "id")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, nil)
}

func getIDfromReq(r *http.Request, param string) (*platform.ID, error) {
	id := chi.URLParam(r, param)
	if id == "" {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  fmt.Sprintf(errMissingParam, param),
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  fmt.Sprintf(errInvalidParam, param),
		}
	}

	return &i, nil
}
