package transport

import (
	"net/http"

	"github.com/go-chi/chi"
	feature "github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/notebooks/service"
)

// notebookFlag is middleware for returning no content if the notebooks feature
// flag is set to false. remove this middleware when the feature flag is removed.
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

// mwRequireOrgID gets the org id from the URL query parameter and sets it on the
// request context. if there is not a valid org id, the server will return an error.
func (h *NotebookHandler) mwRequireOrgID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		orgID := r.URL.Query().Get("orgID")
		o, err := platform.IDFromString(orgID)
		if err != nil {
			h.api.Err(w, r, errBadOrg)
			return
		}

		next.ServeHTTP(w, r.WithContext(withOrgID(r.Context(), *o)))
	})
}

func (h *NotebookHandler) mwRequireNotebookID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, err := platform.IDFromString(chi.URLParam(r, "id"))
		if err != nil {
			h.api.Err(w, r, errBadId)
			return
		}

		next.ServeHTTP(w, r.WithContext(withNotebookID(r.Context(), *id)))
	})
}

func (h *NotebookHandler) mwNotebookReqBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := h.decodeNotebookReqBody(r)
		if err != nil {
			h.api.Err(w, r, err)
			return
		}

		next.ServeHTTP(w, r.WithContext(withNotebookReqBody(r.Context(), b)))
	})
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
