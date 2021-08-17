package transport

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

const (
	prefixRemotes = "/api/v2/remotes"
)

var (
	errBadOrg = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "invalid or missing org ID",
	}

	errBadId = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "remote-connection ID is invalid",
	}
)

type RemoteConnectionHandler struct {
	chi.Router

	log *zap.Logger
	api *kithttp.API

	remotesService influxdb.RemoteConnectionService
}

func NewRemoteConnectionHandler(log *zap.Logger, svc influxdb.RemoteConnectionService) *RemoteConnectionHandler {
	h := &RemoteConnectionHandler{
		log:            log,
		api:            kithttp.NewAPI(kithttp.WithLog(log)),
		remotesService: svc,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
		h.mwRemotesFlag, // Temporary, remove when feature flag for remote connections is perma-enabled.
	)

	r.Route("/", func(r chi.Router) {
		r.Get("/", h.handleGetRemotes)
		r.Post("/", h.handlePostRemote)

		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", h.handleGetRemote)
			r.Patch("/", h.handlePatchRemote)
			r.Delete("/", h.handleDeleteRemote)
			r.Post("/validate", h.handleValidateRemote)
		})
	})

	h.Router = r
	return h
}

func (h *RemoteConnectionHandler) Prefix() string {
	return prefixRemotes
}

func (h *RemoteConnectionHandler) mwRemotesFlag(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flags := feature.FlagsFromContext(r.Context())

		if flagVal, ok := flags[feature.ReplicationStreamBackend().Key()]; !ok || !flagVal.(bool) {
			h.api.Respond(w, r, http.StatusNotFound, nil)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (h *RemoteConnectionHandler) handleGetRemotes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// orgID is required for listing remotes.
	orgID := q.Get("orgID")
	o, err := platform.IDFromString(orgID)
	if err != nil {
		h.api.Err(w, r, errBadOrg)
		return
	}

	// name and remoteURL are optional additional filters.
	name := q.Get("name")
	remoteURL := q.Get("remoteURL")

	filters := influxdb.RemoteConnectionListFilter{OrgID: *o}
	if name != "" {
		filters.Name = &name
	}
	if remoteURL != "" {
		filters.RemoteURL = &remoteURL
	}

	rs, err := h.remotesService.ListRemoteConnections(r.Context(), filters)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.api.Respond(w, r, http.StatusOK, rs)
}

func (h *RemoteConnectionHandler) handlePostRemote(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	validate := q.Get("validate") == "true"
	var req influxdb.CreateRemoteConnectionRequest
	if err := h.api.DecodeJSON(r.Body, &req); err != nil {
		h.api.Err(w, r, err)
		return
	}

	if validate {
		if err := h.remotesService.ValidateNewRemoteConnection(ctx, req); err != nil {
			h.api.Err(w, r, err)
			return
		}
		h.api.Respond(w, r, http.StatusNoContent, nil)
		return
	}

	remote, err := h.remotesService.CreateRemoteConnection(ctx, req)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.api.Respond(w, r, http.StatusCreated, remote)
}

func (h *RemoteConnectionHandler) handleGetRemote(w http.ResponseWriter, r *http.Request) {
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, errBadId)
		return
	}

	remote, err := h.remotesService.GetRemoteConnection(r.Context(), *id)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.api.Respond(w, r, http.StatusOK, remote)
}

func (h *RemoteConnectionHandler) handlePatchRemote(w http.ResponseWriter, r *http.Request) {
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, errBadId)
		return
	}

	ctx := r.Context()
	q := r.URL.Query()

	validate := q.Get("validate") == "true"
	var req influxdb.UpdateRemoteConnectionRequest
	if err := h.api.DecodeJSON(r.Body, &req); err != nil {
		h.api.Err(w, r, err)
		return
	}

	if validate {
		if err := h.remotesService.ValidateUpdatedRemoteConnection(ctx, *id, req); err != nil {
			h.api.Err(w, r, err)
			return
		}
		h.api.Respond(w, r, http.StatusNoContent, false)
		return
	}

	remote, err := h.remotesService.UpdateRemoteConnection(ctx, *id, req)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.api.Respond(w, r, http.StatusOK, remote)
}

func (h *RemoteConnectionHandler) handleDeleteRemote(w http.ResponseWriter, r *http.Request) {
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, errBadId)
		return
	}

	if err := h.remotesService.DeleteRemoteConnection(r.Context(), *id); err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.api.Respond(w, r, http.StatusNoContent, nil)
}

func (h *RemoteConnectionHandler) handleValidateRemote(w http.ResponseWriter, r *http.Request) {
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, errBadId)
		return
	}

	if err := h.remotesService.ValidateRemoteConnection(r.Context(), *id); err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.api.Respond(w, r, http.StatusNoContent, nil)
}
