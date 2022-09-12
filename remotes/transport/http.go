package transport

import (
	"context"
	"net/http"
	"strings"

	"github.com/influxdata/influxdb/v2/kv"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/prometheus/client_golang/prometheus"
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

	errForeignKey = &errors.Error{
		Code: errors.EInternal,
		Msg:  "remote-connection cannot be deleted as a replication references it",
	}
)

type RemoteConnectionService interface {
	// ListRemoteConnections returns all info about registered remote InfluxDB connections matching a filter.
	ListRemoteConnections(context.Context, influxdb.RemoteConnectionListFilter) (*influxdb.RemoteConnections, error)

	// CreateRemoteConnection registers a new remote InfluxDB connection.
	CreateRemoteConnection(context.Context, influxdb.CreateRemoteConnectionRequest) (*influxdb.RemoteConnection, error)

	// GetRemoteConnection returns metadata about the remote InfluxDB connection with the given ID.
	GetRemoteConnection(context.Context, platform.ID) (*influxdb.RemoteConnection, error)

	// UpdateRemoteConnection updates the settings for the remote InfluxDB connection with the given ID.
	UpdateRemoteConnection(context.Context, platform.ID, influxdb.UpdateRemoteConnectionRequest) (*influxdb.RemoteConnection, error)

	// DeleteRemoteConnection deletes all info for the remote InfluxDB connection with the given ID.
	DeleteRemoteConnection(context.Context, platform.ID) error
}

type RemoteConnectionHandler struct {
	chi.Router

	log *zap.Logger
	api *kithttp.API

	remotesService RemoteConnectionService
}

func NewInstrumentedRemotesHandler(log *zap.Logger, reg prometheus.Registerer, kv kv.Store, svc RemoteConnectionService) *RemoteConnectionHandler {
	// Collect telemetry.
	svc = newTelemetryCollectingService(kv, svc)
	// Collect metrics.
	svc = newMetricCollectingService(reg, svc)
	// Wrap logging.
	svc = newLoggingService(log, svc)
	// Wrap authz.
	svc = newAuthCheckingService(svc)

	return newRemoteConnectionHandler(log, svc)
}

func newRemoteConnectionHandler(log *zap.Logger, svc RemoteConnectionService) *RemoteConnectionHandler {
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
	)

	r.Route("/", func(r chi.Router) {
		r.Get("/", h.handleGetRemotes)
		r.Post("/", h.handlePostRemote)

		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", h.handleGetRemote)
			r.Patch("/", h.handlePatchRemote)
			r.Delete("/", h.handleDeleteRemote)
		})
	})

	h.Router = r
	return h
}

func (h *RemoteConnectionHandler) Prefix() string {
	return prefixRemotes
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

	var req influxdb.CreateRemoteConnectionRequest
	if err := h.api.DecodeJSON(r.Body, &req); err != nil {
		h.api.Err(w, r, err)
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

	var req influxdb.UpdateRemoteConnectionRequest
	if err := h.api.DecodeJSON(r.Body, &req); err != nil {
		h.api.Err(w, r, err)
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
		if strings.Contains(strings.ToLower(err.Error()), "foreign key constraint failed") {
			h.api.Err(w, r, errForeignKey)
		} else {
			h.api.Err(w, r, err)
		}
		return
	}
	h.api.Respond(w, r, http.StatusNoContent, nil)
}
