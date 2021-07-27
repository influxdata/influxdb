package secret

import (
	"fmt"
	"net/http"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

type handler struct {
	log *zap.Logger
	svc influxdb.SecretService
	api *kithttp.API

	idLookupKey string
}

// NewHandler creates a new handler for the secret service
func NewHandler(log *zap.Logger, idLookupKey string, svc influxdb.SecretService) http.Handler {
	h := &handler{
		log: log,
		svc: svc,
		api: kithttp.NewAPI(kithttp.WithLog(log)),

		idLookupKey: idLookupKey,
	}

	r := chi.NewRouter()

	r.Get("/", h.handleGetSecrets)
	r.Patch("/", h.handlePatchSecrets)
	r.Delete("/{secretID}", h.handleDeleteSecret)
	r.Post("/delete", h.handleDeleteSecrets) // deprecated
	return r
}

// handleGetSecrets is the HTTP handler for the GET /api/v2/orgs/:id/secrets route.
func (h *handler) handleGetSecrets(w http.ResponseWriter, r *http.Request) {
	orgID, err := h.decodeOrgID(r)
	if err != nil {
		h.api.Err(w, r, err)
	}

	ks, err := h.svc.GetSecretKeys(r.Context(), orgID)
	if err != nil && errors.ErrorCode(err) != errors.ENotFound {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, newSecretsResponse(orgID, ks))
}

type secretsResponse struct {
	Links   map[string]string `json:"links"`
	Secrets []string          `json:"secrets"`
}

func newSecretsResponse(orgID platform.ID, ks []string) *secretsResponse {
	if ks == nil {
		ks = []string{}
	}
	return &secretsResponse{
		Links: map[string]string{
			"org":  fmt.Sprintf("/api/v2/orgs/%s", orgID),
			"self": fmt.Sprintf("/api/v2/orgs/%s/secrets", orgID),
		},
		Secrets: ks,
	}
}

// handleGetPatchSecrets is the HTTP handler for the PATCH /api/v2/orgs/:id/secrets route.
func (h *handler) handlePatchSecrets(w http.ResponseWriter, r *http.Request) {
	orgID, err := h.decodeOrgID(r)
	if err != nil {
		h.api.Err(w, r, err)
	}

	var secrets map[string]string
	if err := h.api.DecodeJSON(r.Body, &secrets); err != nil {
		h.api.Err(w, r, err)
		return
	}

	if err := h.svc.PatchSecrets(r.Context(), orgID, secrets); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusNoContent, nil)
}

type secretsDeleteBody struct {
	Secrets []string `json:"secrets"`
}

// handleDeleteSecrets is the HTTP handler for the POST /api/v2/orgs/:id/secrets/delete route.
// deprecated.
func (h *handler) handleDeleteSecrets(w http.ResponseWriter, r *http.Request) {
	orgID, err := h.decodeOrgID(r)
	if err != nil {
		h.api.Err(w, r, err)
	}

	var reqBody secretsDeleteBody

	if err := h.api.DecodeJSON(r.Body, &reqBody); err != nil {
		h.api.Err(w, r, err)
		return
	}

	if err := h.svc.DeleteSecret(r.Context(), orgID, reqBody.Secrets...); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusNoContent, nil)
}

// handleDeleteSecret is the HTTP handler for the DELETE /api/v2/orgs/:id/secrets/:id route.
func (h *handler) handleDeleteSecret(w http.ResponseWriter, r *http.Request) {
	orgID, err := h.decodeOrgID(r)
	if err != nil {
		h.api.Err(w, r, err)
	}

	if err := h.svc.DeleteSecret(r.Context(), orgID, chi.URLParam(r, "secretID")); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusNoContent, nil)
}

func (h *handler) decodeOrgID(r *http.Request) (platform.ID, error) {
	org := chi.URLParam(r, h.idLookupKey)
	if org == "" {
		return platform.InvalidID(), &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		}
	}
	id, err := platform.IDFromString(org)
	if err != nil {
		return platform.InvalidID(), err
	}
	return *id, nil
}
