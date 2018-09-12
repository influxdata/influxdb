package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/platform"
	"github.com/julienschmidt/httprouter"
)

// SetupHandler represents an HTTP API handler for onboarding setup.
type SetupHandler struct {
	*httprouter.Router

	OnboardingService platform.OnboardingService
}

// NewSetupHandler returns a new instance of SetupHandler.
func NewSetupHandler() *SetupHandler {
	h := &SetupHandler{
		Router: httprouter.New(),
	}
	h.HandlerFunc("POST", "/setup", h.handlePostSetup)
	h.HandlerFunc("GET", "/setup", h.isOnboarding)
	return h
}

// isOnboarding is the HTTP handler for the GET /setup route.
// returns true/false
func (h *SetupHandler) isOnboarding(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	result, err := h.OnboardingService.IsOnboarding(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"allowed": %v}`, result)
}

func (h *SetupHandler) handlePostSetup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostSetupRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	results, err := h.OnboardingService.Generate(ctx, req)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	if err := encodeResponse(ctx, w, http.StatusCreated, newOnboardingResponse(results)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type onboardingResponse struct {
	User         *userResponse   `json:"user"`
	Bucket       *bucketResponse `json:"bucket"`
	Organization *orgResponse    `json:"org"`
	Auth         *authResponse   `json:"auth"`
}

func newOnboardingResponse(results *platform.OnboardingResults) *onboardingResponse {
	return &onboardingResponse{
		User:         newUserResponse(results.User),
		Bucket:       newBucketResponse(results.Bucket),
		Organization: newOrgResponse(results.Org),
		Auth:         newAuthResponse(results.Auth),
	}
}

func decodePostSetupRequest(ctx context.Context, r *http.Request) (*platform.OnboardingRequest, error) {
	req := &platform.OnboardingRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		return nil, err
	}

	return req, nil
}
