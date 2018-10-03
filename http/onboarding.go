package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/influxdata/platform"
	"github.com/julienschmidt/httprouter"
)

// SetupHandler represents an HTTP API handler for onboarding setup.
type SetupHandler struct {
	*httprouter.Router

	OnboardingService platform.OnboardingService
}

const (
	setupPath = "/api/v2/setup"
)

// NewSetupHandler returns a new instance of SetupHandler.
func NewSetupHandler() *SetupHandler {
	h := &SetupHandler{
		Router: httprouter.New(),
	}
	h.HandlerFunc("POST", setupPath, h.handlePostSetup)
	h.HandlerFunc("GET", setupPath, h.isOnboarding)
	return h
}

type isOnboardingResponse struct {
	Allowed bool `json:"allowed"`
}

// isOnboarding is the HTTP handler for the GET /api/v2/setup route.
func (h *SetupHandler) isOnboarding(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	result, err := h.OnboardingService.IsOnboarding(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	if err := encodeResponse(ctx, w, http.StatusOK, isOnboardingResponse{result}); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

// isOnboarding is the HTTP handler for the POST /api/v2/setup route.
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

// SetupService connects to Influx via HTTP to perform onboarding operations
type SetupService struct {
	Addr               string
	InsecureSkipVerify bool
}

// IsOnboarding determine if onboarding request is allowed.
func (s *SetupService) IsOnboarding(ctx context.Context) (bool, error) {
	u, err := newURL(s.Addr, setupPath)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return false, err
	}
	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return false, err
	}
	if err := CheckError(resp); err != nil {
		return false, err
	}
	defer resp.Body.Close()
	var ir isOnboardingResponse
	if err := json.NewDecoder(resp.Body).Decode(&ir); err != nil {
		return false, err
	}
	return ir.Allowed, nil
}

// Generate OnboardingResults.
func (s *SetupService) Generate(ctx context.Context, or *platform.OnboardingRequest) (*platform.OnboardingResults, error) {
	u, err := newURL(s.Addr, setupPath)
	if err != nil {
		return nil, err
	}
	octets, err := json.Marshal(or)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(octets))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp, true); err != nil {
		return nil, err
	}

	var oResp onboardingResponse
	if err := json.NewDecoder(resp.Body).Decode(&oResp); err != nil {
		return nil, err
	}

	bkt, err := oResp.Bucket.toPlatform()
	if err != nil {
		return nil, err
	}
	return &platform.OnboardingResults{
		User:   &oResp.User.User,
		Auth:   &oResp.Auth.Authorization,
		Org:    &oResp.Organization.Organization,
		Bucket: bkt,
	}, nil
}
