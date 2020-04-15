package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/influxdata/httprouter"
	platform "github.com/influxdata/influxdb/v2"
	"go.uber.org/zap"
)

// SetupBackend is all services and associated parameters required to construct
// the SetupHandler.
type SetupBackend struct {
	platform.HTTPErrorHandler
	log               *zap.Logger
	OnboardingService platform.OnboardingService
}

// NewSetupBackend returns a new instance of SetupBackend.
func NewSetupBackend(log *zap.Logger, b *APIBackend) *SetupBackend {
	return &SetupBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,
		// OnboardingService: b.OnboardingService,
	}
}

// SetupHandler represents an HTTP API handler for onboarding setup.
type SetupHandler struct {
	*httprouter.Router
	platform.HTTPErrorHandler
	log *zap.Logger

	OnboardingService platform.OnboardingService
}

const (
	prefixSetup = "/api/v2/setup"
)

// NewSetupHandler returns a new instance of SetupHandler.
func NewSetupHandler(log *zap.Logger, b *SetupBackend) *SetupHandler {
	h := &SetupHandler{
		Router:            NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler:  b.HTTPErrorHandler,
		log:               log,
		OnboardingService: b.OnboardingService,
	}
	h.HandlerFunc("POST", prefixSetup, h.handlePostSetup)
	h.HandlerFunc("GET", prefixSetup, h.isOnboarding)
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
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Onboarding eligibility check finished", zap.String("result", fmt.Sprint(result)))

	if err := encodeResponse(ctx, w, http.StatusOK, isOnboardingResponse{result}); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

// isOnboarding is the HTTP handler for the POST /api/v2/setup route.
func (h *SetupHandler) handlePostSetup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePostSetupRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	results, err := h.OnboardingService.OnboardInitialUser(ctx, req)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Onboarding setup completed", zap.String("results", fmt.Sprint(results)))

	if err := encodeResponse(ctx, w, http.StatusCreated, newOnboardingResponse(results)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type onboardingResponse struct {
	User         *UserResponse   `json:"user"`
	Bucket       *bucketResponse `json:"bucket"`
	Organization orgResponse     `json:"org"`
	Auth         *authResponse   `json:"auth"`
}

func newOnboardingResponse(results *platform.OnboardingResults) *onboardingResponse {
	// when onboarding the permissions are for all resources and no
	// specifically named resources.  Therefore, there is no need to
	// lookup the name.
	ps := make([]permissionResponse, len(results.Auth.Permissions))
	for i, p := range results.Auth.Permissions {
		ps[i] = permissionResponse{
			Action: p.Action,
			Resource: resourceResponse{
				Resource: p.Resource,
			},
		}
	}
	return &onboardingResponse{
		User:         newUserResponse(results.User),
		Bucket:       NewBucketResponse(results.Bucket, []*platform.Label{}),
		Organization: newOrgResponse(*results.Org),
		Auth:         newAuthResponse(results.Auth, results.Org, results.User, ps),
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
	u, err := NewURL(s.Addr, prefixSetup)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return false, err
	}
	hc := NewClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return false, err
	}
	var ir isOnboardingResponse
	if err := json.NewDecoder(resp.Body).Decode(&ir); err != nil {
		return false, err
	}
	return ir.Allowed, nil
}

// OnboardInitialUser OnboardingResults.
func (s *SetupService) OnboardInitialUser(ctx context.Context, or *platform.OnboardingRequest) (*platform.OnboardingResults, error) {
	u, err := NewURL(s.Addr, prefixSetup)
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
	hc := NewClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var oResp onboardingResponse
	if err := json.NewDecoder(resp.Body).Decode(&oResp); err != nil {
		return nil, err
	}

	bkt, err := oResp.Bucket.toInfluxDB()
	if err != nil {
		return nil, err
	}
	return &platform.OnboardingResults{
		Org:    &oResp.Organization.Organization,
		User:   &oResp.User.User,
		Auth:   oResp.Auth.toPlatform(),
		Bucket: bkt,
	}, nil
}

func (s *SetupService) OnboardUser(ctx context.Context, or *platform.OnboardingRequest) (*platform.OnboardingResults, error) {
	return nil, errors.New("not yet implemented")
}
