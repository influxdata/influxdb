package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"path"

	"github.com/influxdata/influxdb"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// ScraperHandler represents an HTTP API handler for scraper targets.
type ScraperHandler struct {
	*httprouter.Router
	Logger                *zap.Logger
	ScraperStorageService influxdb.ScraperTargetStoreService
	BucketService         influxdb.BucketService
	OrganizationService   influxdb.OrganizationService
}

const (
	targetPath = "/api/v2/scrapers"
)

// NewScraperHandler returns a new instance of ScraperHandler.
func NewScraperHandler() *ScraperHandler {
	h := &ScraperHandler{
		Router: NewRouter(),
	}
	h.HandlerFunc("POST", targetPath, h.handlePostScraperTarget)
	h.HandlerFunc("GET", targetPath, h.handleGetScraperTargets)
	h.HandlerFunc("GET", targetPath+"/:id", h.handleGetScraperTarget)
	h.HandlerFunc("PATCH", targetPath+"/:id", h.handlePatchScraperTarget)
	h.HandlerFunc("DELETE", targetPath+"/:id", h.handleDeleteScraperTarget)
	return h
}

// handlePostScraperTarget is HTTP handler for the POST /api/v2/scrapers route.
func (h *ScraperHandler) handlePostScraperTarget(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeScraperTargetAddRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.ScraperStorageService.AddTarget(ctx, req); err != nil {
		EncodeError(ctx, err, w)
		return
	}
	resp, err := h.newTargetResponse(ctx, *req)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	if err := encodeResponse(ctx, w, http.StatusCreated, resp); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

// handleDeleteScraperTarget is the HTTP handler for the DELETE /api/v2/scrapers/:id route.
func (h *ScraperHandler) handleDeleteScraperTarget(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := decodeScraperTargetIDRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.ScraperStorageService.RemoveTarget(ctx, *id); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handlePatchScraperTarget is the HTTP handler for the PATCH /api/v2/scrapers/:id route.
func (h *ScraperHandler) handlePatchScraperTarget(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	update, err := decodeScraperTargetUpdateRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	target, err := h.ScraperStorageService.UpdateTarget(ctx, update)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	resp, err := h.newTargetResponse(ctx, *target)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, resp); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

func (h *ScraperHandler) handleGetScraperTarget(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := decodeScraperTargetIDRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	target, err := h.ScraperStorageService.GetTargetByID(ctx, *id)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	resp, err := h.newTargetResponse(ctx, *target)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, resp); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

// handleGetScraperTargets is the HTTP handler for the GET /api/v2/scrapers route.
func (h *ScraperHandler) handleGetScraperTargets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	targets, err := h.ScraperStorageService.ListTargets(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	resp, err := h.newListTargetsResponse(ctx, targets)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, resp); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

func decodeScraperTargetUpdateRequest(ctx context.Context, r *http.Request) (*influxdb.ScraperTarget, error) {
	update := &influxdb.ScraperTarget{}
	if err := json.NewDecoder(r.Body).Decode(update); err != nil {
		return nil, err
	}
	id, err := decodeScraperTargetIDRequest(ctx, r)
	if err != nil {
		return nil, err
	}
	update.ID = *id
	return update, nil
}

func decodeScraperTargetAddRequest(ctx context.Context, r *http.Request) (*influxdb.ScraperTarget, error) {
	req := &influxdb.ScraperTarget{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		return nil, err
	}
	return req, nil
}

func decodeScraperTargetIDRequest(ctx context.Context, r *http.Request) (*influxdb.ID, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &i, nil
}

// ScraperService connects to Influx via HTTP using tokens to manage scraper targets.
type ScraperService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
	// OpPrefix is for update invalid ops
	OpPrefix string
}

// ListTargets returns a list of all scraper targets.
func (s *ScraperService) ListTargets(ctx context.Context) ([]influxdb.ScraperTarget, error) {
	url, err := newURL(s.Addr, targetPath)
	if err != nil {
		return nil, err
	}

	query := url.Query()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}

	req.URL.RawQuery = query.Encode()
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := CheckError(resp, true); err != nil {
		return nil, err
	}

	var targetsResp getTargetsResponse
	if err := json.NewDecoder(resp.Body).Decode(&targetsResp); err != nil {
		return nil, err
	}

	targets := make([]influxdb.ScraperTarget, len(targetsResp.Targets))
	for k, v := range targetsResp.Targets {
		targets[k] = v.ScraperTarget
	}

	return targets, nil
}

// UpdateTarget updates a single scraper target with changeset.
// Returns the new target state after update.
func (s *ScraperService) UpdateTarget(ctx context.Context, update *influxdb.ScraperTarget) (*influxdb.ScraperTarget, error) {
	if !update.ID.Valid() {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Op:   s.OpPrefix + influxdb.OpUpdateTarget,
			Msg:  "id is invalid",
		}
	}
	url, err := newURL(s.Addr, targetIDPath(update.ID))
	if err != nil {
		return nil, err
	}

	octets, err := json.Marshal(update)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PATCH", url.String(), bytes.NewReader(octets))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)
	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp, true); err != nil {
		return nil, err
	}
	var targetResp targetResponse
	if err := json.NewDecoder(resp.Body).Decode(&targetResp); err != nil {
		return nil, err
	}

	return &targetResp.ScraperTarget, nil
}

// AddTarget creates a new scraper target and sets target.ID with the new identifier.
func (s *ScraperService) AddTarget(ctx context.Context, target *influxdb.ScraperTarget) error {
	url, err := newURL(s.Addr, targetPath)
	if err != nil {
		return err
	}

	if !target.OrgID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "org id is invalid",
			Op:   s.OpPrefix + influxdb.OpAddTarget,
		}
	}
	if !target.BucketID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "bucket id is invalid",
			Op:   s.OpPrefix + influxdb.OpAddTarget,
		}
	}

	octets, err := json.Marshal(target)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp, true); err != nil {
		return err
	}

	targetResp := new(targetResponse)
	if err := json.NewDecoder(resp.Body).Decode(targetResp); err != nil {
		return err
	}

	return nil
}

// RemoveTarget removes a scraper target by ID.
func (s *ScraperService) RemoveTarget(ctx context.Context, id influxdb.ID) error {
	url, err := newURL(s.Addr, targetIDPath(id))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", url.String(), nil)
	if err != nil {
		return err
	}
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckErrorStatus(http.StatusNoContent, resp, true)
}

// GetTargetByID returns a single target by ID.
func (s *ScraperService) GetTargetByID(ctx context.Context, id influxdb.ID) (*influxdb.ScraperTarget, error) {
	url, err := newURL(s.Addr, targetIDPath(id))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp, true); err != nil {
		return nil, err
	}

	var targetResp targetResponse
	if err := json.NewDecoder(resp.Body).Decode(&targetResp); err != nil {
		return nil, err
	}

	return &targetResp.ScraperTarget, nil
}

func targetIDPath(id influxdb.ID) string {
	return path.Join(targetPath, id.String())
}

type getTargetsLinks struct {
	Self string `json:"self"`
}

type getTargetsResponse struct {
	Links   getTargetsLinks  `json:"links"`
	Targets []targetResponse `json:"configurations"`
}

type targetLinks struct {
	Self         string `json:"self"`
	Bucket       string `json:"bucket,omitempty"`
	Organization string `json:"organization,omitempty"`
}

type targetResponse struct {
	influxdb.ScraperTarget
	Organization string      `json:"organization,omitempty"`
	Bucket       string      `json:"bucket,omitempty"`
	Links        targetLinks `json:"links"`
}

func (h *ScraperHandler) newListTargetsResponse(ctx context.Context, targets []influxdb.ScraperTarget) (getTargetsResponse, error) {
	res := getTargetsResponse{
		Links: getTargetsLinks{
			Self: targetPath,
		},
		Targets: make([]targetResponse, 0, len(targets)),
	}

	for _, target := range targets {
		resp, err := h.newTargetResponse(ctx, target)
		if err != nil {
			return res, err
		}
		res.Targets = append(res.Targets, resp)
	}

	return res, nil
}

func (h *ScraperHandler) newTargetResponse(ctx context.Context, target influxdb.ScraperTarget) (targetResponse, error) {
	res := targetResponse{
		Links: targetLinks{
			Self: targetIDPath(target.ID),
		},
		ScraperTarget: target,
	}
	bucket, err := h.BucketService.FindBucketByID(ctx, target.BucketID)
	if err == nil {
		res.Bucket = bucket.Name
		res.BucketID = bucket.ID
		res.Links.Bucket = bucketIDPath(bucket.ID)
	} else {
		res.BucketID = influxdb.InvalidID()
	}

	org, err := h.OrganizationService.FindOrganizationByID(ctx, target.OrgID)
	if err == nil {
		res.Organization = org.Name
		res.OrgID = org.ID
		res.Links.Organization = organizationIDPath(org.ID)
	} else {
		res.OrgID = influxdb.InvalidID()
	}

	return res, nil
}
