package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	pctx "github.com/influxdata/influxdb/v2/context"
	"go.uber.org/zap"
)

const (
	prefixOrganizations = "/api/v2/orgs"
	prefixBuckets       = "/api/v2/buckets"
)

// ScraperBackend is all services and associated parameters required to construct
// the ScraperHandler.
type ScraperBackend struct {
	influxdb.HTTPErrorHandler
	log *zap.Logger

	ScraperStorageService      influxdb.ScraperTargetStoreService
	BucketService              influxdb.BucketService
	OrganizationService        influxdb.OrganizationService
	UserService                influxdb.UserService
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
}

// NewScraperBackend returns a new instance of ScraperBackend.
func NewScraperBackend(log *zap.Logger, b *APIBackend) *ScraperBackend {
	return &ScraperBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		ScraperStorageService:      b.ScraperTargetStoreService,
		BucketService:              b.BucketService,
		OrganizationService:        b.OrganizationService,
		UserService:                b.UserService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
	}
}

// ScraperHandler represents an HTTP API handler for scraper targets.
type ScraperHandler struct {
	*httprouter.Router
	influxdb.HTTPErrorHandler
	log                        *zap.Logger
	UserService                influxdb.UserService
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
	ScraperStorageService      influxdb.ScraperTargetStoreService
	BucketService              influxdb.BucketService
	OrganizationService        influxdb.OrganizationService
}

const (
	prefixTargets          = "/api/v2/scrapers"
	targetsIDMembersPath   = prefixTargets + "/:id/members"
	targetsIDMembersIDPath = prefixTargets + "/:id/members/:userID"
	targetsIDOwnersPath    = prefixTargets + "/:id/owners"
	targetsIDOwnersIDPath  = prefixTargets + "/:id/owners/:userID"
	targetsIDLabelsPath    = prefixTargets + "/:id/labels"
	targetsIDLabelsIDPath  = prefixTargets + "/:id/labels/:lid"
)

// NewScraperHandler returns a new instance of ScraperHandler.
func NewScraperHandler(log *zap.Logger, b *ScraperBackend) *ScraperHandler {
	h := &ScraperHandler{
		Router:                     NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        log,
		UserService:                b.UserService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		ScraperStorageService:      b.ScraperStorageService,
		BucketService:              b.BucketService,
		OrganizationService:        b.OrganizationService,
	}
	h.HandlerFunc("POST", prefixTargets, h.handlePostScraperTarget)
	h.HandlerFunc("GET", prefixTargets, h.handleGetScraperTargets)
	h.HandlerFunc("GET", prefixTargets+"/:id", h.handleGetScraperTarget)
	h.HandlerFunc("PATCH", prefixTargets+"/:id", h.handlePatchScraperTarget)
	h.HandlerFunc("DELETE", prefixTargets+"/:id", h.handleDeleteScraperTarget)

	memberBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               influxdb.ScraperResourceType,
		UserType:                   influxdb.Member,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", targetsIDMembersPath, newPostMemberHandler(memberBackend))
	h.HandlerFunc("GET", targetsIDMembersPath, newGetMembersHandler(memberBackend))
	h.HandlerFunc("DELETE", targetsIDMembersIDPath, newDeleteMemberHandler(memberBackend))

	ownerBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               influxdb.ScraperResourceType,
		UserType:                   influxdb.Owner,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", targetsIDOwnersPath, newPostMemberHandler(ownerBackend))
	h.HandlerFunc("GET", targetsIDOwnersPath, newGetMembersHandler(ownerBackend))
	h.HandlerFunc("DELETE", targetsIDOwnersIDPath, newDeleteMemberHandler(ownerBackend))

	labelBackend := &LabelBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              b.log.With(zap.String("handler", "label")),
		LabelService:     b.LabelService,
		ResourceType:     influxdb.ScraperResourceType,
	}
	h.HandlerFunc("GET", targetsIDLabelsPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", targetsIDLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", targetsIDLabelsIDPath, newDeleteLabelHandler(labelBackend))

	return h
}

// handlePostScraperTarget is HTTP handler for the POST /api/v2/scrapers route.
func (h *ScraperHandler) handlePostScraperTarget(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeScraperTargetAddRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auth, err := pctx.GetAuthorizer(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.ScraperStorageService.AddTarget(ctx, req, auth.GetUserID()); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Scraper created", zap.String("scraper", fmt.Sprint(req)))

	resp, err := h.newTargetResponse(ctx, *req)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	if err := encodeResponse(ctx, w, http.StatusCreated, resp); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

// handleDeleteScraperTarget is the HTTP handler for the DELETE /api/v2/scrapers/:id route.
func (h *ScraperHandler) handleDeleteScraperTarget(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, err := decodeScraperTargetIDRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.ScraperStorageService.RemoveTarget(ctx, *id); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Scraper deleted", zap.String("scraperTargetID", fmt.Sprint(id)))

	w.WriteHeader(http.StatusNoContent)
}

// handlePatchScraperTarget is the HTTP handler for the PATCH /api/v2/scrapers/:id route.
func (h *ScraperHandler) handlePatchScraperTarget(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	update, err := decodeScraperTargetUpdateRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auth, err := pctx.GetAuthorizer(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	target, err := h.ScraperStorageService.UpdateTarget(ctx, update, auth.GetUserID())
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Scraper updated", zap.String("scraper", fmt.Sprint(target)))

	resp, err := h.newTargetResponse(ctx, *target)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, resp); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

func (h *ScraperHandler) handleGetScraperTarget(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, err := decodeScraperTargetIDRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	target, err := h.ScraperStorageService.GetTargetByID(ctx, *id)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Scraper retrieved", zap.String("scraper", fmt.Sprint(target)))

	resp, err := h.newTargetResponse(ctx, *target)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, resp); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getScraperTargetsRequest struct {
	filter influxdb.ScraperTargetFilter
}

func decodeScraperTargetsRequest(ctx context.Context, r *http.Request) (*getScraperTargetsRequest, error) {
	qp := r.URL.Query()
	req := &getScraperTargetsRequest{}

	initialID := influxdb.InvalidID()
	if ids, ok := qp["id"]; ok {
		req.filter.IDs = make(map[influxdb.ID]bool)
		for _, id := range ids {
			i := initialID
			if err := i.DecodeFromString(id); err != nil {
				return nil, err
			}
			req.filter.IDs[i] = false
		}
	}
	if name := qp.Get("name"); name != "" {
		req.filter.Name = &name
	}
	if orgID := qp.Get("orgID"); orgID != "" {
		id := influxdb.InvalidID()
		if err := id.DecodeFromString(orgID); err != nil {
			return nil, err
		}
		req.filter.OrgID = &id
	} else if org := qp.Get("org"); org != "" {
		req.filter.Org = &org
	}

	return req, nil
}

// handleGetScraperTargets is the HTTP handler for the GET /api/v2/scrapers route.
func (h *ScraperHandler) handleGetScraperTargets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeScraperTargetsRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	targets, err := h.ScraperStorageService.ListTargets(ctx, req.filter)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Scrapers retrieved", zap.String("scrapers", fmt.Sprint(targets)))

	resp, err := h.newListTargetsResponse(ctx, targets)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, resp); err != nil {
		logEncodingError(h.log, r, err)
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
func (s *ScraperService) ListTargets(ctx context.Context, filter influxdb.ScraperTargetFilter) ([]influxdb.ScraperTarget, error) {
	url, err := NewURL(s.Addr, prefixTargets)
	if err != nil {
		return nil, err
	}

	query := url.Query()
	if filter.IDs != nil {
		for id := range filter.IDs {
			query.Add("id", id.String())
		}
	}
	if filter.Name != nil {
		query.Set("name", *filter.Name)
	}
	if filter.OrgID != nil {
		query.Set("orgID", filter.OrgID.String())
	}
	if filter.Org != nil {
		query.Set("org", *filter.Org)
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}

	req.URL.RawQuery = query.Encode()
	SetToken(s.Token, req)

	hc := NewClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := CheckError(resp); err != nil {
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
func (s *ScraperService) UpdateTarget(ctx context.Context, update *influxdb.ScraperTarget, userID influxdb.ID) (*influxdb.ScraperTarget, error) {
	if !update.ID.Valid() {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Op:   s.OpPrefix + influxdb.OpUpdateTarget,
			Msg:  "provided scraper target ID has invalid format",
		}
	}
	url, err := NewURL(s.Addr, targetIDPath(update.ID))
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
	hc := NewClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, err
	}
	var targetResp targetResponse
	if err := json.NewDecoder(resp.Body).Decode(&targetResp); err != nil {
		return nil, err
	}

	return &targetResp.ScraperTarget, nil
}

// AddTarget creates a new scraper target and sets target.ID with the new identifier.
func (s *ScraperService) AddTarget(ctx context.Context, target *influxdb.ScraperTarget, userID influxdb.ID) error {
	url, err := NewURL(s.Addr, prefixTargets)
	if err != nil {
		return err
	}

	if !target.OrgID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "provided organization ID has invalid format",
			Op:   s.OpPrefix + influxdb.OpAddTarget,
		}
	}
	if !target.BucketID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "provided bucket ID has invalid format",
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

	hc := NewClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp); err != nil {
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
	url, err := NewURL(s.Addr, targetIDPath(id))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", url.String(), nil)
	if err != nil {
		return err
	}
	SetToken(s.Token, req)

	hc := NewClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckErrorStatus(http.StatusNoContent, resp)
}

// GetTargetByID returns a single target by ID.
func (s *ScraperService) GetTargetByID(ctx context.Context, id influxdb.ID) (*influxdb.ScraperTarget, error) {
	url, err := NewURL(s.Addr, targetIDPath(id))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}
	SetToken(s.Token, req)

	hc := NewClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var targetResp targetResponse
	if err := json.NewDecoder(resp.Body).Decode(&targetResp); err != nil {
		return nil, err
	}

	return &targetResp.ScraperTarget, nil
}

func targetIDPath(id influxdb.ID) string {
	return path.Join(prefixTargets, id.String())
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
	Members      string `json:"members"`
	Owners       string `json:"owners"`
}

type targetResponse struct {
	influxdb.ScraperTarget
	Org    string      `json:"org,omitempty"`
	Bucket string      `json:"bucket,omitempty"`
	Links  targetLinks `json:"links"`
}

func (h *ScraperHandler) newListTargetsResponse(ctx context.Context, targets []influxdb.ScraperTarget) (getTargetsResponse, error) {
	res := getTargetsResponse{
		Links: getTargetsLinks{
			Self: prefixTargets,
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
			Self:    targetIDPath(target.ID),
			Members: fmt.Sprintf("/api/v2/scrapers/%s/members", target.ID),
			Owners:  fmt.Sprintf("/api/v2/scrapers/%s/owners", target.ID),
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
		res.Org = org.Name
		res.OrgID = org.ID
		res.Links.Organization = organizationIDPath(org.ID)
	} else {
		res.OrgID = influxdb.InvalidID()
	}

	return res, nil
}

func organizationIDPath(id influxdb.ID) string {
	return path.Join(prefixOrganizations, id.String())
}

func bucketIDPath(id influxdb.ID) string {
	return path.Join(prefixBuckets, id.String())
}
