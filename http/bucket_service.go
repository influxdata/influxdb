package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/errors"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// BucketHandler represents an HTTP API handler for buckets.
type BucketHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	BucketService              platform.BucketService
	BucketOperationLogService  platform.BucketOperationLogService
	UserResourceMappingService platform.UserResourceMappingService
	LabelService               platform.LabelService
	UserService                platform.UserService
	OrganizationService        platform.OrganizationService
}

const (
	bucketsPath            = "/api/v2/buckets"
	bucketsIDPath          = "/api/v2/buckets/:id"
	bucketsIDLogPath       = "/api/v2/buckets/:id/log"
	bucketsIDMembersPath   = "/api/v2/buckets/:id/members"
	bucketsIDMembersIDPath = "/api/v2/buckets/:id/members/:userID"
	bucketsIDOwnersPath    = "/api/v2/buckets/:id/owners"
	bucketsIDOwnersIDPath  = "/api/v2/buckets/:id/owners/:userID"
	bucketsIDLabelsPath    = "/api/v2/buckets/:id/labels"
	bucketsIDLabelsIDPath  = "/api/v2/buckets/:id/labels/:lid"
)

// NewBucketHandler returns a new instance of BucketHandler.
func NewBucketHandler(mappingService platform.UserResourceMappingService, labelService platform.LabelService, userService platform.UserService) *BucketHandler {
	h := &BucketHandler{
		Router: NewRouter(),
		Logger: zap.NewNop(),

		UserResourceMappingService: mappingService,
		LabelService:               labelService,
		UserService:                userService,
	}

	h.HandlerFunc("POST", bucketsPath, h.handlePostBucket)
	h.HandlerFunc("GET", bucketsPath, h.handleGetBuckets)
	h.HandlerFunc("GET", bucketsIDPath, h.handleGetBucket)
	h.HandlerFunc("GET", bucketsIDLogPath, h.handleGetBucketLog)
	h.HandlerFunc("PATCH", bucketsIDPath, h.handlePatchBucket)
	h.HandlerFunc("DELETE", bucketsIDPath, h.handleDeleteBucket)

	h.HandlerFunc("POST", bucketsIDMembersPath, newPostMemberHandler(h.UserResourceMappingService, h.UserService, platform.BucketsResourceType, platform.Member))
	h.HandlerFunc("GET", bucketsIDMembersPath, newGetMembersHandler(h.UserResourceMappingService, h.UserService, platform.BucketsResourceType, platform.Member))
	h.HandlerFunc("DELETE", bucketsIDMembersIDPath, newDeleteMemberHandler(h.UserResourceMappingService, platform.Member))

	h.HandlerFunc("POST", bucketsIDOwnersPath, newPostMemberHandler(h.UserResourceMappingService, h.UserService, platform.BucketsResourceType, platform.Owner))
	h.HandlerFunc("GET", bucketsIDOwnersPath, newGetMembersHandler(h.UserResourceMappingService, h.UserService, platform.BucketsResourceType, platform.Owner))
	h.HandlerFunc("DELETE", bucketsIDOwnersIDPath, newDeleteMemberHandler(h.UserResourceMappingService, platform.Owner))

	h.HandlerFunc("GET", bucketsIDLabelsPath, newGetLabelsHandler(h.LabelService))
	h.HandlerFunc("POST", bucketsIDLabelsPath, newPostLabelHandler(h.LabelService))
	h.HandlerFunc("DELETE", bucketsIDLabelsIDPath, newDeleteLabelHandler(h.LabelService))

	return h
}

// bucket is used for serialization/deserialization with duration string syntax.
type bucket struct {
	ID                  platform.ID     `json:"id,omitempty"`
	OrganizationID      platform.ID     `json:"organizationID,omitempty"`
	Organization        string          `json:"organization,omitempty"`
	Name                string          `json:"name"`
	RetentionPolicyName string          `json:"rp,omitempty"` // This to support v1 sources
	RetentionRules      []retentionRule `json:"retentionRules"`
}

// retentionRule is the retention rule action for a bucket.
type retentionRule struct {
	Type         string `json:"type"`
	EverySeconds int64  `json:"everySeconds"`
}

func (b *bucket) toPlatform() (*platform.Bucket, error) {
	if b == nil {
		return nil, nil
	}

	var d time.Duration // zero value implies infinite retention policy

	// Only support a single retention period for the moment
	if len(b.RetentionRules) > 0 {
		d = time.Duration(b.RetentionRules[0].EverySeconds) * time.Second
		if d < time.Second {
			return nil, errors.InvalidDataf("expiration seconds must be greater than or equal to one second")
		}
	}

	return &platform.Bucket{
		ID:                  b.ID,
		OrganizationID:      b.OrganizationID,
		Organization:        b.Organization,
		Name:                b.Name,
		RetentionPolicyName: b.RetentionPolicyName,
		RetentionPeriod:     d,
	}, nil
}

func newBucket(pb *platform.Bucket) *bucket {
	if pb == nil {
		return nil
	}

	rules := []retentionRule{}
	rp := int64(pb.RetentionPeriod.Round(time.Second) / time.Second)
	if rp > 0 {
		rules = append(rules, retentionRule{
			Type:         "expire",
			EverySeconds: rp,
		})
	}

	return &bucket{
		ID:                  pb.ID,
		OrganizationID:      pb.OrganizationID,
		Organization:        pb.Organization,
		Name:                pb.Name,
		RetentionPolicyName: pb.RetentionPolicyName,
		RetentionRules:      rules,
	}
}

// bucketUpdate is used for serialization/deserialization with retention rules.
type bucketUpdate struct {
	Name           *string         `json:"name,omitempty"`
	RetentionRules []retentionRule `json:"retentionRules,omitempty"`
}

func (b *bucketUpdate) toPlatform() (*platform.BucketUpdate, error) {
	if b == nil {
		return nil, nil
	}

	// For now, only use a single retention rule.
	var d time.Duration
	if len(b.RetentionRules) > 0 {
		d = time.Duration(b.RetentionRules[0].EverySeconds) * time.Second
		if d < time.Second {
			return nil, errors.InvalidDataf("expiration seconds must be greater than or equal to one second")
		}
	}

	return &platform.BucketUpdate{
		Name:            b.Name,
		RetentionPeriod: &d,
	}, nil
}

func newBucketUpdate(pb *platform.BucketUpdate) *bucketUpdate {
	if pb == nil {
		return nil
	}

	up := &bucketUpdate{
		Name:           pb.Name,
		RetentionRules: []retentionRule{},
	}

	if pb.RetentionPeriod != nil {
		d := int64((*pb.RetentionPeriod).Round(time.Second) / time.Second)
		up.RetentionRules = append(up.RetentionRules, retentionRule{
			Type:         "expire",
			EverySeconds: d,
		})
	}
	return up
}

type bucketResponse struct {
	Links map[string]string `json:"links"`
	bucket
	Labels []platform.Label `json:"labels"`
}

func newBucketResponse(b *platform.Bucket, labels []*platform.Label) *bucketResponse {
	res := &bucketResponse{
		Links: map[string]string{
			"self":   fmt.Sprintf("/api/v2/buckets/%s", b.ID),
			"log":    fmt.Sprintf("/api/v2/buckets/%s/log", b.ID),
			"labels": fmt.Sprintf("/api/v2/buckets/%s/labels", b.ID),
			"org":    fmt.Sprintf("/api/v2/orgs/%s", b.OrganizationID),
		},
		bucket: *newBucket(b),
		Labels: []platform.Label{},
	}

	for _, l := range labels {
		res.Labels = append(res.Labels, *l)
	}

	return res
}

type bucketsResponse struct {
	Links   *platform.PagingLinks `json:"links"`
	Buckets []*bucketResponse     `json:"buckets"`
}

func newBucketsResponse(ctx context.Context, opts platform.FindOptions, f platform.BucketFilter, bs []*platform.Bucket, labelService platform.LabelService) *bucketsResponse {
	rs := make([]*bucketResponse, 0, len(bs))
	for _, b := range bs {
		labels, _ := labelService.FindResourceLabels(ctx, platform.LabelMappingFilter{ResourceID: b.ID})
		rs = append(rs, newBucketResponse(b, labels))
	}
	return &bucketsResponse{
		Links:   newPagingLinks(bucketsPath, opts, f, len(bs)),
		Buckets: rs,
	}
}

// handlePostBucket is the HTTP handler for the POST /api/v2/buckets route.
func (h *BucketHandler) handlePostBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostBucketRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if !req.Bucket.OrganizationID.Valid() {
		// Resolve organization name to ID before create
		o, err := h.OrganizationService.FindOrganization(ctx, platform.OrganizationFilter{Name: &req.Bucket.Organization})
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}
		req.Bucket.OrganizationID = o.ID
	}

	if err := h.BucketService.CreateBucket(ctx, req.Bucket); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newBucketResponse(req.Bucket, []*platform.Label{})); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type postBucketRequest struct {
	Bucket *platform.Bucket
}

func (b postBucketRequest) Validate() error {
	if b.Bucket.Organization == "" && !b.Bucket.OrganizationID.Valid() {
		return fmt.Errorf("bucket requires an organization")
	}
	return nil
}

func decodePostBucketRequest(ctx context.Context, r *http.Request) (*postBucketRequest, error) {
	b := &bucket{}
	if err := json.NewDecoder(r.Body).Decode(b); err != nil {
		return nil, err
	}

	pb, err := b.toPlatform()
	if err != nil {
		return nil, err
	}

	req := &postBucketRequest{
		Bucket: pb,
	}

	return req, req.Validate()
}

// handleGetBucket is the HTTP handler for the GET /api/v2/buckets/:id route.
func (h *BucketHandler) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetBucketRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	b, err := h.BucketService.FindBucketByID(ctx, req.BucketID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, platform.LabelMappingFilter{ResourceID: b.ID})
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newBucketResponse(b, labels)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type getBucketRequest struct {
	BucketID platform.ID
}

func decodeGetBucketRequest(ctx context.Context, r *http.Request) (*getBucketRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req := &getBucketRequest{
		BucketID: i,
	}

	return req, nil
}

// handleDeleteBucket is the HTTP handler for the DELETE /api/v2/buckets/:id route.
func (h *BucketHandler) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteBucketRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.BucketService.DeleteBucket(ctx, req.BucketID); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type deleteBucketRequest struct {
	BucketID platform.ID
}

func decodeDeleteBucketRequest(ctx context.Context, r *http.Request) (*deleteBucketRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req := &deleteBucketRequest{
		BucketID: i,
	}

	return req, nil
}

// handleGetBuckets is the HTTP handler for the GET /api/v2/buckets route.
func (h *BucketHandler) handleGetBuckets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetBucketsRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	bs, _, err := h.BucketService.FindBuckets(ctx, req.filter, req.opts)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newBucketsResponse(ctx, req.opts, req.filter, bs, h.LabelService)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type getBucketsRequest struct {
	filter platform.BucketFilter
	opts   platform.FindOptions
}

func decodeGetBucketsRequest(ctx context.Context, r *http.Request) (*getBucketsRequest, error) {
	qp := r.URL.Query()
	req := &getBucketsRequest{}

	opts, err := decodeFindOptions(ctx, r)
	if err != nil {
		return nil, err
	}

	req.opts = *opts

	if orgID := qp.Get("orgID"); orgID != "" {
		id, err := platform.IDFromString(orgID)
		if err != nil {
			return nil, err
		}
		req.filter.OrganizationID = id
	}

	if org := qp.Get("org"); org != "" {
		req.filter.Organization = &org
	}

	if name := qp.Get("name"); name != "" {
		req.filter.Name = &name
	}

	if bucketID := qp.Get("id"); bucketID != "" {
		id, err := platform.IDFromString(bucketID)
		if err != nil {
			return nil, err
		}
		req.filter.ID = id
	}

	return req, nil
}

// handlePatchBucket is the HTTP handler for the PATCH /api/v2/buckets route.
func (h *BucketHandler) handlePatchBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchBucketRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	b, err := h.BucketService.UpdateBucket(ctx, req.BucketID, req.Update)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, platform.LabelMappingFilter{ResourceID: b.ID})
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newBucketResponse(b, labels)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type patchBucketRequest struct {
	Update   platform.BucketUpdate
	BucketID platform.ID
}

func decodePatchBucketRequest(ctx context.Context, r *http.Request) (*patchBucketRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	bu := &bucketUpdate{}
	if err := json.NewDecoder(r.Body).Decode(bu); err != nil {
		return nil, err
	}

	upd, err := bu.toPlatform()
	if err != nil {
		return nil, err
	}

	return &patchBucketRequest{
		Update:   *upd,
		BucketID: i,
	}, nil
}

const (
	bucketPath = "/api/v2/buckets"
)

// BucketService connects to Influx via HTTP using tokens to manage buckets
type BucketService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
	// OpPrefix is an additional property for error
	// find bucket service, when finds nothing.
	OpPrefix string
}

// FindBucketByID returns a single bucket by ID.
func (s *BucketService) FindBucketByID(ctx context.Context, id platform.ID) (*platform.Bucket, error) {
	u, err := newURL(s.Addr, bucketIDPath(id))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp, true); err != nil {
		return nil, err
	}

	var br bucketResponse
	if err := json.NewDecoder(resp.Body).Decode(&br); err != nil {
		return nil, err
	}
	return br.toPlatform()
}

// FindBucket returns the first bucket that matches filter.
func (s *BucketService) FindBucket(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
	bs, n, err := s.FindBuckets(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Op:   s.OpPrefix + platform.OpFindBucket,
			Msg:  "bucket not found",
			Err:  ErrNotFound,
		}
	}

	return bs[0], nil
}

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
// Additional options provide pagination & sorting.
func (s *BucketService) FindBuckets(ctx context.Context, filter platform.BucketFilter, opt ...platform.FindOptions) ([]*platform.Bucket, int, error) {
	u, err := newURL(s.Addr, bucketPath)
	if err != nil {
		return nil, 0, err
	}

	query := u.Query()
	if filter.OrganizationID != nil {
		query.Add("orgID", filter.OrganizationID.String())
	}
	if filter.Organization != nil {
		query.Add("org", *filter.Organization)
	}
	if filter.ID != nil {
		query.Add("id", filter.ID.String())
	}
	if filter.Name != nil {
		query.Add("name", *filter.Name)
	}

	if len(opt) > 0 {
		for k, vs := range opt[0].QueryParams() {
			for _, v := range vs {
				query.Add(k, v)
			}
		}
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	req.URL.RawQuery = query.Encode()
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp, true); err != nil {
		return nil, 0, err
	}

	var bs bucketsResponse
	if err := json.NewDecoder(resp.Body).Decode(&bs); err != nil {
		return nil, 0, err
	}

	buckets := make([]*platform.Bucket, 0, len(bs.Buckets))
	for _, b := range bs.Buckets {
		pb, err := b.bucket.toPlatform()
		if err != nil {
			return nil, 0, err
		}

		buckets = append(buckets, pb)
	}

	return buckets, len(buckets), nil
}

// CreateBucket creates a new bucket and sets b.ID with the new identifier.
func (s *BucketService) CreateBucket(ctx context.Context, b *platform.Bucket) error {
	u, err := newURL(s.Addr, bucketPath)
	if err != nil {
		return err
	}

	octets, err := json.Marshal(newBucket(b))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp, true); err != nil {
		return err
	}

	var br bucketResponse
	if err := json.NewDecoder(resp.Body).Decode(&br); err != nil {
		return err
	}

	pb, err := br.toPlatform()
	if err != nil {
		return err
	}
	*b = *pb
	return nil
}

// UpdateBucket updates a single bucket with changeset.
// Returns the new bucket state after update.
func (s *BucketService) UpdateBucket(ctx context.Context, id platform.ID, upd platform.BucketUpdate) (*platform.Bucket, error) {
	u, err := newURL(s.Addr, bucketIDPath(id))
	if err != nil {
		return nil, err
	}

	bu := newBucketUpdate(&upd)
	octets, err := json.Marshal(bu)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PATCH", u.String(), bytes.NewReader(octets))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp, true); err != nil {
		return nil, err
	}

	var br bucketResponse
	if err := json.NewDecoder(resp.Body).Decode(&br); err != nil {
		return nil, err
	}
	return br.toPlatform()
}

// DeleteBucket removes a bucket by ID.
func (s *BucketService) DeleteBucket(ctx context.Context, id platform.ID) error {
	u, err := newURL(s.Addr, bucketIDPath(id))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckError(resp, true)
}

func bucketIDPath(id platform.ID) string {
	return path.Join(bucketPath, id.String())
}

// hanldeGetBucketLog retrieves a bucket log by the buckets ID.
func (h *BucketHandler) handleGetBucketLog(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetBucketLogRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	log, _, err := h.BucketOperationLogService.GetBucketOperationLog(ctx, req.BucketID, req.opts)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newBucketLogResponse(req.BucketID, log)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type getBucketLogRequest struct {
	BucketID platform.ID
	opts     platform.FindOptions
}

func decodeGetBucketLogRequest(ctx context.Context, r *http.Request) (*getBucketLogRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	opts := platform.DefaultOperationLogFindOptions
	qp := r.URL.Query()
	if v := qp.Get("desc"); v == "false" {
		opts.Descending = false
	}
	if v := qp.Get("limit"); v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}
		opts.Limit = i
	}
	if v := qp.Get("offset"); v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}
		opts.Offset = i
	}

	return &getBucketLogRequest{
		BucketID: i,
		opts:     opts,
	}, nil
}

func newBucketLogResponse(id platform.ID, es []*platform.OperationLogEntry) *operationLogResponse {
	log := make([]*operationLogEntryResponse, 0, len(es))
	for _, e := range es {
		log = append(log, newOperationLogEntryResponse(e))
	}
	return &operationLogResponse{
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/buckets/%s/log", id),
		},
		Log: log,
	}
}
