package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"go.uber.org/zap"
)

// BucketBackend is all services and associated parameters required to construct
// the BucketHandler.
type BucketBackend struct {
	log *zap.Logger
	influxdb.HTTPErrorHandler

	BucketService              influxdb.BucketService
	BucketOperationLogService  influxdb.BucketOperationLogService
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
	UserService                influxdb.UserService
	OrganizationService        influxdb.OrganizationService
}

// NewBucketBackend returns a new instance of BucketBackend.
func NewBucketBackend(log *zap.Logger, b *APIBackend) *BucketBackend {
	return &BucketBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		BucketService:              b.BucketService,
		BucketOperationLogService:  b.BucketOperationLogService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
		OrganizationService:        b.OrganizationService,
	}
}

// BucketHandler represents an HTTP API handler for buckets.
type BucketHandler struct {
	*httprouter.Router
	influxdb.HTTPErrorHandler
	log *zap.Logger

	BucketService              influxdb.BucketService
	BucketOperationLogService  influxdb.BucketOperationLogService
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
	UserService                influxdb.UserService
	OrganizationService        influxdb.OrganizationService
}

const (
	prefixBuckets          = "/api/v2/buckets"
	bucketsIDPath          = "/api/v2/buckets/:id"
	bucketsIDLogPath       = "/api/v2/buckets/:id/logs"
	bucketsIDMembersPath   = "/api/v2/buckets/:id/members"
	bucketsIDMembersIDPath = "/api/v2/buckets/:id/members/:userID"
	bucketsIDOwnersPath    = "/api/v2/buckets/:id/owners"
	bucketsIDOwnersIDPath  = "/api/v2/buckets/:id/owners/:userID"
	bucketsIDLabelsPath    = "/api/v2/buckets/:id/labels"
	bucketsIDLabelsIDPath  = "/api/v2/buckets/:id/labels/:lid"
)

// NewBucketHandler returns a new instance of BucketHandler.
func NewBucketHandler(log *zap.Logger, b *BucketBackend) *BucketHandler {
	h := &BucketHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		BucketService:              b.BucketService,
		BucketOperationLogService:  b.BucketOperationLogService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
		OrganizationService:        b.OrganizationService,
	}

	h.HandlerFunc("POST", prefixBuckets, h.handlePostBucket)
	h.HandlerFunc("GET", prefixBuckets, h.handleGetBuckets)
	h.HandlerFunc("GET", bucketsIDPath, h.handleGetBucket)
	h.HandlerFunc("GET", bucketsIDLogPath, h.handleGetBucketLog)
	h.HandlerFunc("PATCH", bucketsIDPath, h.handlePatchBucket)
	h.HandlerFunc("DELETE", bucketsIDPath, h.handleDeleteBucket)

	memberBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               influxdb.BucketsResourceType,
		UserType:                   influxdb.Member,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", bucketsIDMembersPath, newPostMemberHandler(memberBackend))
	h.HandlerFunc("GET", bucketsIDMembersPath, newGetMembersHandler(memberBackend))
	h.HandlerFunc("DELETE", bucketsIDMembersIDPath, newDeleteMemberHandler(memberBackend))

	ownerBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               influxdb.BucketsResourceType,
		UserType:                   influxdb.Owner,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", bucketsIDOwnersPath, newPostMemberHandler(ownerBackend))
	h.HandlerFunc("GET", bucketsIDOwnersPath, newGetMembersHandler(ownerBackend))
	h.HandlerFunc("DELETE", bucketsIDOwnersIDPath, newDeleteMemberHandler(ownerBackend))

	labelBackend := &LabelBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              b.log.With(zap.String("handler", "label")),
		LabelService:     b.LabelService,
		ResourceType:     influxdb.BucketsResourceType,
	}
	h.HandlerFunc("GET", bucketsIDLabelsPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", bucketsIDLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", bucketsIDLabelsIDPath, newDeleteLabelHandler(labelBackend))

	return h
}

// bucket is used for serialization/deserialization with duration string syntax.
type bucket struct {
	ID                  influxdb.ID     `json:"id,omitempty"`
	OrgID               influxdb.ID     `json:"orgID,omitempty"`
	Type                string          `json:"type"`
	Description         string          `json:"description,omitempty"`
	Name                string          `json:"name"`
	RetentionPolicyName string          `json:"rp,omitempty"` // This to support v1 sources
	RetentionRules      []retentionRule `json:"retentionRules"`
	influxdb.CRUDLog
}

// retentionRule is the retention rule action for a bucket.
type retentionRule struct {
	Type         string `json:"type"`
	EverySeconds int64  `json:"everySeconds"`
}

func (rr *retentionRule) RetentionPeriod() (time.Duration, error) {
	t := time.Duration(rr.EverySeconds) * time.Second
	if t < time.Second {
		return t, &influxdb.Error{
			Code: influxdb.EUnprocessableEntity,
			Msg:  "expiration seconds must be greater than or equal to one second",
		}
	}

	return t, nil
}

func (b *bucket) toInfluxDB() (*influxdb.Bucket, error) {
	if b == nil {
		return nil, nil
	}

	var d time.Duration // zero value implies infinite retention policy

	// Only support a single retention period for the moment
	if len(b.RetentionRules) > 0 {
		d = time.Duration(b.RetentionRules[0].EverySeconds) * time.Second
		if d < time.Second {
			return nil, &influxdb.Error{
				Code: influxdb.EUnprocessableEntity,
				Msg:  "expiration seconds must be greater than or equal to one second",
			}
		}
	}

	return &influxdb.Bucket{
		ID:                  b.ID,
		OrgID:               b.OrgID,
		Type:                influxdb.ParseBucketType(b.Type),
		Description:         b.Description,
		Name:                b.Name,
		RetentionPolicyName: b.RetentionPolicyName,
		RetentionPeriod:     d,
		CRUDLog:             b.CRUDLog,
	}, nil
}

func newBucket(pb *influxdb.Bucket) *bucket {
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
		OrgID:               pb.OrgID,
		Type:                pb.Type.String(),
		Name:                pb.Name,
		Description:         pb.Description,
		RetentionPolicyName: pb.RetentionPolicyName,
		RetentionRules:      rules,
		CRUDLog:             pb.CRUDLog,
	}
}

// bucketUpdate is used for serialization/deserialization with retention rules.
type bucketUpdate struct {
	Name           *string         `json:"name,omitempty"`
	Description    *string         `json:"description,omitempty"`
	RetentionRules []retentionRule `json:"retentionRules,omitempty"`
}

func (b *bucketUpdate) toInfluxDB() (*influxdb.BucketUpdate, error) {
	if b == nil {
		return nil, nil
	}

	// For now, only use a single retention rule.
	var d time.Duration
	var err error

	if len(b.RetentionRules) > 0 {
		d, err = b.RetentionRules[0].RetentionPeriod()
		if err != nil {
			return nil, err
		}
	}

	return &influxdb.BucketUpdate{
		Name:            b.Name,
		Description:     b.Description,
		RetentionPeriod: &d,
	}, nil
}

func newBucketUpdate(pb *influxdb.BucketUpdate) *bucketUpdate {
	if pb == nil {
		return nil
	}

	up := &bucketUpdate{
		Name:           pb.Name,
		Description:    pb.Description,
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
	bucket
	Links  map[string]string `json:"links"`
	Labels []influxdb.Label  `json:"labels"`
}

func newBucketResponse(b *influxdb.Bucket, labels []*influxdb.Label) *bucketResponse {
	res := &bucketResponse{
		Links: map[string]string{
			"labels":  fmt.Sprintf("/api/v2/buckets/%s/labels", b.ID),
			"logs":    fmt.Sprintf("/api/v2/buckets/%s/logs", b.ID),
			"members": fmt.Sprintf("/api/v2/buckets/%s/members", b.ID),
			"org":     fmt.Sprintf("/api/v2/orgs/%s", b.OrgID),
			"owners":  fmt.Sprintf("/api/v2/buckets/%s/owners", b.ID),
			"self":    fmt.Sprintf("/api/v2/buckets/%s", b.ID),
			"write":   fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", b.OrgID, b.ID),
		},
		bucket: *newBucket(b),
		Labels: []influxdb.Label{},
	}

	for _, l := range labels {
		res.Labels = append(res.Labels, *l)
	}

	return res
}

type bucketsResponse struct {
	Links   *influxdb.PagingLinks `json:"links"`
	Buckets []*bucketResponse     `json:"buckets"`
}

func newBucketsResponse(ctx context.Context, opts influxdb.FindOptions, f influxdb.BucketFilter, bs []*influxdb.Bucket, labelService influxdb.LabelService) *bucketsResponse {
	rs := make([]*bucketResponse, 0, len(bs))
	for _, b := range bs {
		labels, _ := labelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: b.ID})
		rs = append(rs, newBucketResponse(b, labels))
	}
	return &bucketsResponse{
		Links:   newPagingLinks(prefixBuckets, opts, f, len(bs)),
		Buckets: rs,
	}
}

// handlePostBucket is the HTTP handler for the POST /api/v2/buckets route.
func (h *BucketHandler) handlePostBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePostBucketRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	bucket, err := req.toInfluxDB()
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	// names starting with an underscore are reserved for system buckets
	if err := validBucketName(bucket); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.BucketService.CreateBucket(ctx, bucket); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Bucket created", zap.String("bucket", fmt.Sprint(bucket)))

	if err := encodeResponse(ctx, w, http.StatusCreated, newBucketResponse(bucket, []*influxdb.Label{})); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type postBucketRequest struct {
	OrgID               influxdb.ID     `json:"orgID,omitempty"`
	Name                string          `json:"name"`
	Description         string          `json:"description"`
	RetentionPolicyName string          `json:"rp,omitempty"` // This to support v1 sources
	RetentionRules      []retentionRule `json:"retentionRules"`
}

func (b postBucketRequest) Validate() error {
	if !b.OrgID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "bucket requires an organization",
		}

	}
	return nil
}

func (b postBucketRequest) toInfluxDB() (*influxdb.Bucket, error) {
	var dur time.Duration // zero value implies infinite retention policy
	var err error

	// Only support a single retention period for the moment
	if len(b.RetentionRules) > 0 {
		dur, err = b.RetentionRules[0].RetentionPeriod()
		if err != nil {
			return nil, err
		}
	}

	return &influxdb.Bucket{
		OrgID:               b.OrgID,
		Description:         b.Description,
		Name:                b.Name,
		Type:                influxdb.BucketTypeUser,
		RetentionPolicyName: b.RetentionPolicyName,
		RetentionPeriod:     dur,
	}, err
}

func decodePostBucketRequest(ctx context.Context, r *http.Request) (*postBucketRequest, error) {
	b := &postBucketRequest{}
	if err := json.NewDecoder(r.Body).Decode(b); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	return b, b.Validate()
}

// handleGetBucket is the HTTP handler for the GET /api/v2/buckets/:id route.
func (h *BucketHandler) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetBucketRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	b, err := h.BucketService.FindBucketByID(ctx, req.BucketID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: b.ID})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Bucket retrieved", zap.String("bucket", fmt.Sprint(b)))

	if err := encodeResponse(ctx, w, http.StatusOK, newBucketResponse(b, labels)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getBucketRequest struct {
	BucketID influxdb.ID
}

func bucketIDPath(id influxdb.ID) string {
	return path.Join(prefixBuckets, id.String())
}

// hanldeGetBucketLog retrieves a bucket log by the buckets ID.
func (h *BucketHandler) handleGetBucketLog(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetBucketLogRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	log, _, err := h.BucketOperationLogService.GetBucketOperationLog(ctx, req.BucketID, req.opts)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Bucket log retrived", zap.String("bucket", fmt.Sprint(log)))

	if err := encodeResponse(ctx, w, http.StatusOK, newBucketLogResponse(req.BucketID, log)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getBucketLogRequest struct {
	BucketID influxdb.ID
	opts     influxdb.FindOptions
}

func decodeGetBucketLogRequest(ctx context.Context, r *http.Request) (*getBucketLogRequest, error) {
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

	opts, err := decodeFindOptions(ctx, r)
	if err != nil {
		return nil, err
	}

	return &getBucketLogRequest{
		BucketID: i,
		opts:     *opts,
	}, nil
}

func newBucketLogResponse(id influxdb.ID, es []*influxdb.OperationLogEntry) *operationLogResponse {
	logs := make([]*operationLogEntryResponse, 0, len(es))
	for _, e := range es {
		logs = append(logs, newOperationLogEntryResponse(e))
	}
	return &operationLogResponse{
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/buckets/%s/logs", id),
		},
		Logs: logs,
	}
}

func decodeGetBucketRequest(ctx context.Context, r *http.Request) (*getBucketRequest, error) {
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
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.BucketService.DeleteBucket(ctx, req.BucketID); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Bucket deleted", zap.String("bucketID", req.BucketID.String()))

	w.WriteHeader(http.StatusNoContent)
}

type deleteBucketRequest struct {
	BucketID influxdb.ID
}

func decodeDeleteBucketRequest(ctx context.Context, r *http.Request) (*deleteBucketRequest, error) {
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
	req := &deleteBucketRequest{
		BucketID: i,
	}

	return req, nil
}

// handleGetBuckets is the HTTP handler for the GET /api/v2/buckets route.
func (h *BucketHandler) handleGetBuckets(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "BucketHandler")
	defer span.Finish()

	ctx := r.Context()
	req, err := decodeGetBucketsRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	bs, _, err := h.BucketService.FindBuckets(ctx, req.filter, req.opts)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Buckets retrieved", zap.String("buckets", fmt.Sprint(bs)))

	if err := encodeResponse(ctx, w, http.StatusOK, newBucketsResponse(ctx, req.opts, req.filter, bs, h.LabelService)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getBucketsRequest struct {
	filter influxdb.BucketFilter
	opts   influxdb.FindOptions
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
		id, err := influxdb.IDFromString(orgID)
		if err != nil {
			return nil, err
		}
		req.filter.OrganizationID = id
	}

	if org := qp.Get("org"); org != "" {
		req.filter.Org = &org
	}

	if name := qp.Get("name"); name != "" {
		req.filter.Name = &name
	}

	if bucketID := qp.Get("id"); bucketID != "" {
		id, err := influxdb.IDFromString(bucketID)
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
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if req.Update.Name != nil {
		b, err := h.BucketService.FindBucketByID(ctx, req.BucketID)
		if err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}
		b.Name = *req.Update.Name
		if err := validBucketName(b); err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}
	}

	b, err := h.BucketService.UpdateBucket(ctx, req.BucketID, req.Update)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: b.ID})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Bucket updated", zap.String("bucket", fmt.Sprint(b)))

	if err := encodeResponse(ctx, w, http.StatusOK, newBucketResponse(b, labels)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type patchBucketRequest struct {
	Update   influxdb.BucketUpdate
	BucketID influxdb.ID
}

func decodePatchBucketRequest(ctx context.Context, r *http.Request) (*patchBucketRequest, error) {
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
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  err.Error(),
		}
	}

	bu := &bucketUpdate{}
	if err := json.NewDecoder(r.Body).Decode(bu); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  err.Error(),
		}
	}

	upd, err := bu.toInfluxDB()
	if err != nil {
		return nil, err
	}

	return &patchBucketRequest{
		Update:   *upd,
		BucketID: i,
	}, nil
}

// BucketService connects to Influx via HTTP using tokens to manage buckets
type BucketService struct {
	Client *httpc.Client
	// OpPrefix is an additional property for error
	// find bucket service, when finds nothing.
	OpPrefix string
}

// FindBucketByName returns a single bucket by name
func (s *BucketService) FindBucketByName(ctx context.Context, orgID influxdb.ID, name string) (*influxdb.Bucket, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if name == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EUnprocessableEntity,
			Op:   s.OpPrefix + influxdb.OpFindBuckets,
			Msg:  "bucket name is required",
		}
	}

	bkts, n, err := s.FindBuckets(ctx, influxdb.BucketFilter{
		Name:           &name,
		OrganizationID: &orgID,
	})
	if err != nil {
		return nil, err
	}
	if n == 0 || len(bkts) == 0 {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Op:   s.OpPrefix + influxdb.OpFindBucket,
			Msg:  fmt.Sprintf("bucket %q not found", name),
		}
	}

	return bkts[0], nil
}

// FindBucketByID returns a single bucket by ID.
func (s *BucketService) FindBucketByID(ctx context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
	// TODO(@jsteenb2): are tracing
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var br bucketResponse
	err := s.Client.
		Get(bucketIDPath(id)).
		DecodeJSON(&br).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return br.toInfluxDB()
}

// FindBucket returns the first bucket that matches filter.
func (s *BucketService) FindBucket(ctx context.Context, filter influxdb.BucketFilter) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bs, n, err := s.FindBuckets(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n == 0 && filter.Name != nil {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Op:   s.OpPrefix + influxdb.OpFindBucket,
			Msg:  fmt.Sprintf("bucket %q not found", *filter.Name),
		}
	} else if n == 0 {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Op:   s.OpPrefix + influxdb.OpFindBucket,
			Msg:  "bucket not found",
		}
	}

	return bs[0], nil
}

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
// Additional options provide pagination & sorting.
func (s *BucketService) FindBuckets(ctx context.Context, filter influxdb.BucketFilter, opt ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	params := findOptionParams(opt...)
	if filter.OrganizationID != nil {
		params = append(params, [2]string{"orgID", filter.OrganizationID.String()})
	}
	if filter.Org != nil {
		params = append(params, [2]string{"org", *filter.Org})
	}
	if filter.ID != nil {
		params = append(params, [2]string{"id", filter.ID.String()})
	}
	if filter.Name != nil {
		params = append(params, [2]string{"name", (*filter.Name)})
	}

	var bs bucketsResponse
	err := s.Client.
		Get(prefixBuckets).
		QueryParams(params...).
		DecodeJSON(&bs).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	buckets := make([]*influxdb.Bucket, 0, len(bs.Buckets))
	for _, b := range bs.Buckets {
		pb, err := b.bucket.toInfluxDB()
		if err != nil {
			return nil, 0, err
		}

		buckets = append(buckets, pb)
	}

	return buckets, len(buckets), nil
}

// CreateBucket creates a new bucket and sets b.ID with the new identifier.
func (s *BucketService) CreateBucket(ctx context.Context, b *influxdb.Bucket) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var br bucketResponse
	err := s.Client.
		PostJSON(newBucket(b), prefixBuckets).
		DecodeJSON(&br).
		Do(ctx)
	if err != nil {
		return err
	}

	pb, err := br.toInfluxDB()
	if err != nil {
		return err
	}
	*b = *pb
	return nil
}

// UpdateBucket updates a single bucket with changeset.
// Returns the new bucket state after update.
func (s *BucketService) UpdateBucket(ctx context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	var br bucketResponse
	err := s.Client.
		PatchJSON(newBucketUpdate(&upd), bucketIDPath(id)).
		DecodeJSON(&br).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return br.toInfluxDB()
}

// DeleteBucket removes a bucket by ID.
func (s *BucketService) DeleteBucket(ctx context.Context, id influxdb.ID) error {
	return s.Client.
		Delete(bucketIDPath(id)).
		Do(ctx)
}

// validBucketName reports any errors with bucket names
func validBucketName(bucket *influxdb.Bucket) error {
	// names starting with an underscore are reserved for system buckets
	if strings.HasPrefix(bucket.Name, "_") && bucket.Type != influxdb.BucketTypeSystem {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Op:   "http/bucket",
			Msg:  fmt.Sprintf("bucket name %s is invalid. Buckets may not start with underscore", bucket.Name),
		}
	}
	return nil
}
