package http

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
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
	api *kithttp.API
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
		Router: NewRouter(b.HTTPErrorHandler),
		api:    kithttp.NewAPI(kithttp.WithLog(log)),
		log:    log,

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

func (b *bucketUpdate) OK() error {
	if len(b.RetentionRules) > 0 {
		_, err := b.RetentionRules[0].RetentionPeriod()
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *bucketUpdate) toInfluxDB() *influxdb.BucketUpdate {
	if b == nil {
		return nil
	}

	// For now, only use a single retention rule.
	var d time.Duration
	if len(b.RetentionRules) > 0 {
		d, _ = b.RetentionRules[0].RetentionPeriod()
	}

	return &influxdb.BucketUpdate{
		Name:            b.Name,
		Description:     b.Description,
		RetentionPeriod: &d,
	}
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

func NewBucketResponse(b *influxdb.Bucket, labels []*influxdb.Label) *bucketResponse {
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
		labels, _ := labelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: b.ID, ResourceType: influxdb.BucketsResourceType})
		rs = append(rs, NewBucketResponse(b, labels))
	}
	return &bucketsResponse{
		Links:   influxdb.NewPagingLinks(prefixBuckets, opts, f, len(bs)),
		Buckets: rs,
	}
}

// handlePostBucket is the HTTP handler for the POST /api/v2/buckets route.
func (h *BucketHandler) handlePostBucket(w http.ResponseWriter, r *http.Request) {
	var b postBucketRequest
	if err := h.api.DecodeJSON(r.Body, &b); err != nil {
		h.api.Err(w, r, err)
		return
	}

	bucket := b.toInfluxDB()
	if err := h.BucketService.CreateBucket(r.Context(), bucket); err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Bucket created", zap.String("bucket", fmt.Sprint(bucket)))

	h.api.Respond(w, r, http.StatusCreated, NewBucketResponse(bucket, []*influxdb.Label{}))
}

type postBucketRequest struct {
	OrgID               influxdb.ID     `json:"orgID,omitempty"`
	Name                string          `json:"name"`
	Description         string          `json:"description"`
	RetentionPolicyName string          `json:"rp,omitempty"` // This to support v1 sources
	RetentionRules      []retentionRule `json:"retentionRules"`
}

func (b *postBucketRequest) OK() error {
	if !b.OrgID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "organization id must be provided",
		}
	}

	// Only support a single retention period for the moment
	if len(b.RetentionRules) > 0 {
		if _, err := b.RetentionRules[0].RetentionPeriod(); err != nil {
			return &influxdb.Error{
				Code: influxdb.EUnprocessableEntity,
				Msg:  err.Error(),
			}
		}
	}

	// names starting with an underscore are reserved for system buckets
	if err := validBucketName(b.toInfluxDB()); err != nil {
		return &influxdb.Error{
			Code: influxdb.EUnprocessableEntity,
			Msg:  err.Error(),
		}
	}

	return nil
}

func (b postBucketRequest) toInfluxDB() *influxdb.Bucket {
	// Only support a single retention period for the moment
	var dur time.Duration
	if len(b.RetentionRules) > 0 {
		dur, _ = b.RetentionRules[0].RetentionPeriod()
	}

	return &influxdb.Bucket{
		OrgID:               b.OrgID,
		Description:         b.Description,
		Name:                b.Name,
		Type:                influxdb.BucketTypeUser,
		RetentionPolicyName: b.RetentionPolicyName,
		RetentionPeriod:     dur,
	}
}

// handleGetBucket is the HTTP handler for the GET /api/v2/buckets/:id route.
func (h *BucketHandler) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id, err := decodeIDFromCtx(r.Context(), "id")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	b, err := h.BucketService.FindBucketByID(ctx, id)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: b.ID, ResourceType: influxdb.BucketsResourceType})
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.log.Debug("Bucket retrieved", zap.String("bucket", fmt.Sprint(b)))

	h.api.Respond(w, r, http.StatusOK, NewBucketResponse(b, labels))
}

func bucketIDPath(id influxdb.ID) string {
	return path.Join(prefixBuckets, id.String())
}

// handleGetBucketLog retrieves a bucket log by the buckets ID.
func (h *BucketHandler) handleGetBucketLog(w http.ResponseWriter, r *http.Request) {
	id, err := decodeIDFromCtx(r.Context(), "id")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	opts, err := influxdb.DecodeFindOptions(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	log, _, err := h.BucketOperationLogService.GetBucketOperationLog(r.Context(), id, *opts)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.log.Debug("Bucket log retrived", zap.String("bucket", fmt.Sprint(log)))

	h.api.Respond(w, r, http.StatusOK, newBucketLogResponse(id, log))
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

// handleDeleteBucket is the HTTP handler for the DELETE /api/v2/buckets/:id route.
func (h *BucketHandler) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	id, err := decodeIDFromCtx(r.Context(), "id")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	if err := h.BucketService.DeleteBucket(r.Context(), id); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.log.Debug("Bucket deleted", zap.String("bucketID", id.String()))

	h.api.Respond(w, r, http.StatusNoContent, nil)
}

// handleGetBuckets is the HTTP handler for the GET /api/v2/buckets route.
func (h *BucketHandler) handleGetBuckets(w http.ResponseWriter, r *http.Request) {
	var filter influxdb.BucketFilter
	q := r.URL.Query()
	if org := q.Get("org"); org != "" {
		filter.Org = &org
	}
	if name := q.Get("name"); name != "" {
		filter.Name = &name
	}

	bucketID, err := decodeIDFromQuery(q, "id")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	if bucketID > 0 {
		filter.ID = &bucketID
	}

	orgID, err := decodeIDFromQuery(q, "orgID")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	if orgID > 0 {
		filter.OrganizationID = &orgID
	}

	opts, err := influxdb.DecodeFindOptions(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	bs, _, err := h.BucketService.FindBuckets(r.Context(), filter, *opts)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Buckets retrieved", zap.String("buckets", fmt.Sprint(bs)))

	h.api.Respond(w, r, http.StatusOK, newBucketsResponse(r.Context(), *opts, filter, bs, h.LabelService))
}

type getBucketsRequest struct {
	filter influxdb.BucketFilter
	opts   influxdb.FindOptions
}

func decodeGetBucketsRequest(r *http.Request) (*getBucketsRequest, error) {
	qp := r.URL.Query()
	req := &getBucketsRequest{}

	opts, err := influxdb.DecodeFindOptions(r)
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
	id, err := decodeIDFromCtx(r.Context(), "id")
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	var reqBody bucketUpdate
	if err := h.api.DecodeJSON(r.Body, &reqBody); err != nil {
		h.api.Err(w, r, err)
		return
	}

	if reqBody.Name != nil {
		b, err := h.BucketService.FindBucketByID(r.Context(), id)
		if err != nil {
			h.api.Err(w, r, err)
			return
		}
		b.Name = *reqBody.Name
		if err := validBucketName(b); err != nil {
			h.api.Err(w, r, err)
			return
		}
	}

	b, err := h.BucketService.UpdateBucket(r.Context(), id, *reqBody.toInfluxDB())
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	// TODO: should move to service to encapsulate labels and what any other dependencies. Future
	// 	work for service definition
	labels, err := h.LabelService.FindResourceLabels(r.Context(), influxdb.LabelMappingFilter{
		ResourceID:   b.ID,
		ResourceType: influxdb.BucketsResourceType,
	})
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Bucket updated", zap.String("bucket", fmt.Sprint(b)))

	h.api.Respond(w, r, http.StatusOK, NewBucketResponse(b, labels))
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

	params := influxdb.FindOptionParams(opt...)
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
