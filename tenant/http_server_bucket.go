package tenant

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

// BucketHandler represents an HTTP API handler for users.
type BucketHandler struct {
	chi.Router
	api       *kithttp.API
	log       *zap.Logger
	bucketSvc influxdb.BucketService
}

const (
	prefixBuckets = "/api/v2/buckets"
)

// NewHTTPBucketHandler constructs a new http server.
func NewHTTPBucketHandler(log *zap.Logger, bucketSvc influxdb.BucketService, urmHandler, labelHandler http.Handler) *BucketHandler {
	svr := &BucketHandler{
		api:       kithttp.NewAPI(kithttp.WithLog(log)),
		log:       log,
		bucketSvc: bucketSvc,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)

	// RESTy routes for "articles" resource
	r.Route("/", func(r chi.Router) {
		r.Post("/", svr.handlePostBucket)
		r.Get("/", svr.handleGetBuckets)

		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", svr.handleGetBucket)
			r.Patch("/", svr.handlePatchBucket)
			r.Delete("/", svr.handleDeleteBucket)

			// mount embedded resources
			mountableRouter := r.With(kithttp.ValidResource(svr.api, svr.lookupOrgByBucketID))
			mountableRouter.Mount("/members", urmHandler)
			mountableRouter.Mount("/owners", urmHandler)
			mountableRouter.Mount("/labels", labelHandler)
		})
	})

	svr.Router = r
	return svr
}

func (h *BucketHandler) Prefix() string {
	return prefixBuckets
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
	Links map[string]string `json:"links"`
}

func NewBucketResponse(b *influxdb.Bucket) *bucketResponse {
	res := &bucketResponse{
		Links: map[string]string{
			"self":    fmt.Sprintf("/api/v2/buckets/%s", b.ID),
			"org":     fmt.Sprintf("/api/v2/orgs/%s", b.OrgID),
			"members": fmt.Sprintf("/api/v2/buckets/%s/members", b.ID),
			"owners":  fmt.Sprintf("/api/v2/buckets/%s/owners", b.ID),
			"labels":  fmt.Sprintf("/api/v2/buckets/%s/labels", b.ID),
			"write":   fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", b.OrgID, b.ID),
		},
		bucket: *newBucket(b),
	}

	return res
}

type bucketsResponse struct {
	Links   *influxdb.PagingLinks `json:"links"`
	Buckets []*bucketResponse     `json:"buckets"`
}

func newBucketsResponse(ctx context.Context, opts influxdb.FindOptions, f influxdb.BucketFilter, bs []*influxdb.Bucket) *bucketsResponse {
	rs := make([]*bucketResponse, 0, len(bs))
	for _, b := range bs {
		rs = append(rs, NewBucketResponse(b))
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
	if err := b.OK(); err != nil {
		h.api.Err(w, r, err)
		return
	}

	bucket := b.toInfluxDB()

	if err := validBucketName(bucket); err != nil {
		h.api.Err(w, r, err)
		return
	}

	if err := h.bucketSvc.CreateBucket(r.Context(), bucket); err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Bucket created", zap.String("bucket", fmt.Sprint(bucket)))

	h.api.Respond(w, r, http.StatusCreated, NewBucketResponse(bucket))
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

	id, err := influxdb.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	b, err := h.bucketSvc.FindBucketByID(ctx, *id)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.log.Debug("Bucket retrieved", zap.String("bucket", fmt.Sprint(b)))

	h.api.Respond(w, r, http.StatusOK, NewBucketResponse(b))
}

// handleDeleteBucket is the HTTP handler for the DELETE /api/v2/buckets/:id route.
func (h *BucketHandler) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	id, err := influxdb.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	if err := h.bucketSvc.DeleteBucket(r.Context(), *id); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.log.Debug("Bucket deleted", zap.String("bucketID", id.String()))

	h.api.Respond(w, r, http.StatusNoContent, nil)
}

// handleGetBuckets is the HTTP handler for the GET /api/v2/buckets route.
func (h *BucketHandler) handleGetBuckets(w http.ResponseWriter, r *http.Request) {
	bucketsRequest, err := decodeGetBucketsRequest(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	bs, _, err := h.bucketSvc.FindBuckets(r.Context(), bucketsRequest.filter, bucketsRequest.opts)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Buckets retrieved", zap.String("buckets", fmt.Sprint(bs)))

	h.api.Respond(w, r, http.StatusOK, newBucketsResponse(r.Context(), bucketsRequest.opts, bucketsRequest.filter, bs))
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
	id, err := influxdb.IDFromString(chi.URLParam(r, "id"))
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
		b, err := h.bucketSvc.FindBucketByID(r.Context(), *id)
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

	b, err := h.bucketSvc.UpdateBucket(r.Context(), *id, *reqBody.toInfluxDB())
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.log.Debug("Bucket updated", zap.String("bucket", fmt.Sprint(b)))

	h.api.Respond(w, r, http.StatusOK, NewBucketResponse(b))
}

func (h *BucketHandler) lookupOrgByBucketID(ctx context.Context, id influxdb.ID) (influxdb.ID, error) {
	b, err := h.bucketSvc.FindBucketByID(ctx, id)
	if err != nil {
		return 0, err
	}
	return b.OrgID, nil
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
