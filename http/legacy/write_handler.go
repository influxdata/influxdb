package legacy

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http/metric"
	"github.com/influxdata/influxdb/v2/http/points"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/storage"
	"go.uber.org/zap"
)

var _ http.Handler = (*WriteHandler)(nil)

const (
	opWriteHandler = "http/v1WriteHandler"
)

// PointsWriterBackend contains all the services needed to run a PointsWriterHandler.
type PointsWriterBackend struct {
	influxdb.HTTPErrorHandler
	Logger *zap.Logger

	EventRecorder      metric.EventRecorder
	BucketService      influxdb.BucketService
	PointsWriter       storage.PointsWriter
	DBRPMappingService influxdb.DBRPMappingServiceV2
}

// NewPointsWriterBackend creates a new backend for legacy work.
func NewPointsWriterBackend(b *Backend) *PointsWriterBackend {
	return &PointsWriterBackend{
		HTTPErrorHandler:   b.HTTPErrorHandler,
		Logger:             b.Logger.With(zap.String("handler", "points_writer")),
		EventRecorder:      b.WriteEventRecorder,
		BucketService:      b.BucketService,
		PointsWriter:       b.PointsWriter,
		DBRPMappingService: b.DBRPMappingServiceV2,
	}
}

// PointsWriterHandler represents an HTTP API handler for writing points.
type WriteHandler struct {
	influxdb.HTTPErrorHandler
	EventRecorder      metric.EventRecorder
	BucketService      influxdb.BucketService
	PointsWriter       storage.PointsWriter
	DBRPMappingService influxdb.DBRPMappingServiceV2

	router            *httprouter.Router
	logger            *zap.Logger
	maxBatchSizeBytes int64
}

// NewWriterHandler returns a new instance of PointsWriterHandler.
func NewWriterHandler(b *PointsWriterBackend, opts ...WriteHandlerOption) *WriteHandler {
	h := &WriteHandler{
		HTTPErrorHandler:   b.HTTPErrorHandler,
		EventRecorder:      b.EventRecorder,
		BucketService:      b.BucketService,
		PointsWriter:       b.PointsWriter,
		DBRPMappingService: b.DBRPMappingService,

		router: NewRouter(b.HTTPErrorHandler),
		logger: b.Logger.With(zap.String("handler", "points_writer")),
	}

	for _, opt := range opts {
		opt(h)
	}

	h.router.HandlerFunc(http.MethodPost, "/write", h.handleWrite)

	return h
}

// WriteHandlerOption is a functional option for a *PointsWriterHandler
type WriteHandlerOption func(*WriteHandler)

// WithMaxBatchSizeBytes configures the maximum size for a
// (decompressed) points batch allowed by the write handler
func WithMaxBatchSizeBytes(n int64) WriteHandlerOption {
	return func(w *WriteHandler) {
		w.maxBatchSizeBytes = n
	}
}

// ServeHTTP implements http.Handler
func (h *WriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.router.ServeHTTP(w, r)
}

// handleWrite handles requests for the v1 write endpoint
func (h *WriteHandler) handleWrite(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "WriteHandler")
	defer span.Finish()

	ctx := r.Context()
	auth, err := getAuthorization(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	sw := kithttp.NewStatusResponseWriter(w)
	recorder := newWriteUsageRecorder(sw, h.EventRecorder)
	var requestBytes int
	defer func() {
		// Close around the requestBytes variable to placate the linter.
		recorder.Record(ctx, requestBytes, auth.OrgID, r.URL.Path)
	}()

	req, err := decodeWriteRequest(ctx, r, h.maxBatchSizeBytes)
	if err != nil {
		h.HandleHTTPError(ctx, err, sw)
		return
	}

	bucket, err := h.findBucket(ctx, auth.OrgID, req.Database, req.RetentionPolicy)
	if err != nil {
		h.HandleHTTPError(ctx, err, sw)
		return
	}
	span.LogKV("bucket_id", bucket.ID)

	if err := checkBucketWritePermissions(auth, bucket.OrgID, bucket.ID); err != nil {
		h.HandleHTTPError(ctx, err, sw)
		return
	}

	parsed, err := points.NewParser(req.Precision).Parse(ctx, auth.OrgID, bucket.ID, req.Body)
	if err != nil {
		h.HandleHTTPError(ctx, err, sw)
		return
	}

	if err := h.PointsWriter.WritePoints(ctx, auth.OrgID, bucket.ID, parsed.Points); err != nil {
		h.HandleHTTPError(ctx, &influxdb.Error{
			Code: influxdb.EInternal,
			Op:   opWriteHandler,
			Msg:  "unexpected error writing points to database",
			Err:  err,
		}, sw)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// findBucket finds a bucket for the specified database and
// retention policy combination.
func (h *WriteHandler) findBucket(ctx context.Context, orgID influxdb.ID, db, rp string) (*influxdb.Bucket, error) {
	mapping, err := h.findMapping(ctx, orgID, db, rp)
	if err != nil {
		return nil, err
	}

	return h.BucketService.FindBucketByID(ctx, mapping.BucketID)
}

// checkBucketWritePermissions checks an Authorizer for write permissions to a
// specific Bucket.
func checkBucketWritePermissions(auth influxdb.Authorizer, orgID, bucketID influxdb.ID) error {
	p, err := influxdb.NewPermissionAtID(bucketID, influxdb.WriteAction, influxdb.BucketsResourceType, orgID)
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Op:   opWriteHandler,
			Msg:  fmt.Sprintf("unable to create permission for bucket: %v", err),
			Err:  err,
		}
	}
	if pset, err := auth.PermissionSet(); err != nil || !pset.Allowed(*p) {
		return &influxdb.Error{
			Code: influxdb.EForbidden,
			Op:   opWriteHandler,
			Msg:  "insufficient permissions for write",
			Err:  err,
		}
	}
	return nil
}

// findMapping finds a DBRPMappingV2 for the database and retention policy
// combination.
func (h *WriteHandler) findMapping(ctx context.Context, orgID influxdb.ID, db, rp string) (*influxdb.DBRPMappingV2, error) {
	filter := influxdb.DBRPMappingFilterV2{
		OrgID:    &orgID,
		Database: &db,
	}
	if rp != "" {
		filter.RetentionPolicy = &rp
	} else {
		b := true // Can't get a direct pointer to `true`...
		filter.Default = &b
	}

	mappings, count, err := h.DBRPMappingService.FindMany(ctx, filter)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "no dbrp mapping found",
		}
	}
	return mappings[0], nil
}

// writeRequest is a transport-agnostic write request. It holds all inputs for
// processing a v1 write request.
type writeRequest struct {
	OrganizationName string
	Database         string
	RetentionPolicy  string
	Precision        string
	Body             io.ReadCloser
}

// decodeWriteRequest extracts write request information from an inbound
// http.Request and returns a writeRequest.
func decodeWriteRequest(_ context.Context, r *http.Request, maxBatchSizeBytes int64) (*writeRequest, error) {
	qp := r.URL.Query()
	precision := qp.Get("precision")
	if precision == "" {
		precision = "ns"
	}
	db := qp.Get("db")
	if db == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "missing db",
		}
	}

	encoding := r.Header.Get("Content-Encoding")
	body, err := points.BatchReadCloser(r.Body, encoding, maxBatchSizeBytes)
	if err != nil {
		return nil, err
	}

	return &writeRequest{
		OrganizationName: qp.Get("org"),
		Database:         db,
		RetentionPolicy:  qp.Get("rp"),
		Precision:        precision,
		Body:             body,
	}, nil
}
