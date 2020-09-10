package legacy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

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
	opPointsWriter = "http/v1PointsWriter"
	opWriteHandler = "http/v1WriteHandler"

	autoCreatedBucketDescription     = "Auto-created from v1 db/rp mapping."
	autoCreatedBucketRetentionPeriod = 3 * 24 * time.Hour
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
	//parserOptions     []models.ParserOption
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

//// WithParserOptions configures options for points parsing
//func WithParserOptions(opts ...models.ParserOption) WriteHandlerOption {
//	return func(w *WriteHandler) {
//		w.parserOptions = opts
//	}
//}

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

	bucket, err := h.findOrCreateMappedBucket(ctx, auth.OrgID, req.Database, req.RetentionPolicy)
	if err != nil {
		h.HandleHTTPError(ctx, err, sw)
		return
	}
	span.LogKV("bucket_id", bucket.ID)

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

// findOrCreateMappedBucket finds a DBRPMappingV2 for the database and
// retention policy combination. If the mapping doesn't exist, it will be
// created and bound to either an existing Bucket or a new one created for this
// purpose.
func (h *WriteHandler) findOrCreateMappedBucket(ctx context.Context, orgID influxdb.ID, db, rp string) (*influxdb.Bucket, error) {
	mapping, err := h.findMapping(ctx, orgID, db, rp)
	if err == nil {
		return h.BucketService.FindBucketByID(ctx, mapping.BucketID)
	}

	if !isErrNotFound(err) {
		return nil, err
	}

	bucket, err := h.mapToBucket(ctx, orgID, db, rp)
	if err != nil {
		return nil, err
	}
	return bucket, nil
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

// createMapping creates a DBRPMappingV2 for the database and retention policy
// combination.
func (h *WriteHandler) createMapping(ctx context.Context, orgID, bucketID influxdb.ID, db, rp string) error {
	return h.DBRPMappingService.Create(ctx, &influxdb.DBRPMappingV2{
		OrganizationID:  orgID,
		BucketID:        bucketID,
		Database:        db,
		RetentionPolicy: rp,
	})
}

// mapToBucket creates a new DBRPMappingV2 to either an existing Bucket (if it
// can find it) or a new one it creates for this purpose.
func (h *WriteHandler) mapToBucket(ctx context.Context, orgID influxdb.ID, db, rp string) (*influxdb.Bucket, error) {
	if rp == "" {
		rp = "autogen"
	}

	name := fmt.Sprintf("%s/%s", db, rp)
	bucket, err := h.BucketService.FindBucket(ctx, influxdb.BucketFilter{
		OrganizationID: &orgID,
		Name:           &name,
	})
	if err == nil {
		if err := h.createMapping(ctx, orgID, bucket.ID, db, rp); err != nil {
			return nil, err
		}
		return bucket, nil
	}
	if !isErrNotFound(err) {
		return nil, err
	}

	now := time.Now().UTC()
	bucket = &influxdb.Bucket{
		Type:                influxdb.BucketTypeUser,
		Name:                name,
		Description:         autoCreatedBucketDescription,
		OrgID:               orgID,
		RetentionPolicyName: rp,
		RetentionPeriod:     autoCreatedBucketRetentionPeriod,
		CRUDLog: influxdb.CRUDLog{
			CreatedAt: now,
			UpdatedAt: now,
		},
	}
	err = h.BucketService.CreateBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if err := h.createMapping(ctx, orgID, bucket.ID, db, rp); err != nil {
		return nil, err
	}
	return bucket, nil
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
func decodeWriteRequest(ctx context.Context, r *http.Request, maxBatchSizeBytes int64) (*writeRequest, error) {
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

func isErrNotFound(err error) bool {
	var idErr *influxdb.Error
	return errors.As(err, &idErr) && idErr.Code == influxdb.ENotFound
}
