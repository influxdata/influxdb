package http

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/http/metric"
	"github.com/influxdata/influxdb/v2/http/points"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/tsdb"
	"go.uber.org/zap"
)

// WriteBackend is all services and associated parameters required to construct
// the WriteHandler.
type WriteBackend struct {
	errors.HTTPErrorHandler
	log                *zap.Logger
	WriteEventRecorder metric.EventRecorder

	PointsWriter        storage.PointsWriter
	BucketService       influxdb.BucketService
	OrganizationService influxdb.OrganizationService
}

// NewWriteBackend returns a new instance of WriteBackend.
func NewWriteBackend(log *zap.Logger, b *APIBackend) *WriteBackend {
	return &WriteBackend{
		HTTPErrorHandler:   b.HTTPErrorHandler,
		log:                log,
		WriteEventRecorder: b.WriteEventRecorder,

		PointsWriter:        b.PointsWriter,
		BucketService:       b.BucketService,
		OrganizationService: b.OrganizationService,
	}
}

// WriteHandler receives line protocol and sends to a publish function.
type WriteHandler struct {
	errors.HTTPErrorHandler
	BucketService       influxdb.BucketService
	OrganizationService influxdb.OrganizationService
	PointsWriter        storage.PointsWriter
	EventRecorder       metric.EventRecorder

	router            *httprouter.Router
	log               *zap.Logger
	maxBatchSizeBytes int64
	// parserOptions     []models.ParserOption
}

// WriteHandlerOption is a functional option for a *WriteHandler
type WriteHandlerOption func(*WriteHandler)

// WithMaxBatchSizeBytes configures the maximum size for a
// (decompressed) points batch allowed by the write handler
func WithMaxBatchSizeBytes(n int64) WriteHandlerOption {
	return func(w *WriteHandler) {
		w.maxBatchSizeBytes = n
	}
}

//func WithParserOptions(opts ...models.ParserOption) WriteHandlerOption {
//	return func(w *WriteHandler) {
//		w.parserOptions = opts
//	}
//}

// Prefix provides the route prefix.
func (*WriteHandler) Prefix() string {
	return prefixWrite
}

const (
	prefixWrite          = "/api/v2/write"
	msgInvalidGzipHeader = "gzipped HTTP body contains an invalid header"
	msgInvalidPrecision  = "invalid precision; valid precision units are ns, us, ms, and s"

	opWriteHandler = "http/writeHandler"
)

// NewWriteHandler creates a new handler at /api/v2/write to receive line protocol.
func NewWriteHandler(log *zap.Logger, b *WriteBackend, opts ...WriteHandlerOption) *WriteHandler {
	h := &WriteHandler{
		HTTPErrorHandler:    b.HTTPErrorHandler,
		PointsWriter:        b.PointsWriter,
		BucketService:       b.BucketService,
		OrganizationService: b.OrganizationService,
		EventRecorder:       b.WriteEventRecorder,

		router: NewRouter(b.HTTPErrorHandler),
		log:    log,
	}

	for _, opt := range opts {
		opt(h)
	}

	h.router.HandlerFunc(http.MethodPost, prefixWrite, h.handleWrite)
	return h
}

func (h *WriteHandler) findBucket(ctx context.Context, orgID platform.ID, bucket string) (*influxdb.Bucket, error) {
	if id, err := platform.IDFromString(bucket); err == nil {
		b, err := h.BucketService.FindBucket(ctx, influxdb.BucketFilter{
			OrganizationID: &orgID,
			ID:             id,
		})
		if err != nil && errors.ErrorCode(err) != errors.ENotFound {
			return nil, err
		} else if err == nil {
			return b, err
		}
	}

	return h.BucketService.FindBucket(ctx, influxdb.BucketFilter{
		OrganizationID: &orgID,
		Name:           &bucket,
	})
}

func (h *WriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.router.ServeHTTP(w, r)
}

func (h *WriteHandler) handleWrite(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "WriteHandler")
	defer span.Finish()

	ctx := r.Context()
	auth, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	req, err := decodeWriteRequest(ctx, r, h.maxBatchSizeBytes)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	org, err := queryOrganization(ctx, r, h.OrganizationService)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	span.LogKV("org_id", org.ID)

	sw := kithttp.NewStatusResponseWriter(w)
	recorder := NewWriteUsageRecorder(sw, h.EventRecorder)
	var requestBytes int
	defer func() {
		// Close around the requestBytes variable to placate the linter.
		recorder.Record(ctx, requestBytes, org.ID, r.URL.Path)
	}()

	bucket, err := h.findBucket(ctx, org.ID, req.Bucket)
	if err != nil {
		h.HandleHTTPError(ctx, err, sw)
		return
	}
	span.LogKV("bucket_id", bucket.ID)

	if err := checkBucketWritePermissions(auth, org.ID, bucket.ID); err != nil {
		h.HandleHTTPError(ctx, err, sw)
		return
	}

	// TODO: Backport?
	//opts := append([]models.ParserOption{}, h.parserOptions...)
	//opts = append(opts, models.WithParserPrecision(req.Precision))
	parsed, err := points.NewParser(req.Precision).Parse(ctx, org.ID, bucket.ID, req.Body)
	if err != nil {
		h.HandleHTTPError(ctx, err, sw)
		return
	}
	requestBytes = parsed.RawSize

	if err := h.PointsWriter.WritePoints(ctx, org.ID, bucket.ID, parsed.Points); err != nil {
		if partialErr, ok := err.(tsdb.PartialWriteError); ok {
			h.HandleHTTPError(ctx, &errors.Error{
				Code: errors.EUnprocessableEntity,
				Op:   opWriteHandler,
				Msg:  "failure writing points to database",
				Err:  partialErr,
			}, sw)
			return
		}

		h.HandleHTTPError(ctx, &errors.Error{
			Code: errors.EInternal,
			Op:   opWriteHandler,
			Msg:  "unexpected error writing points to database",
			Err:  err,
		}, sw)
		return
	}

	sw.WriteHeader(http.StatusNoContent)
}

// checkBucketWritePermissions checks an Authorizer for write permissions to a
// specific Bucket.
func checkBucketWritePermissions(auth influxdb.Authorizer, orgID, bucketID platform.ID) error {
	p, err := influxdb.NewPermissionAtID(bucketID, influxdb.WriteAction, influxdb.BucketsResourceType, orgID)
	if err != nil {
		return &errors.Error{
			Code: errors.EInternal,
			Op:   opWriteHandler,
			Msg:  fmt.Sprintf("unable to create permission for bucket: %v", err),
			Err:  err,
		}
	}
	if pset, err := auth.PermissionSet(); err != nil || !pset.Allowed(*p) {
		return &errors.Error{
			Code: errors.EForbidden,
			Op:   opWriteHandler,
			Msg:  "insufficient permissions for write",
			Err:  err,
		}
	}
	return nil
}

// writeRequest is a request object holding information about a batch of points
// to be written to a Bucket.
type writeRequest struct {
	Org       string
	Bucket    string
	Precision string
	Body      io.ReadCloser
}

// decodeWriteRequest extracts information from an http.Request object to
// produce a writeRequest.
func decodeWriteRequest(ctx context.Context, r *http.Request, maxBatchSizeBytes int64) (*writeRequest, error) {
	qp := r.URL.Query()
	precision := qp.Get("precision")
	if precision == "" {
		precision = "ns"
	}

	if !models.ValidPrecision(precision) {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Op:   "http/newWriteRequest",
			Msg:  msgInvalidPrecision,
		}
	}

	bucket := qp.Get("bucket")
	if bucket == "" {
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Op:   "http/newWriteRequest",
			Msg:  "bucket not found",
		}
	}

	encoding := r.Header.Get("Content-Encoding")
	body, err := points.BatchReadCloser(r.Body, encoding, maxBatchSizeBytes)
	if err != nil {
		return nil, err
	}

	return &writeRequest{
		Bucket:    qp.Get("bucket"),
		Org:       qp.Get("org"),
		Precision: precision,
		Body:      body,
	}, nil
}

// WriteService sends data over HTTP to influxdb via line protocol.
type WriteService struct {
	Addr               string
	Token              string
	Precision          string
	InsecureSkipVerify bool
}

var _ influxdb.WriteService = (*WriteService)(nil)

func compressWithGzip(data io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()
	gw := gzip.NewWriter(pw)
	var err error

	go func() {
		_, err = io.Copy(gw, data)
		gw.Close()
		pw.Close()
	}()

	return pr, err
}

// WriteTo writes to the bucket matching the filter.
func (s *WriteService) WriteTo(ctx context.Context, filter influxdb.BucketFilter, r io.Reader) error {
	precision := s.Precision
	if precision == "" {
		precision = "ns"
	}

	if !models.ValidPrecision(precision) {
		return &errors.Error{
			Code: errors.EInvalid,
			Op:   "http/Write",
			Msg:  msgInvalidPrecision,
		}
	}

	u, err := NewURL(s.Addr, prefixWrite)
	if err != nil {
		return err
	}

	r, err = compressWithGzip(r)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), r)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Content-Encoding", "gzip")
	SetToken(s.Token, req)

	params := req.URL.Query()

	// In other CLI commands that take either an ID or a name as input, the ID
	// is prioritized and used to short-circuit looking up the name. We simulate
	// the same behavior here for a consistent experience.
	if filter.OrganizationID != nil && filter.OrganizationID.Valid() {
		params.Set("org", filter.OrganizationID.String())
	} else if filter.Org != nil && *filter.Org != "" {
		params.Set("org", *filter.Org)
	}
	if filter.ID != nil && filter.ID.Valid() {
		params.Set("bucket", filter.ID.String())
	} else if filter.Name != nil && *filter.Name != "" {
		params.Set("bucket", *filter.Name)
	}
	params.Set("precision", precision)
	req.URL.RawQuery = params.Encode()

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckError(resp)
}
