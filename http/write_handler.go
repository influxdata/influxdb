package http

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/http/metric"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/tsdb"
	"go.uber.org/zap"
)

var (
	// ErrMaxBatchSizeExceeded is returned when a points batch exceeds
	// the defined upper limit in bytes. This pertains to the size of the
	// batch after inflation from any compression (i.e. ungzipped).
	ErrMaxBatchSizeExceeded = errors.New("points batch is too large")
)

// WriteBackend is all services and associated parameters required to construct
// the WriteHandler.
type WriteBackend struct {
	influxdb.HTTPErrorHandler
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
	*httprouter.Router
	influxdb.HTTPErrorHandler
	log *zap.Logger

	BucketService       influxdb.BucketService
	OrganizationService influxdb.OrganizationService

	PointsWriter storage.PointsWriter

	EventRecorder metric.EventRecorder

	maxBatchSizeBytes int64
	parserOptions     []models.ParserOption
	parserMaxBytes    int
	parserMaxLines    int
	parserMaxValues   int
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

// WithParserMaxBytes specifies the maximum number of bytes that may be allocated when processing a single
// write request. When n is zero, there is no limit.
func WithParserMaxBytes(n int) WriteHandlerOption {
	return func(w *WriteHandler) {
		w.parserMaxBytes = n
	}
}

// WithParserMaxLines specifies the maximum number of lines that may be parsed when processing a single
// write request. When n is zero, there is no limit.
func WithParserMaxLines(n int) WriteHandlerOption {
	return func(w *WriteHandler) {
		w.parserMaxLines = n
	}
}

// WithParserMaxValues specifies the maximum number of values that may be parsed when processing a single
// write request. When n is zero, there is no limit.
func WithParserMaxValues(n int) WriteHandlerOption {
	return func(w *WriteHandler) {
		w.parserMaxValues = n
	}
}

// Prefix provides the route prefix.
func (*WriteHandler) Prefix() string {
	return prefixWrite
}

const (
	prefixWrite          = "/api/v2/write"
	errInvalidGzipHeader = "gzipped HTTP body contains an invalid header"
	errInvalidPrecision  = "invalid precision; valid precision units are ns, us, ms, and s"
)

// NewWriteHandler creates a new handler at /api/v2/write to receive line protocol.
func NewWriteHandler(log *zap.Logger, b *WriteBackend, opts ...WriteHandlerOption) *WriteHandler {
	h := &WriteHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		PointsWriter:        b.PointsWriter,
		BucketService:       b.BucketService,
		OrganizationService: b.OrganizationService,
		EventRecorder:       b.WriteEventRecorder,
	}

	for _, opt := range opts {
		opt(h)
	}

	// cache configured options
	if h.parserMaxBytes > 0 {
		h.parserOptions = append(h.parserOptions, models.WithParserMaxBytes(h.parserMaxBytes))
	}
	if h.parserMaxLines > 0 {
		h.parserOptions = append(h.parserOptions, models.WithParserMaxLines(h.parserMaxLines))
	}
	if h.parserMaxValues > 0 {
		h.parserOptions = append(h.parserOptions, models.WithParserMaxValues(h.parserMaxValues))
	}

	h.HandlerFunc("POST", prefixWrite, h.handleWrite)
	return h
}

func (h *WriteHandler) handleWrite(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "WriteHandler")
	defer span.Finish()

	ctx := r.Context()
	defer r.Body.Close()

	// TODO(desa): I really don't like how we're recording the usage metrics here
	// Ideally this will be moved when we solve https://github.com/influxdata/influxdb/issues/13403
	var (
		orgID        influxdb.ID
		requestBytes int
		sw           = kithttp.NewStatusResponseWriter(w)
		handleError  = func(err error, code, message string) {
			h.HandleHTTPError(ctx, &influxdb.Error{
				Code: code,
				Op:   "http/handleWrite",
				Msg:  message,
				Err:  err,
			}, w)
		}
	)
	w = sw
	defer func() {
		h.EventRecorder.Record(ctx, metric.Event{
			OrgID:         orgID,
			Endpoint:      r.URL.Path, // This should be sufficient for the time being as it should only be single endpoint.
			RequestBytes:  requestBytes,
			ResponseBytes: sw.ResponseBytes(),
			Status:        sw.Code(),
		})
	}()

	a, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	req, err := decodeWriteRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	log := h.log.With(zap.String("org", req.Org), zap.String("bucket", req.Bucket))

	var org *influxdb.Organization
	org, err = queryOrganization(ctx, r, h.OrganizationService)
	if err != nil {
		log.Info("Failed to find organization", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	orgID = org.ID
	span.LogKV("org_id", orgID)

	var bucket *influxdb.Bucket
	if id, err := influxdb.IDFromString(req.Bucket); err == nil {
		// Decoded ID successfully. Make sure it's a real bucket.
		b, err := h.BucketService.FindBucket(ctx, influxdb.BucketFilter{
			OrganizationID: &org.ID,
			ID:             id,
		})
		if err == nil {
			bucket = b
		} else if influxdb.ErrorCode(err) != influxdb.ENotFound {
			h.HandleHTTPError(ctx, err, w)
			return
		}
	}

	if bucket == nil {
		b, err := h.BucketService.FindBucket(ctx, influxdb.BucketFilter{
			OrganizationID: &org.ID,
			Name:           &req.Bucket,
		})
		if err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}

		bucket = b
	}
	span.LogKV("bucket_id", bucket.ID)

	p, err := influxdb.NewPermissionAtID(bucket.ID, influxdb.WriteAction, influxdb.BucketsResourceType, org.ID)
	if err != nil {
		handleError(err, influxdb.EInternal, fmt.Sprintf("unable to create permission for bucket: %v", err))
		return
	}

	if pset, err := a.PermissionSet(); err != nil || !pset.Allowed(*p) {
		handleError(nil, influxdb.EForbidden, "insufficient permissions for write")
		return
	}

	data, err := readWriteRequest(ctx, r.Body, r.Header.Get("Content-Encoding"), h.maxBatchSizeBytes)
	if err != nil {
		log.Error("Error reading body", zap.Error(err))

		code := influxdb.EInternal
		if errors.Is(err, ErrMaxBatchSizeExceeded) {
			code = influxdb.ETooLarge
		} else if errors.Is(err, gzip.ErrHeader) || errors.Is(err, gzip.ErrChecksum) {
			code = influxdb.EInvalid
		}

		handleError(err, code, "unable to read data")
		return
	}

	requestBytes = len(data)
	if requestBytes == 0 {
		handleError(err, influxdb.EInvalid, "writing requires points")
		return
	}

	span, _ = tracing.StartSpanFromContextWithOperationName(ctx, "encoding and parsing")
	encoded := tsdb.EncodeName(org.ID, bucket.ID)
	mm := models.EscapeMeasurement(encoded[:])

	var options []models.ParserOption
	if len(h.parserOptions) > 0 {
		options = make([]models.ParserOption, 0, len(h.parserOptions)+1)
		options = append(options, h.parserOptions...)
	}

	if req.Precision != nil {
		options = append(options, req.Precision)
	}

	points, err := models.ParsePointsWithOptions(data, mm, options...)
	span.LogKV("values_total", len(points))
	span.Finish()
	if err != nil {
		log.Error("Error parsing points", zap.Error(err))

		code := influxdb.EInvalid
		if errors.Is(err, models.ErrLimitMaxBytesExceeded) ||
			errors.Is(err, models.ErrLimitMaxLinesExceeded) ||
			errors.Is(err, models.ErrLimitMaxValuesExceeded) {
			code = influxdb.ETooLarge
		}

		handleError(err, code, "")
		return
	}

	if err := h.PointsWriter.WritePoints(ctx, points); err != nil {
		log.Error("Error writing points", zap.Error(err))
		handleError(err, influxdb.EInternal, "unexpected error writing points to database")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func decodeWriteRequest(ctx context.Context, r *http.Request) (*postWriteRequest, error) {
	qp := r.URL.Query()
	p := qp.Get("precision")
	if p == "" {
		p = "ns"
	}

	if !models.ValidPrecision(p) {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Op:   "http/decodeWriteRequest",
			Msg:  errInvalidPrecision,
		}
	}

	var precision models.ParserOption
	if p != "ns" {
		precision = models.WithParserPrecision(p)
	}

	return &postWriteRequest{
		Bucket:    qp.Get("bucket"),
		Org:       qp.Get("org"),
		Precision: precision,
	}, nil
}

func readWriteRequest(ctx context.Context, rc io.ReadCloser, encoding string, maxBatchSizeBytes int64) (v []byte, err error) {
	defer func() {
		// close the reader now that all bytes have been consumed
		// this will return non-nil in the case of a configured limit
		// being exceeded
		if cerr := rc.Close(); err == nil {
			err = cerr
		}
	}()

	switch encoding {
	case "gzip", "x-gzip":
		rc, err = gzip.NewReader(rc)
		if err != nil {
			return nil, err
		}
	}

	// given a limit is configured on the number of bytes in a
	// batch then wrap the reader in a limited reader
	if maxBatchSizeBytes > 0 {
		rc = newLimitedReadCloser(rc, maxBatchSizeBytes)
	}

	span, _ := tracing.StartSpanFromContextWithOperationName(ctx, "read request body")
	defer func() {
		span.LogKV("request_bytes", len(v))
		span.Finish()
	}()

	return ioutil.ReadAll(rc)
}

type postWriteRequest struct {
	Org       string
	Bucket    string
	Precision models.ParserOption
}

// WriteService sends data over HTTP to influxdb via line protocol.
type WriteService struct {
	Addr               string
	Token              string
	Precision          string
	InsecureSkipVerify bool
}

var _ influxdb.WriteService = (*WriteService)(nil)

func (s *WriteService) Write(ctx context.Context, orgID, bucketID influxdb.ID, r io.Reader) error {
	precision := s.Precision
	if precision == "" {
		precision = "ns"
	}

	if !models.ValidPrecision(precision) {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Op:   "http/Write",
			Msg:  errInvalidPrecision,
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

	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), r)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Content-Encoding", "gzip")
	SetToken(s.Token, req)

	org, err := orgID.Encode()
	if err != nil {
		return err
	}

	bucket, err := bucketID.Encode()
	if err != nil {
		return err
	}

	params := req.URL.Query()
	params.Set("org", string(org))
	params.Set("bucket", string(bucket))
	params.Set("precision", string(precision))
	req.URL.RawQuery = params.Encode()

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckError(resp)
}

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

type limitedReader struct {
	*io.LimitedReader
	err   error
	close func() error
}

func newLimitedReadCloser(r io.ReadCloser, n int64) *limitedReader {
	// read up to max + 1 as limited reader just returns EOF when the limit is reached
	// or when there is nothing left to read. If we exceed the max batch size by one
	// then we know the limit has been passed.
	return &limitedReader{
		LimitedReader: &io.LimitedReader{R: r, N: n + 1},
		close:         r.Close,
	}
}

// Close returns an ErrMaxBatchSizeExceeded when the wrapped reader
// exceeds the set limit for number of bytes.
// This is safe to call more than once but not concurrently.
func (l *limitedReader) Close() (err error) {
	defer func() {
		if cerr := l.close(); cerr != nil && err == nil {
			err = cerr
		}

		// only call close once
		l.close = func() error { return nil }
	}()

	if l.N < 1 {
		l.err = ErrMaxBatchSizeExceeded
	}

	return l.err
}
