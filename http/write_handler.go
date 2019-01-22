package http

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	platform "github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// WriteHandler receives line protocol and sends to a publish function.
type WriteHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	BucketService       platform.BucketService
	OrganizationService platform.OrganizationService

	PointsWriter storage.PointsWriter
}

const (
	writePath            = "/api/v2/write"
	errInvalidGzipHeader = "gzipped HTTP body contains an invalid header"
	errInvalidPrecision  = "invalid precision; valid precision units are ns, us, ms, and s"
)

// NewWriteHandler creates a new handler at /api/v2/write to receive line protocol.
func NewWriteHandler(writer storage.PointsWriter) *WriteHandler {
	h := &WriteHandler{
		Router:       NewRouter(),
		Logger:       zap.NewNop(),
		PointsWriter: writer,
	}

	h.HandlerFunc("POST", writePath, h.handleWrite)
	return h
}

func (h *WriteHandler) handleWrite(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	defer r.Body.Close()

	in := r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		var err error
		in, err = gzip.NewReader(r.Body)
		if err != nil {
			EncodeError(ctx, &platform.Error{
				Code: platform.EInvalid,
				Op:   "http/handleWrite",
				Msg:  errInvalidGzipHeader,
				Err:  err,
			}, w)
			return
		}
		defer in.Close()
	}

	a, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	req, err := decodeWriteRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	logger := h.Logger.With(zap.String("org", req.Org), zap.String("bucket", req.Bucket))

	var org *platform.Organization
	if id, err := platform.IDFromString(req.Org); err == nil {
		// Decoded ID successfully. Make sure it's a real org.
		o, err := h.OrganizationService.FindOrganizationByID(ctx, *id)
		if err == nil {
			org = o
		} else if platform.ErrorCode(err) != platform.ENotFound {
			EncodeError(ctx, err, w)
			return
		}
	}
	if org == nil {
		o, err := h.OrganizationService.FindOrganization(ctx, platform.OrganizationFilter{Name: &req.Org})
		if err != nil {
			logger.Info("Failed to find organization", zap.Error(err))
			EncodeError(ctx, err, w)
			return
		}

		org = o
	}

	var bucket *platform.Bucket
	if id, err := platform.IDFromString(req.Bucket); err == nil {
		// Decoded ID successfully. Make sure it's a real bucket.
		b, err := h.BucketService.FindBucket(ctx, platform.BucketFilter{
			OrganizationID: &org.ID,
			ID:             id,
		})
		if err == nil {
			bucket = b
		} else if platform.ErrorCode(err) != platform.ENotFound {
			EncodeError(ctx, err, w)
			return
		}
	}

	if bucket == nil {
		b, err := h.BucketService.FindBucket(ctx, platform.BucketFilter{
			OrganizationID: &org.ID,
			Name:           &req.Bucket,
		})
		if err != nil {
			EncodeError(ctx, &platform.Error{
				Op:  "http/handleWrite",
				Err: err,
			}, w)
			return
		}

		bucket = b
	}

	p, err := platform.NewPermissionAtID(bucket.ID, platform.WriteAction, platform.BucketsResourceType, org.ID)
	if err != nil {
		EncodeError(ctx, &platform.Error{
			Code: platform.EInternal,
			Op:   "http/handleWrite",
			Msg:  fmt.Sprintf("unable to create permission for bucket: %v", err),
			Err:  err,
		}, w)
		return
	}

	if !a.Allowed(*p) {
		EncodeError(ctx, &platform.Error{
			Code: platform.EForbidden,
			Op:   "http/handleWrite",
			Msg:  "insufficient permissions for write",
		}, w)
		return
	}

	// TODO(jeff): we should be publishing with the org and bucket instead of
	// parsing, rewriting, and publishing, but the interface isn't quite there yet.
	// be sure to remove this when it is there!
	data, err := ioutil.ReadAll(in)
	if err != nil {
		logger.Error("Error reading body", zap.Error(err))
		EncodeError(ctx, &platform.Error{
			Code: platform.EInternal,
			Op:   "http/handleWrite",
			Msg:  fmt.Sprintf("unable to read data: %v", err),
			Err:  err,
		}, w)
		return
	}

	points, err := models.ParsePointsWithPrecision(data, time.Now(), req.Precision)
	if err != nil {
		logger.Error("Error parsing points", zap.Error(err))
		EncodeError(ctx, &platform.Error{
			Code: platform.EInvalid,
			Op:   "http/handleWrite",
			Msg:  fmt.Sprintf("unable to parse points: %v", err),
			Err:  err,
		}, w)
		return
	}

	exploded, err := tsdb.ExplodePoints(org.ID, bucket.ID, points)
	if err != nil {
		logger.Error("Error exploding points", zap.Error(err))
		EncodeError(ctx, &platform.Error{
			Code: platform.EInternal,
			Op:   "http/handleWrite",
			Msg:  fmt.Sprintf("unable to convert points to internal structures: %v", err),
			Err:  err,
		}, w)
		return
	}

	if err := h.PointsWriter.WritePoints(exploded); err != nil {
		logger.Error("Error writing points", zap.Error(err))
		EncodeError(ctx, &platform.Error{
			Code: platform.EInternal,
			Op:   "http/handleWrite",
			Msg:  fmt.Sprintf("unable to write points to database: %v", err),
			Err:  err,
		}, w)
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
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Op:   "http/decodeWriteRequest",
			Msg:  errInvalidPrecision,
		}
	}

	return &postWriteRequest{
		Bucket:    qp.Get("bucket"),
		Org:       qp.Get("org"),
		Precision: p,
	}, nil
}

type postWriteRequest struct {
	Org       string
	Bucket    string
	Precision string
}

// WriteService sends data over HTTP to influxdb via line protocol.
type WriteService struct {
	Addr               string
	Token              string
	Precision          string
	InsecureSkipVerify bool
}

var _ platform.WriteService = (*WriteService)(nil)

func (s *WriteService) Write(ctx context.Context, orgID, bucketID platform.ID, r io.Reader) error {
	precision := s.Precision
	if precision == "" {
		precision = "ns"
	}

	if !models.ValidPrecision(precision) {
		return &platform.Error{
			Code: platform.EInvalid,
			Op:   "http/Write",
			Msg:  errInvalidPrecision,
		}
	}

	u, err := newURL(s.Addr, writePath)
	if err != nil {
		return err
	}

	r, err = compressWithGzip(r)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), r)
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

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckError(resp, true)
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
