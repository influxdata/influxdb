package http

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/influxdata/platform"
	pcontext "github.com/influxdata/platform/context"
	"github.com/influxdata/platform/kit/errors"
	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/storage"
	"github.com/influxdata/platform/tsdb"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// WriteHandler receives line protocol and sends to a publish function.
type WriteHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	AuthorizationService platform.AuthorizationService
	BucketService        platform.BucketService
	OrganizationService  platform.OrganizationService

	PointsWriter storage.PointsWriter
}

const (
	writePath = "/api/v2/write"
)

// NewWriteHandler creates a new handler at /api/v2/write to receive line protocol.
func NewWriteHandler(writer storage.PointsWriter) *WriteHandler {
	h := &WriteHandler{
		Router:       httprouter.New(),
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
			EncodeError(ctx, errors.Wrap(err, "invalid gzip", errors.InvalidData), w)
			return
		}
		defer in.Close()
	}

	a, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	auth, err := h.AuthorizationService.FindAuthorizationByID(ctx, a.Identifier())
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
		} else if err != ErrNotFound {
			EncodeError(ctx, err, w)
			return
		}
	}
	if org == nil {
		o, err := h.OrganizationService.FindOrganization(ctx, platform.OrganizationFilter{Name: &req.Org})
		if err != nil {
			logger.Info("Failed to find organization", zap.Error(err))
			EncodeError(ctx, fmt.Errorf("organization %q not found", req.Org), w)
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
		} else if err != ErrNotFound {
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
			logger.Info("Failed to find bucket", zap.Stringer("org_id", org.ID), zap.Error(err))
			EncodeError(ctx, fmt.Errorf("bucket %q not found", req.Bucket), w)
			return
		}

		bucket = b
	}

	if !auth.Allowed(platform.WriteBucketPermission(bucket.ID)) {
		EncodeError(ctx, errors.Forbiddenf("insufficient permissions for write"), w)
		return
	}

	// TODO(jeff): we should be publishing with the org and bucket instead of
	// parsing, rewriting, and publishing, but the interface isn't quite there yet.
	// be sure to remove this when it is there!
	data, err := ioutil.ReadAll(in)
	if err != nil {
		logger.Info("Error reading body", zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}

	points, err := models.ParsePointsWithPrecision(data, time.Now(), req.Precision)
	if err != nil {
		logger.Info("Error parsing points", zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}

	exploded, err := tsdb.ExplodePoints(org.ID, bucket.ID, points)
	if err != nil {
		logger.Info("Error exploding points", zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}

	if err := h.PointsWriter.WritePoints(exploded); err != nil {
		EncodeError(ctx, errors.BadRequestError(err.Error()), w)
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
		return nil, errors.InvalidDataf("invalid precision")
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
		return fmt.Errorf("invalid precision")
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
