package http

import (
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

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

// NewWriteHandler creates a new handler at /api/v2/write to receive line protocol.
func NewWriteHandler(writer storage.PointsWriter) *WriteHandler {
	h := &WriteHandler{
		Router: httprouter.New(),
		Logger: zap.NewNop(),
		PointsWriter: writer,
	}

	h.HandlerFunc("POST", "/api/v2/write", h.handleWrite)
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

	req := decodeWriteRequest(ctx, r)
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

		points, err := models.ParsePoints(data)
		if err != nil {
			logger.Info("Error parsing points", zap.Error(err))
			EncodeError(ctx, err, w)
			return
		}

		exploded, err := tsdb.ExplodePoints([]byte(org.ID), []byte(bucket.ID), points)
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

func decodeWriteRequest(ctx context.Context, r *http.Request) *postWriteRequest {
	qp := r.URL.Query()

	return &postWriteRequest{
		Bucket: qp.Get("bucket"),
		Org:    qp.Get("org"),
	}
}

type postWriteRequest struct {
	Org    string
	Bucket string
}
