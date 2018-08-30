package http

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/influxdata/platform"
	pcontext "github.com/influxdata/platform/context"
	"github.com/influxdata/platform/kit/errors"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

const NatsServerID = "nats"
const NatsClientID = "nats-client"

type WriteHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	AuthorizationService platform.AuthorizationService
	BucketService        platform.BucketService
	OrganizationService  platform.OrganizationService

	Publish func(io.Reader) error
}

func NewWriteHandler(publishFn func(io.Reader) error) *WriteHandler {
	h := &WriteHandler{
		Router:  httprouter.New(),
		Logger:  zap.NewNop(),
		Publish: publishFn,
	}

	h.HandlerFunc("POST", "/v2/write", h.handleWrite)
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

	tok, err := pcontext.GetToken(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	auth, err := h.AuthorizationService.FindAuthorizationByToken(ctx, tok)
	if err != nil {
		EncodeError(ctx, errors.Wrap(err, "invalid token", errors.InvalidData), w)
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
		if o, err := h.OrganizationService.FindOrganizationByID(ctx, *id); err == nil {
			org = o
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
		if b, err := h.BucketService.FindBucket(ctx, platform.BucketFilter{
			OrganizationID: &org.ID,
			ID:             id,
		}); err == nil {
			bucket = b
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

	if !platform.Allowed(platform.WriteBucketPermission(bucket.ID), auth) {
		EncodeError(ctx, errors.Forbiddenf("insufficient permissions for write"), w)
		return
	}

	if err := h.Publish(in); err != nil {
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
