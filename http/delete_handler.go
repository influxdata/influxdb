package http

import (
	"context"
	http "net/http"
	"time"

	platform "github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// DeleteBackend is all services and associated parameters required to construct
// the DeleteHandler.
type DeleteBackend struct {
	Logger *zap.Logger

	BucketService       platform.BucketService
	OrganizationService platform.OrganizationService
}

// NewDeleteBackend returns a new instance of DeleteBackend
func NewDeleteBackend(b *APIBackend) *DeleteBackend {
	return &DeleteBackend{
		Logger: b.Logger.With(zap.String("handler", "delete")),

		BucketService:       b.BucketService,
		OrganizationService: b.OrganizationService,
	}
}

// DeleteHandler receives a delete request with a predicate and sends it to storage.
type DeleteHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	BucketService       platform.BucketService
	OrganizationService platform.OrganizationService
}

const (
	deletePath = "/api/v2/delete"
)

// NewDeleteHandler creates a new handler at /api/v2/delete to recieve delete requests.
func NewDeleteHandler(b *DeleteBackend) *DeleteHandler {
	h := &DeleteHandler{
		Router: NewRouter(),
		Logger: b.Logger,

		BucketService:       b.BucketService,
		OrganizationService: b.OrganizationService,
	}

	h.HandlerFunc("POST", deletePath, h.handleDelete)
	return h
}

func (h *DeleteHandler) handleDelete(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "DeleteHandler")
	defer span.Finish()

	ctx := r.Context()
	defer r.Body.Close()

	_, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	_, err = decodeDeleteRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	// send delete points request to storage

	w.WriteHeader(http.StatusNoContent)
}

func decodeDeleteRequest(ctx context.Context, r *http.Request) (*deleteRequest, error) {
	qp := r.URL.Query()
	start, err := time.Parse(time.RFC3339, qp.Get("start"))
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Op:   "http/Delete",
			Msg:  "start time is not valid RFC3339", // TODO: extract this into a constant
		}
	}

	stop, err := time.Parse(time.RFC3339, qp.Get("stop"))
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Op:   "http/Delete",
			Msg:  "stop time is not valid RFC3339",
		}
	}

	return &deleteRequest{
		Bucket: qp.Get("bucket"),
		Org:    qp.Get("org"),
		Start:  start.UTC(),
		Stop:   stop.UTC(),
	}, nil
}

type deleteRequest struct {
	Org    string
	Bucket string
	Start  time.Time
	Stop   time.Time
}
