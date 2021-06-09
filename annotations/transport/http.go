package transport

import (
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

const (
	// this is the base api prefix, since the annotations service mounts handlers at
	// both the ../annotations and ../streams paths.
	prefixAnnotations = "/api/v2private"
)

var (
	errBadOrg = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "invalid or missing org id",
	}

	errBadAnnotationId = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "annotation id is invalid",
	}

	errBadStreamId = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "stream id is invalid",
	}

	errBadStreamName = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "invalid stream name",
	}
)

// AnnotationsHandler is the handler for the annotation service
type AnnotationHandler struct {
	chi.Router

	log *zap.Logger
	api *kithttp.API

	annotationService influxdb.AnnotationService
}

func NewAnnotationHandler(
	log *zap.Logger,
	annotationService influxdb.AnnotationService,
) *AnnotationHandler {
	h := &AnnotationHandler{
		log:               log,
		api:               kithttp.NewAPI(kithttp.WithLog(log)),
		annotationService: annotationService,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)

	r.Mount("/annotations", h.annotationsRouter())
	r.Mount("/streams", h.streamsRouter())
	h.Router = r

	return h
}

func (h *AnnotationHandler) Prefix() string {
	return prefixAnnotations
}

func tFromReq(r *http.Request) (*time.Time, *time.Time, error) {
	st, err := tStringToPointer(r.URL.Query().Get("startTime"))
	if err != nil {
		return nil, nil, err
	}

	et, err := tStringToPointer(r.URL.Query().Get("endTime"))
	if err != nil {
		return nil, nil, err
	}

	return st, et, nil
}

func tStringToPointer(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}

	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, err
	}
	return &t, nil
}
