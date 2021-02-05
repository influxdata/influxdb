package feature

import (
	"context"
	"encoding/json"
	"net/http"

	"go.uber.org/zap"
)

// Handler is a middleware that annotates the context with a map of computed feature flags.
// To accurately compute identity-scoped flags, this middleware should be executed after any
// authorization middleware has annotated the request context with an authorizer.
type Handler struct {
	log     *zap.Logger
	next    http.Handler
	flagger Flagger
	flags   []Flag
}

// NewHandler returns a configured feature flag middleware that will annotate request context
// with a computed map of the given flags using the provided Flagger.
func NewHandler(log *zap.Logger, flagger Flagger, flags []Flag, next http.Handler) http.Handler {
	return &Handler{
		log:     log,
		next:    next,
		flagger: flagger,
		flags:   flags,
	}
}

// ServeHTTP annotates the request context with a map of computed feature flags before
// continuing to serve the request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, err := Annotate(r.Context(), h.flagger, h.flags...)
	if err != nil {
		h.log.Warn("Unable to annotate context with feature flags", zap.Error(err))
	} else {
		r = r.WithContext(ctx)
	}

	if h.next != nil {
		h.next.ServeHTTP(w, r)
	}
}

// HTTPErrorHandler is an influxdb.HTTPErrorHandler. It's defined here instead
// of referencing the other interface type, because we want to try our best to
// avoid cyclical dependencies when feature package is used throughout the
// codebase.
type HTTPErrorHandler interface {
	HandleHTTPError(ctx context.Context, err error, w http.ResponseWriter)
}

// NewFlagsHandler returns a handler that returns the map of computed feature flags on the request context.
func NewFlagsHandler(errorHandler HTTPErrorHandler, byKey ByKeyFn) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		var (
			ctx   = r.Context()
			flags = ExposedFlagsFromContext(ctx, byKey)
		)
		if err := json.NewEncoder(w).Encode(flags); err != nil {
			errorHandler.HandleHTTPError(ctx, err, w)
		}
	}
	return http.HandlerFunc(fn)
}
