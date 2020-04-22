package feature

import (
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
