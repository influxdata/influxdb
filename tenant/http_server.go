package tenant

import (
	"context"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	kit "github.com/influxdata/influxdb/v2/kit/transport/http"
)

type tenantContext string

const ctxOrgKey tenantContext = "orgID"

// ValidResource make sure a resource exists when a sub system needs to be mounted to an api
func ValidResource(api *kit.API, lookupOrgByResourceID func(context.Context, influxdb.ID) (influxdb.ID, error)) kit.Middleware {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			statusW := kit.NewStatusResponseWriter(w)
			id, err := influxdb.IDFromString(chi.URLParam(r, "id"))
			if err != nil {
				api.Err(w, ErrCorruptID(err))
				return
			}

			ctx := r.Context()

			orgID, err := lookupOrgByResourceID(ctx, *id)
			if err != nil {
				api.Err(w, err)
				return
			}

			next.ServeHTTP(statusW, r.WithContext(context.WithValue(ctx, ctxOrgKey, orgID)))
		}
		return http.HandlerFunc(fn)
	}
}

func orgIDFromContext(ctx context.Context) *influxdb.ID {
	v := ctx.Value(ctxOrgKey)
	if v == nil {
		return nil
	}
	id := v.(influxdb.ID)
	return &id
}
