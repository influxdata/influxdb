package tenant

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	kit "github.com/influxdata/influxdb/v2/kit/transport/http"
)

type tenantContext string

const ctxOrgKey tenantContext = "orgID"

// findOptionsParams converts find options into a paramiterizated key pair
func findOptionParams(opts ...influxdb.FindOptions) [][2]string {
	var out [][2]string
	for _, o := range opts {
		for k, vals := range o.QueryParams() {
			for _, v := range vals {
				out = append(out, [2]string{k, v})
			}
		}
	}
	return out
}

// decodeFindOptions returns a FindOptions decoded from http request.
func decodeFindOptions(r *http.Request) (*influxdb.FindOptions, error) {
	opts := &influxdb.FindOptions{}
	qp := r.URL.Query()

	if offset := qp.Get("offset"); offset != "" {
		o, err := strconv.Atoi(offset)
		if err != nil {
			return nil, &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "offset is invalid",
			}
		}

		opts.Offset = o
	}

	if limit := qp.Get("limit"); limit != "" {
		l, err := strconv.Atoi(limit)
		if err != nil {
			return nil, &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "limit is invalid",
			}
		}

		if l < 1 || l > influxdb.MaxPageSize {
			return nil, &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  fmt.Sprintf("limit must be between 1 and %d", influxdb.MaxPageSize),
			}
		}

		opts.Limit = l
	} else {
		opts.Limit = influxdb.DefaultPageSize
	}

	if sortBy := qp.Get("sortBy"); sortBy != "" {
		opts.SortBy = sortBy
	}

	if descending := qp.Get("descending"); descending != "" {
		desc, err := strconv.ParseBool(descending)
		if err != nil {
			return nil, &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "descending is invalid",
			}
		}

		opts.Descending = desc
	}

	return opts, nil
}

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
