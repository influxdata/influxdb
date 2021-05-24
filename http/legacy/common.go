package legacy

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
)

// getAuthorization extracts authorization information from a context.Context.
// It guards against non influxdb.Authorization values for authorization and
// InfluxQL feature flag not enabled.
func getAuthorization(ctx context.Context) (*influxdb.Authorization, error) {
	authorizer, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		return nil, err
	}

	a, ok := authorizer.(*influxdb.Authorization)
	if !ok {
		return nil, &errors.Error{
			Code: errors.EForbidden,
			Msg:  "insufficient permissions; session not supported",
		}
	}
	return a, nil
}
