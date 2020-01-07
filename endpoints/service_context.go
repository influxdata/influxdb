package endpoints

import (
	"context"

	"github.com/influxdata/influxdb"
)

type ctxKey uint8

const endpointKey ctxKey = 1

func setEndpoint(ctx context.Context, endpoint influxdb.NotificationEndpoint) context.Context {
	return context.WithValue(ctx, endpointKey, endpoint)
}

func getEndpoint(ctx context.Context) influxdb.NotificationEndpoint {
	edp, ok := ctx.Value(endpointKey).(influxdb.NotificationEndpoint)
	if !ok {
		return nil
	}
	return edp
}
