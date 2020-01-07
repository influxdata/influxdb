package endpoints

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
)

type mwTracing struct {
	next influxdb.NotificationEndpointService
}

var _ influxdb.NotificationEndpointService = (*mwTracing)(nil)

// MiddlewareTracing is a tracing service middleware for the notification endpoint service.
func MiddlewareTracing() ServiceMW {
	return func(svc influxdb.NotificationEndpointService) influxdb.NotificationEndpointService {
		return &mwTracing{next: svc}
	}
}

func (mw *mwTracing) Delete(ctx context.Context, id influxdb.ID) error {
	span, ctx := tracing.StartSpanFromContextWithOperationName(ctx, "delete")
	defer span.Finish()
	return mw.next.Delete(ctx, id)
}

func (mw *mwTracing) FindByID(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
	span, ctx := tracing.StartSpanFromContextWithOperationName(ctx, "find_by_id")
	defer span.Finish()
	return mw.next.FindByID(ctx, id)
}

func (mw *mwTracing) Find(ctx context.Context, f influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, error) {
	span, ctx := tracing.StartSpanFromContextWithOperationName(ctx, "find")
	defer span.Finish()
	return mw.next.Find(ctx, f, opt...)
}

func (mw *mwTracing) Create(ctx context.Context, userID influxdb.ID, endpoint influxdb.NotificationEndpoint) error {
	span, ctx := tracing.StartSpanFromContextWithOperationName(ctx, "create")
	defer span.Finish()
	return mw.next.Create(ctx, userID, endpoint)
}

func (mw *mwTracing) Update(ctx context.Context, update influxdb.EndpointUpdate) (influxdb.NotificationEndpoint, error) {
	opName := "update_" + update.UpdateType
	span, ctx := tracing.StartSpanFromContextWithOperationName(ctx, opName)
	defer span.Finish()
	return mw.next.Update(ctx, update)
}
