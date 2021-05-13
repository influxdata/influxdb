package transport

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/notebooks/service"
)

type contextKey string

const (
	contextKeyOrgID           = contextKey("orgID")
	contextKeyNotebookID      = contextKey("notebookID")
	contextKeyNotebookReqBody = contextKey("notebookReqBody")
)

func orgIDFromContext(ctx context.Context) platform.ID {
	return ctx.Value(contextKeyOrgID).(platform.ID)
}

func notebookIDFromContext(ctx context.Context) platform.ID {
	return ctx.Value(contextKeyNotebookID).(platform.ID)
}

func notebookReqBodyFromContext(ctx context.Context) *service.NotebookReqBody {
	return ctx.Value(contextKeyNotebookReqBody).(*service.NotebookReqBody)
}

func withOrgID(ctx context.Context, id platform.ID) context.Context {
	return context.WithValue(ctx, contextKeyOrgID, id)
}

func withNotebookID(ctx context.Context, id platform.ID) context.Context {
	return context.WithValue(ctx, contextKeyNotebookID, id)
}

func withNotebookReqBody(ctx context.Context, b *service.NotebookReqBody) context.Context {
	return context.WithValue(ctx, contextKeyNotebookID, b)
}
