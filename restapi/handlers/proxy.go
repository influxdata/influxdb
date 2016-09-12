package handlers

import (
	"github.com/go-openapi/runtime/middleware"
	op "github.com/influxdata/mrfusion/restapi/operations"
	"golang.org/x/net/context"
)

func MockProxy(ctx context.Context, params op.PostSourcesIDProxyParams) middleware.Responder {
	return nil
}
