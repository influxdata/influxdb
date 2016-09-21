package handlers

import (
	"github.com/go-openapi/runtime/middleware"
	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/models"
	"golang.org/x/net/context"

	op "github.com/influxdata/mrfusion/restapi/operations"
)

type InfluxProxy struct {
	TimeSeries mrfusion.TimeSeries
}

func (h *InfluxProxy) Proxy(ctx context.Context, params op.PostSourcesIDProxyParams) middleware.Responder {
	// TODO: Add support for multiple TimeSeries with lookup based on params.ID
	query := mrfusion.Query{
		Command: *params.Query.Query,
		DB:      params.Query.DB,
		RP:      params.Query.Rp,
	}

	response, err := h.TimeSeries.Query(ctx, query)
	if err != nil {
		if err == mrfusion.ErrUpstreamTimeout {
			e := &models.Error{
				Code:    408,
				Message: "Timeout waiting for Influx response",
			}

			return op.NewPostSourcesIDProxyRequestTimeout().WithPayload(e)
		}
		// TODO: Here I want to return the error code from influx.
		e := &models.Error{
			Code:    400,
			Message: err.Error(),
		}

		return op.NewPostSourcesIDProxyBadRequest().WithPayload(e)
	}

	res := &models.ProxyResponse{
		Results: response,
	}
	return op.NewPostSourcesIDProxyOK().WithPayload(res)
}
