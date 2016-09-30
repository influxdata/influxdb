package handlers

import (
	"fmt"
	"strconv"

	"github.com/go-openapi/runtime/middleware"
	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/models"
	"golang.org/x/net/context"

	op "github.com/influxdata/mrfusion/restapi/operations"
)

type InfluxProxy struct {
	Srcs       mrfusion.SourcesStore
	TimeSeries mrfusion.TimeSeries
}

func (h *InfluxProxy) Proxy(ctx context.Context, params op.PostSourcesIDProxyParams) middleware.Responder {
	id, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewPostSourcesIDProxyDefault(500).WithPayload(errMsg)
	}

	src, err := h.Srcs.Get(ctx, id)
	if err != nil {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.ID)}
		return op.NewPostSourcesIDProxyNotFound().WithPayload(errMsg)
	}

	if err = h.TimeSeries.Connect(ctx, &src); err != nil {
		errMsg := &models.Error{Code: 400, Message: fmt.Sprintf("Unable to connect to source %s", params.ID)}
		return op.NewPostSourcesIDProxyNotFound().WithPayload(errMsg)
	}

	query := mrfusion.Query{
		Command: *params.Query.Query,
		DB:      params.Query.Db,
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
