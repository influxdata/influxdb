package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-openapi/runtime/middleware"
	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/models"
	"golang.org/x/net/context"

	op "github.com/influxdata/mrfusion/restapi/operations"
)

type InfluxProxy struct {
	Srcs           mrfusion.SourcesStore
	ServersStore   mrfusion.ServersStore
	TimeSeries     mrfusion.TimeSeries
	KapacitorProxy mrfusion.Proxy
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

func (h *InfluxProxy) KapacitorProxyPost(ctx context.Context, params op.PostSourcesIDKapacitorsKapaIDProxyParams) middleware.Responder {
	id, err := strconv.Atoi(params.KapaID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.KapaID)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	srcID, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.KapaID)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyNotFound().WithPayload(errMsg)
	}

	if err = h.KapacitorProxy.Connect(ctx, &srv); err != nil {
		errMsg := &models.Error{Code: 400, Message: fmt.Sprintf("Unable to connect to servers store %s", params.KapaID)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyNotFound().WithPayload(errMsg)
	}

	body, err := json.Marshal(params.Query)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting to JSON %v", err)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	req := &mrfusion.Request{
		Method: "POST",
		Path:   params.Path,
		Body:   body,
	}

	resp, err := h.KapacitorProxy.Do(ctx, req)
	defer resp.Body.Close()
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error with proxy %v", err)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)

	}

	if resp.StatusCode == http.StatusNoContent {
		return op.NewPostSourcesIDKapacitorsKapaIDProxyNoContent()
	}

	dec := json.NewDecoder(resp.Body)
	var j interface{}
	err = dec.Decode(&j)
	if err != nil && err.Error() == "EOF" && resp.StatusCode != http.StatusOK {
		err = nil
	}

	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error reading body of response: %v", err)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	return op.NewPostSourcesIDKapacitorsKapaIDProxyDefault(resp.StatusCode).WithPayload(j)
}

func (h *InfluxProxy) KapacitorProxyPatch(ctx context.Context, params op.PatchSourcesIDKapacitorsKapaIDProxyParams) middleware.Responder {
	id, err := strconv.Atoi(params.KapaID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.KapaID)}
		return op.NewPatchSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	srcID, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.KapaID)}
		return op.NewPatchSourcesIDKapacitorsKapaIDProxyNotFound().WithPayload(errMsg)
	}

	if err = h.KapacitorProxy.Connect(ctx, &srv); err != nil {
		errMsg := &models.Error{Code: 400, Message: fmt.Sprintf("Unable to connect to servers store %s", params.KapaID)}
		return op.NewPatchSourcesIDKapacitorsKapaIDProxyNotFound().WithPayload(errMsg)
	}

	body, err := json.Marshal(params.Query)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting to JSON %v", err)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	req := &mrfusion.Request{
		Method: "PATCH",
		Path:   params.Path,
		Body:   body,
	}

	resp, err := h.KapacitorProxy.Do(ctx, req)
	defer resp.Body.Close()
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error with proxy %v", err)}
		return op.NewPatchSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	if resp.StatusCode == http.StatusNoContent {
		return op.NewPatchSourcesIDKapacitorsKapaIDProxyNoContent()
	}

	dec := json.NewDecoder(resp.Body)
	var j interface{}
	err = dec.Decode(&j)
	if err != nil && err.Error() == "EOF" && resp.StatusCode != http.StatusOK {
		err = nil
	}

	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error reading body of response: %v", err)}
		return op.NewPatchSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)

	}

	return op.NewPatchSourcesIDKapacitorsKapaIDProxyDefault(resp.StatusCode).WithPayload(j)
}

func (h *InfluxProxy) KapacitorProxyGet(ctx context.Context, params op.GetSourcesIDKapacitorsKapaIDProxyParams) middleware.Responder {
	id, err := strconv.Atoi(params.KapaID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.KapaID)}
		return op.NewGetSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	srcID, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.KapaID)}
		return op.NewGetSourcesIDKapacitorsKapaIDProxyNotFound().WithPayload(errMsg)
	}

	if err = h.KapacitorProxy.Connect(ctx, &srv); err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Unable to connect to servers store %s", params.KapaID)}
		return op.NewGetSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	req := &mrfusion.Request{
		Method: "GET",
		Path:   params.Path,
	}

	resp, err := h.KapacitorProxy.Do(ctx, req)
	defer resp.Body.Close()
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error with proxy %v", err)}
		return op.NewGetSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)

	}

	if resp.StatusCode == http.StatusNoContent {
		return op.NewGetSourcesIDKapacitorsKapaIDProxyNoContent()
	}

	dec := json.NewDecoder(resp.Body)
	var j interface{}
	err = dec.Decode(&j)
	if err != nil && err.Error() == "EOF" && resp.StatusCode != http.StatusOK {
		err = nil
	}

	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error reading body of response: %v", err)}
		return op.NewGetSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	return op.NewGetSourcesIDKapacitorsKapaIDProxyDefault(resp.StatusCode).WithPayload(j)
}

func (h *InfluxProxy) KapacitorProxyDelete(ctx context.Context, params op.DeleteSourcesIDKapacitorsKapaIDProxyParams) middleware.Responder {
	id, err := strconv.Atoi(params.KapaID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.KapaID)}
		return op.NewDeleteSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	srcID, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.KapaID)}
		return op.NewDeleteSourcesIDKapacitorsKapaIDProxyNotFound().WithPayload(errMsg)
	}

	if err = h.KapacitorProxy.Connect(ctx, &srv); err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Unable to connect to servers store %s", params.KapaID)}
		return op.NewDeleteSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	req := &mrfusion.Request{
		Method: "DELETE",
		Path:   params.Path,
	}

	resp, err := h.KapacitorProxy.Do(ctx, req)
	defer resp.Body.Close()
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error with proxy %v", err)}
		return op.NewDeleteSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)

	}

	if resp.StatusCode == http.StatusNoContent {
		return op.NewGetSourcesIDKapacitorsKapaIDProxyNoContent()
	}

	dec := json.NewDecoder(resp.Body)
	var j interface{}
	err = dec.Decode(&j)
	if err != nil && err.Error() == "EOF" && resp.StatusCode != http.StatusOK {
		err = nil
	}

	return op.NewDeleteSourcesIDKapacitorsKapaIDProxyDefault(resp.StatusCode).WithPayload(j)
}
