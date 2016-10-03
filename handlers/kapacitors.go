package handlers

import (
	"fmt"
	"strconv"

	"github.com/go-openapi/runtime/middleware"
	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/models"

	op "github.com/influxdata/mrfusion/restapi/operations"
	"golang.org/x/net/context"
)

func (h *Store) NewKapacitor(ctx context.Context, params op.PostSourcesIDKapacitorsParams) middleware.Responder {
	srv := mrfusion.Server{
		Name:     *params.Kapacitor.Name,
		Username: params.Kapacitor.Username,
		Password: params.Kapacitor.Password,
		URL:      *params.Kapacitor.URL,
	}
	var err error
	if srv, err = h.ServersStore.Add(ctx, srv); err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error storing kapacitor %v: %v", params.Kapacitor, err)}
		return op.NewPostSourcesIDKapacitorsDefault(500).WithPayload(errMsg)
	}
	mSrv := srvToModel(srv)
	return op.NewPostSourcesIDKapacitorsCreated().WithPayload(mSrv).WithLocation(mSrv.Links.Self)
}

func srvLinks(id int) *models.KapacitorLinks {
	return &models.KapacitorLinks{
		Self:  fmt.Sprintf("/chronograf/v1/kapacitors/%d", id),
		Proxy: fmt.Sprintf("/chronograf/v1/kapacitors/%d/proxy", id),
	}
}

func srvToModel(srv mrfusion.Server) *models.Kapacitor {
	return &models.Kapacitor{
		ID:       strconv.Itoa(srv.ID),
		Links:    srvLinks(srv.ID),
		Name:     &srv.Name,
		Username: srv.Username,
		Password: srv.Password,
		URL:      &srv.URL,
	}
}

func (h *Store) Kapacitors(ctx context.Context, params op.GetSourcesIDKapacitorsParams) middleware.Responder {
	mrSrvs, err := h.ServersStore.All(ctx)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: "Error loading kapacitors"}
		return op.NewGetSourcesIDKapacitorsDefault(500).WithPayload(errMsg)
	}

	srvs := make([]*models.Kapacitor, len(mrSrvs))
	for i, srv := range mrSrvs {
		srvs[i] = srvToModel(srv)
	}

	res := &models.Kapacitors{
		Kapacitors: srvs,
	}

	return op.NewGetSourcesIDKapacitorsOK().WithPayload(res)
}

func (h *Store) KapacitorsID(ctx context.Context, params op.GetSourcesIDKapacitorsKapaIDParams) middleware.Responder {
	id, err := strconv.Atoi(params.KapaID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.KapaID)}
		return op.NewGetSourcesIDKapacitorsKapaIDDefault(500).WithPayload(errMsg)
	}

	srcID, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.KapaID)}
		return op.NewGetSourcesIDKapacitorsKapaIDNotFound().WithPayload(errMsg)
	}

	return op.NewGetSourcesIDKapacitorsKapaIDOK().WithPayload(srvToModel(srv))
}

func (h *Store) RemoveKapacitor(ctx context.Context, params op.DeleteSourcesIDKapacitorsKapaIDParams) middleware.Responder {
	id, err := strconv.Atoi(params.KapaID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.KapaID)}
		return op.NewDeleteSourcesIDKapacitorsKapaIDDefault(500).WithPayload(errMsg)
	}

	srcID, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.KapaID)}
		return op.NewDeleteSourcesIDKapacitorsKapaIDNotFound().WithPayload(errMsg)
	}

	if err = h.ServersStore.Delete(ctx, srv); err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Unknown error deleting kapacitor %s", params.KapaID)}
		return op.NewDeleteSourcesIDKapacitorsKapaIDDefault(500).WithPayload(errMsg)
	}

	return op.NewDeleteSourcesIDKapacitorsKapaIDNoContent()
}

func (h *Store) UpdateKapacitor(ctx context.Context, params op.PatchSourcesIDKapacitorsKapaIDParams) middleware.Responder {
	id, err := strconv.Atoi(params.KapaID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.KapaID)}
		return op.NewPatchSourcesIDKapacitorsKapaIDDefault(500).WithPayload(errMsg)
	}

	srcID, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewPostSourcesIDKapacitorsKapaIDProxyDefault(500).WithPayload(errMsg)
	}

	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.KapaID)}
		return op.NewPatchSourcesIDKapacitorsKapaIDNotFound().WithPayload(errMsg)
	}
	if params.Config.Name != nil {
		srv.Name = *params.Config.Name
	}
	if params.Config.Password != "" {
		srv.Password = params.Config.Password
	}
	if params.Config.Username != "" {
		srv.Username = params.Config.Username
	}
	if params.Config.URL != nil {
		srv.URL = *params.Config.URL
	}
	if err := h.ServersStore.Update(ctx, srv); err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error updating kapacitor ID %s", params.KapaID)}
		return op.NewPatchSourcesIDKapacitorsKapaIDDefault(500).WithPayload(errMsg)
	}
	return op.NewPatchSourcesIDKapacitorsKapaIDNoContent()
}
