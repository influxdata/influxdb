package mock

import (
	"strconv"

	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/models"
	op "github.com/influxdata/mrfusion/restapi/operations"
	"golang.org/x/net/context"
)

type Handler struct {
	Store      mrfusion.ExplorationStore
	TimeSeries mrfusion.TimeSeries
}

func NewHandler() Handler {
	return Handler{
		DefaultExplorationStore,
		DefaultTimeSeries,
	}
}

func sampleSource() *models.Source {
	name := "muh name"
	influxType := "influx-enterprise"

	return &models.Source{
		ID: "1",
		Links: &models.SourceLinks{
			Self:  "/chronograf/v1/sources/1",
			Proxy: "/chronograf/v1/sources/1/proxy",
		},
		Name: &name,
		Type: &influxType,
	}

}

func (m *Handler) Sources(ctx context.Context, params op.GetSourcesParams) middleware.Responder {
	res := &models.Sources{
		Sources: []*models.Source{
			sampleSource(),
		},
	}

	return op.NewGetSourcesOK().WithPayload(res)
}

func (m *Handler) SourcesID(ctx context.Context, params op.GetSourcesIDParams) middleware.Responder {
	if params.ID != "1" {
		return op.NewGetSourcesIDNotFound()
	}
	return op.NewGetSourcesIDOK().WithPayload(sampleSource())
}

func (m *Handler) Proxy(ctx context.Context, params op.PostSourcesIDProxyParams) middleware.Responder {
	query := mrfusion.Query{
		Command: *params.Query.Query,
		DB:      params.Query.Db,
		RP:      params.Query.Rp,
	}
	response, err := m.TimeSeries.Query(ctx, mrfusion.Query(query))
	if err != nil {
		return op.NewPostSourcesIDProxyDefault(500)
	}

	res := &models.ProxyResponse{
		Results: response,
	}
	return op.NewPostSourcesIDProxyOK().WithPayload(res)
}

func (m *Handler) MonitoredServices(ctx context.Context, params op.GetSourcesIDMonitoredParams) middleware.Responder {
	srvs, err := m.TimeSeries.MonitoredServices(ctx)
	if err != nil {
		return op.NewGetSourcesIDMonitoredDefault(500)
	}
	res := &models.Services{}
	for _, s := range srvs {
		res.Services = append(res.Services, &models.Service{
			TagKey:   s.TagKey,
			TagValue: s.TagValue,
			Type:     s.Type,
		})
	}
	return op.NewGetSourcesIDMonitoredOK().WithPayload(res)
}

func (m *Handler) Explorations(ctx context.Context, params op.GetSourcesIDUsersUserIDExplorationsParams) middleware.Responder {
	id, err := strconv.Atoi(params.UserID)
	if err != nil {
		return op.NewGetSourcesIDUsersUserIDExplorationsDefault(500)
	}
	exs, err := m.Store.Query(ctx, id)
	if err != nil {
		return op.NewGetSourcesIDUsersUserIDExplorationsNotFound()
	}
	res := &models.Explorations{}
	for _, e := range exs {
		rel := "self"
		href := "/chronograf/v1/source/1/users/1/explorations/1"
		res.Explorations = append(res.Explorations, &models.Exploration{
			Data:      e.Data,
			Name:      e.Name,
			UpdatedAt: strfmt.DateTime(e.UpdatedAt),
			CreatedAt: strfmt.DateTime(e.CreatedAt),
			Link: &models.Link{
				Rel:  &rel,
				Href: &href,
			},
		},
		)
	}
	return op.NewGetSourcesIDUsersUserIDExplorationsOK().WithPayload(res)
}

func (m *Handler) Exploration(ctx context.Context, params op.GetSourcesIDUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
	eID, err := strconv.Atoi(params.ExplorationID)
	if err != nil {
		return op.NewGetSourcesIDUsersUserIDExplorationsExplorationIDDefault(500)
	}

	e, err := m.Store.Get(ctx, eID)
	if err != nil {
		return op.NewGetSourcesIDUsersUserIDExplorationsExplorationIDNotFound()
	}
	res := &models.Exploration{
		Data:      e.Data,
		Name:      e.Name,
		UpdatedAt: strfmt.DateTime(e.UpdatedAt),
		CreatedAt: strfmt.DateTime(e.CreatedAt),
	}
	return op.NewGetSourcesIDUsersUserIDExplorationsExplorationIDOK().WithPayload(res)
}

func (m *Handler) UpdateExploration(ctx context.Context, params op.PatchSourcesIDUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
	eID, err := strconv.Atoi(params.ExplorationID)
	if err != nil {
		return op.NewPatchSourcesIDUsersUserIDExplorationsExplorationIDDefault(500)
	}

	e, err := m.Store.Get(ctx, eID)
	if err != nil {
		return op.NewPatchSourcesIDUsersUserIDExplorationsExplorationIDNotFound()
	}
	if params.Exploration != nil {
		e.Data = params.Exploration.Data.(string)
		e.Name = params.Exploration.Name
		m.Store.Update(ctx, e)
	}
	return op.NewPatchSourcesIDUsersUserIDExplorationsExplorationIDNoContent()
}

func (m *Handler) NewExploration(ctx context.Context, params op.PostSourcesIDUsersUserIDExplorationsParams) middleware.Responder {
	id, err := strconv.Atoi(params.UserID)
	if err != nil {
		return op.NewPostSourcesIDUsersUserIDExplorationsDefault(500)
	}

	exs, err := m.Store.Query(ctx, id)
	if err != nil {
		return op.NewPostSourcesIDUsersUserIDExplorationsNotFound()
	}
	eID := len(exs)

	if params.Exploration != nil {
		e := mrfusion.Exploration{
			Data: params.Exploration.Data.(string),
			Name: params.Exploration.Name,
			ID:   eID,
		}
		m.Store.Add(ctx, e)
	}
	return op.NewPostSourcesIDUsersUserIDExplorationsCreated()
}

func (m *Handler) DeleteExploration(ctx context.Context, params op.DeleteSourcesIDUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
	ID, err := strconv.Atoi(params.ExplorationID)
	if err != nil {
		return op.NewDeleteSourcesIDUsersUserIDExplorationsExplorationIDDefault(500)
	}

	if err := m.Store.Delete(ctx, mrfusion.Exploration{ID: ID}); err != nil {
		return op.NewDeleteSourcesIDUsersUserIDExplorationsExplorationIDNotFound()
	}
	return op.NewDeleteSourcesIDUsersUserIDExplorationsExplorationIDNoContent()
}
