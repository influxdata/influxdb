package mock

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/models"
	op "github.com/influxdata/mrfusion/restapi/operations"
	"golang.org/x/net/context"
)

type Handler struct {
	Store      mrfusion.ExplorationStore
	Srcs       mrfusion.SourcesStore
	TimeSeries mrfusion.TimeSeries
}

func NewHandler() Handler {
	h := Handler{
		Store:      DefaultExplorationStore,
		Srcs:       DefaultSourcesStore,
		TimeSeries: DefaultTimeSeries,
	}
	return h
}

func (m *Handler) AllRoutes(ctx context.Context, params op.GetParams) middleware.Responder {
	routes := &models.Routes{
		Sources:    "/chronograf/v1/sources",
		Dashboards: "/chronograf/v1/dashboards",
		Apps:       "/chronograf/v1/apps",
		Users:      "/chronograf/v1/users",
	}
	return op.NewGetOK().WithPayload(routes)
}

func (m *Handler) NewSource(ctx context.Context, params op.PostSourcesParams) middleware.Responder {
	src := mrfusion.Source{
		Name:     *params.Source.Name,
		Type:     params.Source.Type,
		Username: params.Source.Username,
		Password: params.Source.Password,
		URL:      []string{*params.Source.URL},
		Default:  params.Source.Default,
	}
	var err error
	if src, err = m.Srcs.Add(ctx, src); err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error storing source %v: %v", params.Source, err)}
		return op.NewPostSourcesDefault(500).WithPayload(errMsg)
	}
	mSrc := mrToModel(src)
	return op.NewPostSourcesCreated().WithPayload(mSrc).WithLocation(mSrc.Links.Self)
}

func srcLinks(id int) *models.SourceLinks {
	return &models.SourceLinks{
		Self:        fmt.Sprintf("/chronograf/v1/sources/%d", id),
		Proxy:       fmt.Sprintf("/chronograf/v1/sources/%d/proxy", id),
		Users:       fmt.Sprintf("/chronograf/v1/sources/%d/users", id),
		Roles:       fmt.Sprintf("/chronograf/v1/sources/%d/roles", id),
		Permissions: fmt.Sprintf("/chronograf/v1/sources/%d/permissions", id),
	}
}

func mrToModel(src mrfusion.Source) *models.Source {
	return &models.Source{
		ID:       strconv.Itoa(src.ID),
		Links:    srcLinks(src.ID),
		Name:     &src.Name,
		Type:     src.Type,
		Username: src.Username,
		Password: src.Password,
		URL:      &src.URL[0],
		Default:  src.Default,
	}
}

func (m *Handler) Sources(ctx context.Context, params op.GetSourcesParams) middleware.Responder {
	mrSrcs, err := m.Srcs.All(ctx)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: "Error loading sources"}
		return op.NewGetSourcesDefault(500).WithPayload(errMsg)
	}

	srcs := make([]*models.Source, len(mrSrcs))
	for i, src := range mrSrcs {
		srcs[i] = mrToModel(src)
	}

	res := &models.Sources{
		Sources: srcs,
	}

	return op.NewGetSourcesOK().WithPayload(res)
}

func (m *Handler) SourcesID(ctx context.Context, params op.GetSourcesIDParams) middleware.Responder {
	id, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewGetSourcesIDDefault(500).WithPayload(errMsg)
	}

	src, err := m.Srcs.Get(ctx, id)
	if err != nil {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.ID)}
		return op.NewGetSourcesIDNotFound().WithPayload(errMsg)
	}

	return op.NewGetSourcesIDOK().WithPayload(mrToModel(src))
}

func (m *Handler) RemoveSource(ctx context.Context, params op.DeleteSourcesIDParams) middleware.Responder {
	id, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewDeleteSourcesIDDefault(500).WithPayload(errMsg)
	}
	src := mrfusion.Source{
		ID: id,
	}
	if err = m.Srcs.Delete(ctx, src); err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Unknown error deleting source %s", params.ID)}
		return op.NewDeleteSourcesIDDefault(500).WithPayload(errMsg)
	}

	return op.NewDeleteSourcesIDNoContent()
}

func (m *Handler) UpdateSource(ctx context.Context, params op.PatchSourcesIDParams) middleware.Responder {
	id, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewPatchSourcesIDDefault(500).WithPayload(errMsg)
	}
	src, err := m.Srcs.Get(ctx, id)
	if err != nil {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.ID)}
		return op.NewPatchSourcesIDNotFound().WithPayload(errMsg)
	}
	src.Default = params.Config.Default
	if params.Config.Name != nil {
		src.Name = *params.Config.Name
	}
	if params.Config.Password != "" {
		src.Password = params.Config.Password
	}
	if params.Config.Username != "" {
		src.Username = params.Config.Username
	}
	if params.Config.URL != nil {
		src.URL = []string{*params.Config.URL}
	}
	if params.Config.Type != "" {
		src.Type = params.Config.Type
	}
	if err := m.Srcs.Update(ctx, src); err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error updating source ID %s", params.ID)}
		return op.NewPatchSourcesIDDefault(500).WithPayload(errMsg)
	}
	return op.NewPatchSourcesIDNoContent()
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

func (m *Handler) Proxy(ctx context.Context, params op.PostSourcesIDProxyParams) middleware.Responder {
	id, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewPostSourcesIDProxyDefault(500).WithPayload(errMsg)
	}

	src, err := m.Srcs.Get(ctx, id)
	if err != nil {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.ID)}
		return op.NewPostSourcesIDProxyNotFound().WithPayload(errMsg)
	}

	if err = m.TimeSeries.Connect(ctx, &src); err != nil {
		errMsg := &models.Error{Code: 400, Message: fmt.Sprintf("Unable to connect to source %s", params.ID)}
		return op.NewPostSourcesIDProxyNotFound().WithPayload(errMsg)
	}
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

func (m *Handler) Explorations(ctx context.Context, params op.GetSourcesIDUsersUserIDExplorationsParams) middleware.Responder {
	id, err := strconv.Atoi(params.UserID)
	if err != nil {
		return op.NewGetSourcesIDUsersUserIDExplorationsDefault(500)
	}
	exs, err := m.Store.Query(ctx, mrfusion.UserID(id))
	if err != nil {
		return op.NewGetSourcesIDUsersUserIDExplorationsNotFound()
	}
	res := &models.Explorations{}
	for i, e := range exs {
		rel := "self"
		href := fmt.Sprintf("/chronograf/v1/sources/1/users/%d/explorations/%d", id, i)
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
	id, err := strconv.Atoi(params.UserID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: "Error converting user id"}
		return op.NewGetSourcesIDUsersUserIDExplorationsDefault(500).WithPayload(errMsg)
	}

	eID, err := strconv.Atoi(params.ExplorationID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: "Error converting exploration id"}
		return op.NewGetSourcesIDUsersUserIDExplorationsExplorationIDDefault(500).WithPayload(errMsg)
	}

	e, err := m.Store.Get(ctx, mrfusion.ExplorationID(eID))
	if err != nil {
		log.Printf("Error unknown exploration id: %d: %v", eID, err)
		errMsg := &models.Error{Code: 404, Message: "Error unknown exploration id"}
		return op.NewGetSourcesIDUsersUserIDExplorationsExplorationIDNotFound().WithPayload(errMsg)
	}

	rel := "self"
	href := fmt.Sprintf("/chronograf/v1/sources/1/users/%d/explorations/%d", id, eID)
	res := &models.Exploration{
		Data:      e.Data,
		Name:      e.Name,
		UpdatedAt: strfmt.DateTime(e.UpdatedAt),
		CreatedAt: strfmt.DateTime(e.CreatedAt),
		Link: &models.Link{
			Rel:  &rel,
			Href: &href,
		},
	}
	return op.NewGetSourcesIDUsersUserIDExplorationsExplorationIDOK().WithPayload(res)
}

func (m *Handler) UpdateExploration(ctx context.Context, params op.PatchSourcesIDUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
	eID, err := strconv.Atoi(params.ExplorationID)
	if err != nil {
		return op.NewPatchSourcesIDUsersUserIDExplorationsExplorationIDDefault(500)
	}

	e, err := m.Store.Get(ctx, mrfusion.ExplorationID(eID))
	if err != nil {
		log.Printf("Error unknown exploration id: %d: %v", eID, err)
		errMsg := &models.Error{Code: 404, Message: "Error unknown exploration id"}
		return op.NewPatchSourcesIDUsersUserIDExplorationsExplorationIDNotFound().WithPayload(errMsg)
	}
	if params.Exploration != nil {
		e.ID = mrfusion.ExplorationID(eID)
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

	exs, err := m.Store.Query(ctx, mrfusion.UserID(id))
	if err != nil {
		log.Printf("Error unknown user id: %d: %v", id, err)
		errMsg := &models.Error{Code: 404, Message: "Error unknown user id"}
		return op.NewPostSourcesIDUsersUserIDExplorationsNotFound().WithPayload(errMsg)
	}
	eID := len(exs)

	if params.Exploration != nil {
		e := mrfusion.Exploration{
			Data: params.Exploration.Data.(string),
			Name: params.Exploration.Name,
			ID:   mrfusion.ExplorationID(eID),
		}
		m.Store.Add(ctx, &e)
	}
	params.Exploration.UpdatedAt = strfmt.DateTime(time.Now())
	params.Exploration.CreatedAt = strfmt.DateTime(time.Now())

	loc := fmt.Sprintf("/chronograf/v1/sources/1/users/%d/explorations/%d", id, eID)
	rel := "self"

	link := &models.Link{
		Href: &loc,
		Rel:  &rel,
	}
	params.Exploration.Link = link
	return op.NewPostSourcesIDUsersUserIDExplorationsCreated().WithPayload(params.Exploration).WithLocation(loc)

}

func (m *Handler) DeleteExploration(ctx context.Context, params op.DeleteSourcesIDUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
	ID, err := strconv.Atoi(params.ExplorationID)
	if err != nil {
		return op.NewDeleteSourcesIDUsersUserIDExplorationsExplorationIDDefault(500)
	}

	if err := m.Store.Delete(ctx, &mrfusion.Exploration{ID: mrfusion.ExplorationID(ID)}); err != nil {
		log.Printf("Error unknown explorations id: %d: %v", ID, err)
		errMsg := &models.Error{Code: 404, Message: "Error unknown user id"}
		return op.NewDeleteSourcesIDUsersUserIDExplorationsExplorationIDNotFound().WithPayload(errMsg)
	}
	return op.NewDeleteSourcesIDUsersUserIDExplorationsExplorationIDNoContent()
}
