package mock

import (
	"fmt"
	"log"
	"strconv"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/models"
	op "github.com/influxdata/chronograf/restapi/operations"
	"golang.org/x/net/context"
)

type Handler struct {
	Store      chronograf.ExplorationStore
	Srcs       chronograf.SourcesStore
	TimeSeries chronograf.TimeSeries
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
		Sources:  "/chronograf/v1/sources",
		Layouts:  "/chronograf/v1/layouts",
		Users:    "/chronograf/v1/users",
		Mappings: "/chronograf/v1/mappings",
	}
	return op.NewGetOK().WithPayload(routes)
}

func (m *Handler) NewSource(ctx context.Context, params op.PostSourcesParams) middleware.Responder {
	src := chronograf.Source{
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

func mrToModel(src chronograf.Source) *models.Source {
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
	src := chronograf.Source{
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
	query := chronograf.Query{
		Command: *params.Query.Query,
		DB:      params.Query.Db,
		RP:      params.Query.Rp,
	}
	response, err := m.TimeSeries.Query(ctx, chronograf.Query(query))
	if err != nil {
		return op.NewPostSourcesIDProxyDefault(500)
	}

	res := &models.ProxyResponse{
		Results: response,
	}
	return op.NewPostSourcesIDProxyOK().WithPayload(res)
}

func (m *Handler) Explorations(ctx context.Context, params op.GetUsersUserIDExplorationsParams) middleware.Responder {
	id, err := strconv.Atoi(params.UserID)
	if err != nil {
		return op.NewGetUsersUserIDExplorationsDefault(500)
	}
	exs, err := m.Store.Query(ctx, chronograf.UserID(id))
	if err != nil {
		return op.NewGetUsersUserIDExplorationsNotFound()
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
	return op.NewGetUsersUserIDExplorationsOK().WithPayload(res)
}

func (m *Handler) Exploration(ctx context.Context, params op.GetUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
	id, err := strconv.Atoi(params.UserID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: "Error converting user id"}
		return op.NewGetUsersUserIDExplorationsDefault(500).WithPayload(errMsg)
	}

	eID, err := strconv.Atoi(params.ExplorationID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: "Error converting exploration id"}
		return op.NewGetUsersUserIDExplorationsExplorationIDDefault(500).WithPayload(errMsg)
	}

	e, err := m.Store.Get(ctx, chronograf.ExplorationID(eID))
	if err != nil {
		log.Printf("Error unknown exploration id: %d: %v", eID, err)
		errMsg := &models.Error{Code: 404, Message: "Error unknown exploration id"}
		return op.NewGetUsersUserIDExplorationsExplorationIDNotFound().WithPayload(errMsg)
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
	return op.NewGetUsersUserIDExplorationsExplorationIDOK().WithPayload(res)
}

func (m *Handler) UpdateExploration(ctx context.Context, params op.PatchUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
	eID, err := strconv.Atoi(params.ExplorationID)
	if err != nil {
		return op.NewPatchUsersUserIDExplorationsExplorationIDDefault(500)
	}

	e, err := m.Store.Get(ctx, chronograf.ExplorationID(eID))
	if err != nil {
		log.Printf("Error unknown exploration id: %d: %v", eID, err)
		errMsg := &models.Error{Code: 404, Message: "Error unknown exploration id"}
		return op.NewPatchUsersUserIDExplorationsExplorationIDNotFound().WithPayload(errMsg)
	}
	if params.Exploration != nil {
		e.ID = chronograf.ExplorationID(eID)
		e.Data = params.Exploration.Data.(string)
		e.Name = params.Exploration.Name
		m.Store.Update(ctx, e)
	}
	return op.NewPatchUsersUserIDExplorationsExplorationIDOK()
}

func (m *Handler) NewExploration(ctx context.Context, params op.PostUsersUserIDExplorationsParams) middleware.Responder {
	id, err := strconv.Atoi(params.UserID)
	if err != nil {
		return op.NewPostUsersUserIDExplorationsDefault(500)
	}

	exs, err := m.Store.Query(ctx, chronograf.UserID(id))
	if err != nil {
		log.Printf("Error unknown user id: %d: %v", id, err)
		errMsg := &models.Error{Code: 404, Message: "Error unknown user id"}
		return op.NewPostUsersUserIDExplorationsNotFound().WithPayload(errMsg)
	}
	eID := len(exs)

	if params.Exploration != nil {
		e := chronograf.Exploration{
			Data: params.Exploration.Data.(string),
			Name: params.Exploration.Name,
			ID:   chronograf.ExplorationID(eID),
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
	return op.NewPostUsersUserIDExplorationsCreated().WithPayload(params.Exploration).WithLocation(loc)

}

func (m *Handler) DeleteExploration(ctx context.Context, params op.DeleteUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
	ID, err := strconv.Atoi(params.ExplorationID)
	if err != nil {
		return op.NewDeleteUsersUserIDExplorationsExplorationIDDefault(500)
	}

	if err := m.Store.Delete(ctx, &chronograf.Exploration{ID: chronograf.ExplorationID(ID)}); err != nil {
		log.Printf("Error unknown explorations id: %d: %v", ID, err)
		errMsg := &models.Error{Code: 404, Message: "Error unknown user id"}
		return op.NewDeleteUsersUserIDExplorationsExplorationIDNotFound().WithPayload(errMsg)
	}
	return op.NewDeleteUsersUserIDExplorationsExplorationIDNoContent()
}

func (m *Handler) GetMappings(ctx context.Context, params op.GetMappingsParams) middleware.Responder {
	cpu := "cpu"
	system := "System"
	mp := &models.Mappings{
		Mappings: []*models.Mapping{
			&models.Mapping{
				Measurement: &cpu,
				Name:        &system,
			},
		},
	}
	return op.NewGetMappingsOK().WithPayload(mp)
}

func (m *Handler) Token(ctx context.Context, params op.GetTokenParams) middleware.Responder {

	token := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.MapClaims{
		"sub":      "bob",
		"exp":      time.Now().Add(time.Hour * 24 * 30).Unix(),
		"username": "bob",
		"email":    "bob@mail.com",
		"nbf":      time.Now().Unix(),
		"iat":      time.Now().Unix(),
	})

	// sign token with secret
	ts, err := token.SignedString([]byte("secret"))
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: "Failed to sign token"}
		return op.NewGetTokenDefault(500).WithPayload(errMsg)
	}

	t := models.Token(ts)

	return op.NewGetTokenOK().WithPayload(t)
}
