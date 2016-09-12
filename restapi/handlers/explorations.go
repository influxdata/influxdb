package handlers

import (
	"strconv"

	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/mock"
	"github.com/influxdata/mrfusion/models"
	op "github.com/influxdata/mrfusion/restapi/operations"
	"golang.org/x/net/context"
)

type MockHandler struct {
	Store    mrfusion.ExplorationStore
	Response mrfusion.Response
}

func NewMockHandler() MockHandler {
	return MockHandler{
		mock.DefaultExplorationStore,
		mock.SampleResponse,
	}
}

func (m *MockHandler) Explorations(ctx context.Context, params op.GetSourcesIDUsersUserIDExplorationsParams) middleware.Responder {
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
		res.Explorations = append(res.Explorations, &models.Exploration{
			Data:      e.Data,
			Name:      e.Name,
			UpdatedAt: strfmt.DateTime(e.UpdatedAt),
			CreatedAt: strfmt.DateTime(e.CreatedAt),
		},
		)
	}
	return op.NewGetSourcesIDUsersUserIDExplorationsOK().WithPayload(res)
}

func (m *MockHandler) Exploration(ctx context.Context, params op.GetSourcesIDUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
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

func (m *MockHandler) UpdateExploration(ctx context.Context, params op.PatchSourcesIDUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
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

func (m *MockHandler) NewExploration(ctx context.Context, params op.PostSourcesIDUsersUserIDExplorationsParams) middleware.Responder {
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

func (m *MockHandler) DeleteExploration(ctx context.Context, params op.DeleteSourcesIDUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
	ID, err := strconv.Atoi(params.ExplorationID)
	if err != nil {
		return op.NewDeleteSourcesIDUsersUserIDExplorationsExplorationIDDefault(500)
	}

	if err := m.Store.Delete(ctx, mrfusion.Exploration{ID: ID}); err != nil {
		return op.NewDeleteSourcesIDUsersUserIDExplorationsExplorationIDNotFound()
	}
	return op.NewDeleteSourcesIDUsersUserIDExplorationsExplorationIDNoContent()
}
