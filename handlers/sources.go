package handlers

import (
	"fmt"
	"strconv"

	"github.com/go-openapi/runtime/middleware"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/models"

	op "github.com/influxdata/chronograf/restapi/operations"
	"golang.org/x/net/context"
)

func (h *Store) NewSource(ctx context.Context, params op.PostSourcesParams) middleware.Responder {
	src := chronograf.Source{
		Name:     *params.Source.Name,
		Type:     params.Source.Type,
		Username: params.Source.Username,
		Password: params.Source.Password,
		URL:      []string{*params.Source.URL},
		Default:  params.Source.Default,
	}
	var err error
	if src, err = h.SourcesStore.Add(ctx, src); err != nil {
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
		Kapacitors:  fmt.Sprintf("/chronograf/v1/sources/%d/kapacitors", id),
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

func (h *Store) Sources(ctx context.Context, params op.GetSourcesParams) middleware.Responder {
	mrSrcs, err := h.SourcesStore.All(ctx)
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

func (h *Store) SourcesID(ctx context.Context, params op.GetSourcesIDParams) middleware.Responder {
	id, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewGetSourcesIDDefault(500).WithPayload(errMsg)
	}

	src, err := h.SourcesStore.Get(ctx, id)
	if err != nil {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.ID)}
		return op.NewGetSourcesIDNotFound().WithPayload(errMsg)
	}

	return op.NewGetSourcesIDOK().WithPayload(mrToModel(src))
}

func (h *Store) RemoveSource(ctx context.Context, params op.DeleteSourcesIDParams) middleware.Responder {
	id, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewDeleteSourcesIDDefault(500).WithPayload(errMsg)
	}
	src := chronograf.Source{
		ID: id,
	}
	if err = h.SourcesStore.Delete(ctx, src); err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Unknown error deleting source %s", params.ID)}
		return op.NewDeleteSourcesIDDefault(500).WithPayload(errMsg)
	}

	return op.NewDeleteSourcesIDNoContent()
}

func (h *Store) UpdateSource(ctx context.Context, params op.PatchSourcesIDParams) middleware.Responder {
	id, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewPatchSourcesIDDefault(500).WithPayload(errMsg)
	}
	src, err := h.SourcesStore.Get(ctx, id)
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
		// TODO: Change to bolt when finished
		src.Username = params.Config.Username
	}
	if params.Config.URL != nil {
		src.URL = []string{*params.Config.URL}
	}
	if params.Config.Type != "" {
		src.Type = params.Config.Type
	}
	if err := h.SourcesStore.Update(ctx, src); err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error updating source ID %s", params.ID)}
		return op.NewPatchSourcesIDDefault(500).WithPayload(errMsg)
	}
	return op.NewPatchSourcesIDNoContent()
}
