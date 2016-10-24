package handlers

import (
	"fmt"
	"log"
	"strconv"

	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/models"
	"golang.org/x/net/context"

	op "github.com/influxdata/chronograf/restapi/operations"
)

func (h *Store) Explorations(ctx context.Context, params op.GetUsersUserIDExplorationsParams) middleware.Responder {
	uID, err := strconv.Atoi(params.UserID)
	if err != nil {
		log.Printf("Error: Unable to convert UserID: %s: %v", params.UserID, err)
		errMsg := &models.Error{Code: 500, Message: "Error: Unable to convert UserID"}
		return op.NewGetUsersUserIDExplorationsDefault(500).WithPayload(errMsg)
	}

	mrExs, err := h.ExplorationStore.Query(ctx, chronograf.UserID(uID))
	if err != nil {
		log.Printf("Error: Unknown response from store while querying UserID: %s: %v", params.UserID, err)
		errMsg := &models.Error{Code: 500, Message: "Error: Unknown response from store while querying UserID"}
		return op.NewGetUsersUserIDExplorationsDefault(500).WithPayload(errMsg)
	}

	exs := make([]*models.Exploration, len(mrExs))
	for i, e := range mrExs {
		rel := "self"
		href := fmt.Sprintf("/chronograf/v1/sources/1/users/%d/explorations/%d", uID, e.ID)
		exs[i] = &models.Exploration{
			Data:      e.Data,
			Name:      e.Name,
			UpdatedAt: strfmt.DateTime(e.UpdatedAt),
			CreatedAt: strfmt.DateTime(e.CreatedAt),
			Link: &models.Link{
				Rel:  &rel,
				Href: &href,
			},
		}
	}
	res := &models.Explorations{
		Explorations: exs,
	}
	return op.NewGetUsersUserIDExplorationsOK().WithPayload(res)
}

func (h *Store) Exploration(ctx context.Context, params op.GetUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
	eID, err := strconv.Atoi(params.ExplorationID)
	if err != nil {
		log.Printf("Error: Unable to convert ExplorationID: %s: %v", params.ExplorationID, err)
		errMsg := &models.Error{Code: 500, Message: "Error: Unable to convert ExplorationID"}
		return op.NewGetUsersUserIDExplorationsExplorationIDDefault(500).WithPayload(errMsg)
	}

	uID, err := strconv.Atoi(params.UserID)
	if err != nil {
		log.Printf("Error: Unable to convert UserID: %s: %v", params.UserID, err)
		errMsg := &models.Error{Code: 500, Message: "Error: Unable to convert UserID"}
		return op.NewGetUsersUserIDExplorationsExplorationIDDefault(500).WithPayload(errMsg)
	}

	e, err := h.ExplorationStore.Get(ctx, chronograf.ExplorationID(eID))
	if err != nil {
		log.Printf("Error: Unknown ExplorationID: %s: %v", params.ExplorationID, err)
		errMsg := &models.Error{Code: 404, Message: "Error: Unknown ExplorationID"}
		return op.NewGetUsersUserIDExplorationsExplorationIDNotFound().WithPayload(errMsg)
	}

	if e.UserID != chronograf.UserID(uID) {
		log.Printf("Error: Unknown ExplorationID: %s: %v", params.ExplorationID, err)
		errMsg := &models.Error{Code: 404, Message: "Error: Unknown ExplorationID"}
		return op.NewGetUsersUserIDExplorationsExplorationIDNotFound().WithPayload(errMsg)
	}

	rel := "self"
	href := fmt.Sprintf("/chronograf/v1/sources/1/users/%d/explorations/%d", uID, eID)
	res := &models.Exploration{
		Name:      e.Name,
		Data:      e.Data,
		UpdatedAt: strfmt.DateTime(e.UpdatedAt),
		CreatedAt: strfmt.DateTime(e.CreatedAt),
		Link: &models.Link{
			Rel:  &rel,
			Href: &href,
		},
	}
	return op.NewGetUsersUserIDExplorationsExplorationIDOK().WithPayload(res)
}

func (h *Store) UpdateExploration(ctx context.Context, params op.PatchUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
	if params.Exploration == nil {
		log.Printf("Error: Exploration is nil")
		errMsg := &models.Error{Code: 400, Message: "Error: Exploration is nil"}
		return op.NewPatchUsersUserIDExplorationsExplorationIDDefault(400).WithPayload(errMsg)
	}

	eID, err := strconv.Atoi(params.ExplorationID)
	if err != nil {
		log.Printf("Error: Unable to convert ExplorationID: %s: %v", params.ExplorationID, err)
		errMsg := &models.Error{Code: 500, Message: "Error: Unable to convert ExplorationID"}
		return op.NewPatchUsersUserIDExplorationsExplorationIDDefault(500).WithPayload(errMsg)
	}

	uID, err := strconv.Atoi(params.UserID)
	if err != nil {
		log.Printf("Error: Unable to convert UserID: %s: %v", params.UserID, err)
		errMsg := &models.Error{Code: 500, Message: "Error: Unable to convert UserID"}
		return op.NewPatchUsersUserIDExplorationsExplorationIDDefault(500).WithPayload(errMsg)
	}

	e, err := h.ExplorationStore.Get(ctx, chronograf.ExplorationID(eID))
	if err != nil || e.UserID != chronograf.UserID(uID) {
		log.Printf("Error: Unknown ExplorationID: %s: %v", params.ExplorationID, err)
		errMsg := &models.Error{Code: 404, Message: "Error: Unknown ExplorationID"}
		return op.NewPatchUsersUserIDExplorationsExplorationIDNotFound().WithPayload(errMsg)
	}
	if params.Exploration.Data != nil {
		var ok bool
		if e.Data, ok = params.Exploration.Data.(string); !ok {
			log.Printf("Error: Exploration data is not a string")
			errMsg := &models.Error{Code: 400, Message: "Error: Exploration data is not a string"}
			return op.NewPatchUsersUserIDExplorationsExplorationIDDefault(400).WithPayload(errMsg)
		}
	}

	if params.Exploration.Name != "" {
		e.Name = params.Exploration.Name
	}

	if err := h.ExplorationStore.Update(ctx, e); err != nil {
		log.Printf("Error: Failed to update Exploration: %v: %v", e, err)
		errMsg := &models.Error{Code: 500, Message: "Error: Failed to update Exploration"}
		return op.NewPatchUsersUserIDExplorationsExplorationIDDefault(500).WithPayload(errMsg)
	}

	return op.NewPatchUsersUserIDExplorationsExplorationIDOK().WithPayload(explToModel(e))
}

func explToModel(e *chronograf.Exploration) *models.Exploration {
	rel := "self"
	href := fmt.Sprintf("/chronograf/v1/sources/1/users/%d/explorations/%d", e.UserID, e.ID)
	return &models.Exploration{
		Name:      e.Name,
		Data:      e.Data,
		UpdatedAt: strfmt.DateTime(e.UpdatedAt),
		CreatedAt: strfmt.DateTime(e.CreatedAt),
		Link: &models.Link{
			Rel:  &rel,
			Href: &href,
		},
	}
}

func (h *Store) NewExploration(ctx context.Context, params op.PostUsersUserIDExplorationsParams) middleware.Responder {
	if params.Exploration == nil {
		log.Printf("Error: Exploration is nil")
		errMsg := &models.Error{Code: 400, Message: "Error: Exploration is nil"}
		return op.NewPostUsersUserIDExplorationsDefault(400).WithPayload(errMsg)
	}

	uID, err := strconv.Atoi(params.UserID)
	if err != nil {
		log.Printf("Error: Unable to convert UserID: %s: %v", params.UserID, err)
		errMsg := &models.Error{Code: 500, Message: "Error: Unable to convert UserID"}
		return op.NewPostUsersUserIDExplorationsDefault(500).WithPayload(errMsg)
	}

	// TODO: Check user if user exists.

	data := ""
	if params.Exploration.Data != nil {
		data, _ = params.Exploration.Data.(string)
	}
	e := &chronograf.Exploration{
		Name:   params.Exploration.Name,
		UserID: chronograf.UserID(uID),
		Data:   data,
	}

	e, err = h.ExplorationStore.Add(ctx, e)
	if err != nil {
		log.Printf("Error: Failed to save Exploration: %v: %v", e, err)
		errMsg := &models.Error{Code: 500, Message: "Error: Failed to save Exploration"}
		return op.NewPostUsersUserIDExplorationsDefault(500).WithPayload(errMsg)
	}

	m := explToModel(e)
	res := op.NewPostUsersUserIDExplorationsCreated()
	return res.WithLocation(*m.Link.Href).WithPayload(m)
}

func (h *Store) DeleteExploration(ctx context.Context, params op.DeleteUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
	eID, err := strconv.Atoi(params.ExplorationID)
	if err != nil {
		log.Printf("Error: Unable to convert ExplorationID: %s: %v", params.ExplorationID, err)
		errMsg := &models.Error{Code: 500, Message: "Error: Unable to convert ExplorationID"}
		return op.NewDeleteUsersUserIDExplorationsExplorationIDDefault(500).WithPayload(errMsg)
	}

	uID, err := strconv.Atoi(params.UserID)
	if err != nil {
		log.Printf("Error: Unable to convert UserID: %s: %v", params.UserID, err)
		errMsg := &models.Error{Code: 500, Message: "Error: Unable to convert UserID"}
		return op.NewDeleteUsersUserIDExplorationsExplorationIDDefault(500).WithPayload(errMsg)
	}

	e, err := h.ExplorationStore.Get(ctx, chronograf.ExplorationID(eID))
	if err != nil || e.UserID != chronograf.UserID(uID) {
		log.Printf("Error: Unknown ExplorationID: %s: %v", params.ExplorationID, err)
		errMsg := &models.Error{Code: 404, Message: "Error: Unknown ExplorationID"}
		return op.NewDeleteUsersUserIDExplorationsExplorationIDNotFound().WithPayload(errMsg)
	}

	if err := h.ExplorationStore.Delete(ctx, &chronograf.Exploration{ID: chronograf.ExplorationID(eID)}); err != nil {
		log.Printf("Error: Failed to delete Exploration: %v: %v", params.ExplorationID, err)
		errMsg := &models.Error{Code: 500, Message: "Error: Failed to delete Exploration"}
		return op.NewDeleteUsersUserIDExplorationsExplorationIDDefault(500).WithPayload(errMsg)
	}
	return op.NewDeleteUsersUserIDExplorationsExplorationIDNoContent()
}
