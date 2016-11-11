package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/influxdata/chronograf"
)

type link struct {
	Href string `json:"href"`
	Rel  string `json:"rel"`
}

type exploration struct {
	Name      string      `json:"name"`       // Exploration name given by user.
	Data      interface{} `json:"data"`       // Serialization of the exploration config.
	CreatedAt time.Time   `json:"created_at"` // Time exploration was created
	UpdatedAt time.Time   `json:"updated_at"` // Latest time the exploration was updated.
	Link      link        `json:"link"`       // Self link
}

func newExploration(e *chronograf.Exploration) exploration {
	rel := "self"
	href := fmt.Sprintf("%s/%d/explorations/%d", "/chronograf/v1/users", e.UserID, e.ID)
	return exploration{
		Name:      e.Name,
		Data:      e.Data,
		CreatedAt: e.CreatedAt,
		UpdatedAt: e.UpdatedAt,
		Link: link{
			Rel:  rel,
			Href: href,
		},
	}
}

type explorations struct {
	Explorations []exploration `json:"explorations"`
}

// Explorations returns all explorations scoped by user id.
func (h *Service) Explorations(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	ctx := r.Context()
	mrExs, err := h.ExplorationStore.Query(ctx, chronograf.UserID(id))
	if err != nil {
		unknownErrorWithMessage(w, err)
		return
	}

	exs := make([]exploration, len(mrExs))
	for i, e := range mrExs {
		exs[i] = newExploration(e)
	}
	res := explorations{
		Explorations: exs,
	}

	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// ExplorationsID retrieves exploration ID scoped under user.
func (h *Service) ExplorationsID(w http.ResponseWriter, r *http.Request) {
	eID, err := paramID("eid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	uID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	ctx := r.Context()
	e, err := h.ExplorationStore.Get(ctx, chronograf.ExplorationID(eID))
	if err != nil || e.UserID != chronograf.UserID(uID) {
		notFound(w, eID)
		return
	}

	res := newExploration(e)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

type patchExplorationRequest struct {
	Data interface{} `json:"data,omitempty"` // Serialized configuration
	Name *string     `json:"name,omitempty"` // Exploration name given by user.
}

// UpdateExploration incrementally updates exploration
func (h *Service) UpdateExploration(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("eid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	uID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	ctx := r.Context()
	e, err := h.ExplorationStore.Get(ctx, chronograf.ExplorationID(id))
	if err != nil || e.UserID != chronograf.UserID(uID) {
		notFound(w, id)
		return
	}

	var req patchExplorationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w)
		return
	}

	if req.Data != nil {
		var ok bool
		if e.Data, ok = req.Data.(string); !ok {
			err := fmt.Errorf("Error: Exploration data is not a string")
			invalidData(w, err)
			return
		}
	}

	if req.Name != nil {
		e.Name = *req.Name
	}

	if err := h.ExplorationStore.Update(ctx, e); err != nil {
		msg := "Error: Failed to update Exploration"
		Error(w, http.StatusInternalServerError, msg)
		return
	}

	res := newExploration(e)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

type postExplorationRequest struct {
	Data interface{} `json:"data"`           // Serialization of config.
	Name string      `json:"name,omitempty"` // Exploration name given by user.
}

// NewExploration adds valid exploration scoped by user id.
func (h *Service) NewExploration(w http.ResponseWriter, r *http.Request) {
	uID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	// TODO: Check user if user exists.
	var req postExplorationRequest
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w)
		return
	}

	data := ""
	if req.Data != nil {
		data, _ = req.Data.(string)
	}

	e := &chronograf.Exploration{
		Name:   req.Name,
		UserID: chronograf.UserID(uID),
		Data:   data,
	}

	ctx := r.Context()
	e, err = h.ExplorationStore.Add(ctx, e)
	if err != nil {
		msg := fmt.Errorf("Error: Failed to save Exploration")
		unknownErrorWithMessage(w, msg)
		return
	}

	res := newExploration(e)
	w.Header().Add("Location", res.Link.Href)
	encodeJSON(w, http.StatusCreated, res, h.Logger)
}

// RemoveExploration deletes exploration from store.
func (h *Service) RemoveExploration(w http.ResponseWriter, r *http.Request) {
	eID, err := paramID("eid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	uID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	ctx := r.Context()
	e, err := h.ExplorationStore.Get(ctx, chronograf.ExplorationID(eID))
	if err != nil || e.UserID != chronograf.UserID(uID) {
		notFound(w, eID)
		return
	}

	if err := h.ExplorationStore.Delete(ctx, &chronograf.Exploration{ID: chronograf.ExplorationID(eID)}); err != nil {
		unknownErrorWithMessage(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
