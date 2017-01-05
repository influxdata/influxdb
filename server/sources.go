package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/influxdata/chronograf"
)

type sourceLinks struct {
	Self       string `json:"self"`       // Self link mapping to this resource
	Kapacitors string `json:"kapacitors"` // URL for kapacitors endpoint
	Proxy      string `json:"proxy"`      // URL for proxy endpoint
}

type sourceResponse struct {
	chronograf.Source
	Links sourceLinks `json:"links"`
}

func newSourceResponse(src chronograf.Source) sourceResponse {
	// If telegraf is not set, we'll set it to the default value.
	if src.Telegraf == "" {
		src.Telegraf = "telegraf"
	}

	httpAPISrcs := "/chronograf/v1/sources"
	return sourceResponse{
		Source: src,
		Links: sourceLinks{
			Self:       fmt.Sprintf("%s/%d", httpAPISrcs, src.ID),
			Proxy:      fmt.Sprintf("%s/%d/proxy", httpAPISrcs, src.ID),
			Kapacitors: fmt.Sprintf("%s/%d/kapacitors", httpAPISrcs, src.ID),
		},
	}
}

// NewSource adds a new valid source to the store
func (h *Service) NewSource(w http.ResponseWriter, r *http.Request) {
	var src chronograf.Source
	if err := json.NewDecoder(r.Body).Decode(&src); err != nil {
		invalidJSON(w, h.Logger)
		return
	}

	if err := ValidSourceRequest(src); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	// By default the telegraf database will be telegraf
	if src.Telegraf == "" {
		src.Telegraf = "telegraf"
	}

	var err error
	if src, err = h.SourcesStore.Add(r.Context(), src); err != nil {
		msg := fmt.Errorf("Error storing source %v: %v", src, err)
		unknownErrorWithMessage(w, msg, h.Logger)
		return
	}

	res := newSourceResponse(src)
	w.Header().Add("Location", res.Links.Self)
	encodeJSON(w, http.StatusCreated, res, h.Logger)
}

type getSourcesResponse struct {
	Sources []sourceResponse `json:"sources"`
}

// Sources returns all sources from the store.
func (h *Service) Sources(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	srcs, err := h.SourcesStore.All(ctx)
	if err != nil {
		Error(w, http.StatusInternalServerError, "Error loading sources", h.Logger)
		return
	}

	res := getSourcesResponse{
		Sources: make([]sourceResponse, len(srcs)),
	}

	for i, src := range srcs {
		res.Sources[i] = newSourceResponse(src)
	}

	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// SourcesID retrieves a source from the store
func (h *Service) SourcesID(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	src, err := h.SourcesStore.Get(ctx, id)
	if err != nil {
		notFound(w, id, h.Logger)
		return
	}

	res := newSourceResponse(src)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// RemoveSource deletes the source from the store
func (h *Service) RemoveSource(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	src := chronograf.Source{ID: id}
	ctx := r.Context()
	if err = h.SourcesStore.Delete(ctx, src); err != nil {
		unknownErrorWithMessage(w, err, h.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// UpdateSource handles incremental updates of a data source
func (h *Service) UpdateSource(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	src, err := h.SourcesStore.Get(ctx, id)
	if err != nil {
		notFound(w, id, h.Logger)
		return
	}

	var req chronograf.Source
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}

	src.Default = req.Default
	src.InsecureSkipVerify = req.InsecureSkipVerify
	if req.Name != "" {
		src.Name = req.Name
	}
	if req.Password != "" {
		src.Password = req.Password
	}
	if req.Username != "" {
		src.Username = req.Username
	}
	if req.URL != "" {
		src.URL = req.URL
	}
	if req.Type != "" {
		src.Type = req.Type
	}
	if req.Telegraf != "" {
		src.Telegraf = req.Telegraf
	}

	if err := ValidSourceRequest(src); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	if err := h.SourcesStore.Update(ctx, src); err != nil {
		msg := fmt.Sprintf("Error updating source ID %d", id)
		Error(w, http.StatusInternalServerError, msg, h.Logger)
		return
	}
	encodeJSON(w, http.StatusOK, newSourceResponse(src), h.Logger)
}

// ValidSourceRequest checks if name, url and type are valid
func ValidSourceRequest(s chronograf.Source) error {
	// Name and URL areq required
	if s.Name == "" || s.URL == "" {
		return fmt.Errorf("name and url required")
	}
	// Type must be influx or influx-enterprise
	if s.Type != "" {
		if s.Type != "influx" && s.Type != "influx-enterprise" {
			return fmt.Errorf("invalid source type %s", s.Type)
		}
	}

	url, err := url.ParseRequestURI(s.URL)
	if err != nil {
		return fmt.Errorf("invalid source URI: %v", err)
	}
	if len(url.Scheme) == 0 {
		return fmt.Errorf("Invalid URL; no URL scheme defined")
	}
	return nil
}
