package server

import (
	"fmt"
	"net/http"

	"github.com/bouk/httprouter"
	"github.com/influxdata/platform/chronograf"
)

type link struct {
	Href string `json:"href"`
	Rel  string `json:"rel"`
}

type layoutResponse struct {
	chronograf.Layout
	Link link `json:"link"`
}

func newLayoutResponse(layout chronograf.Layout) layoutResponse {
	httpAPILayouts := "/chronograf/v1/layouts"
	href := fmt.Sprintf("%s/%s", httpAPILayouts, layout.ID)
	rel := "self"

	for idx, cell := range layout.Cells {
		axes := []string{"x", "y", "y2"}

		if cell.Axes == nil {
			layout.Cells[idx].Axes = make(map[string]chronograf.Axis, len(axes))
		}

		if cell.CellColors == nil {
			layout.Cells[idx].CellColors = []chronograf.CellColor{}
		}

		for _, axis := range axes {
			if _, found := cell.Axes[axis]; !found {
				layout.Cells[idx].Axes[axis] = chronograf.Axis{
					Bounds: []string{},
				}
			}
		}
	}

	return layoutResponse{
		Layout: layout,
		Link: link{
			Href: href,
			Rel:  rel,
		},
	}
}

type getLayoutsResponse struct {
	Layouts []layoutResponse `json:"layouts"`
}

// Layouts retrieves all layouts from store
func (s *Service) Layouts(w http.ResponseWriter, r *http.Request) {
	// Construct a filter sieve for both applications and measurements
	filtered := map[string]bool{}
	for _, a := range r.URL.Query()["app"] {
		filtered[a] = true
	}

	for _, m := range r.URL.Query()["measurement"] {
		filtered[m] = true
	}

	ctx := r.Context()
	layouts, err := s.Store.Layouts(ctx).All(ctx)
	if err != nil {
		Error(w, http.StatusInternalServerError, "Error loading layouts", s.Logger)
		return
	}

	filter := func(layout *chronograf.Layout) bool {
		// If the length of the filter is zero then all values are acceptable.
		if len(filtered) == 0 {
			return true
		}

		// If filter contains either measurement or application
		return filtered[layout.Measurement] || filtered[layout.Application]
	}

	res := getLayoutsResponse{
		Layouts: []layoutResponse{},
	}

	seen := make(map[string]bool)
	for _, layout := range layouts {
		// remove duplicates
		if seen[layout.Measurement+layout.ID] {
			continue
		}
		// filter for data that belongs to provided application or measurement
		if filter(&layout) {
			res.Layouts = append(res.Layouts, newLayoutResponse(layout))
		}
	}
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// LayoutsID retrieves layout with ID from store
func (s *Service) LayoutsID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := httprouter.GetParamFromContext(ctx, "id")

	layout, err := s.Store.Layouts(ctx).Get(ctx, id)
	if err != nil {
		Error(w, http.StatusNotFound, fmt.Sprintf("ID %s not found", id), s.Logger)
		return
	}

	res := newLayoutResponse(layout)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}
