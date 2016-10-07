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

func layoutToMrF(l *models.Layout) mrfusion.Layout {
	cells := make([]mrfusion.Cell, len(l.Cells))
	for i, c := range l.Cells {
		queries := make([]mrfusion.Query, len(c.Queries))
		for j, q := range c.Queries {
			queries[j] = mrfusion.Query{
				Command: *q.Query,
				DB:      q.Db,
				RP:      q.Rp,
			}
		}
		cells[i] = mrfusion.Cell{
			X:       *c.X,
			Y:       *c.Y,
			W:       *c.W,
			H:       *c.H,
			Queries: queries,
		}
	}
	return mrfusion.Layout{
		Measurement: *l.Measurement,
		Application: *l.App,
		Cells:       cells,
	}
}

func (h *Store) NewLayout(ctx context.Context, params op.PostLayoutsParams) middleware.Responder {
	layout := layoutToMrF(params.Layout)
	var err error
	if layout, err = h.LayoutStore.Add(ctx, layout); err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error storing layout %v: %v", params.Layout, err)}
		return op.NewPostLayoutsDefault(500).WithPayload(errMsg)
	}
	mlayout := layoutToModel(layout)
	return op.NewPostLayoutsCreated().WithPayload(mlayout).WithLocation(*mlayout.Link.Href)
}

func layoutToModel(l mrfusion.Layout) *models.Layout {
	href := fmt.Sprintf("/chronograf/v1/layouts/%d", l.ID)
	rel := "self"

	cells := make([]*models.Cell, len(l.Cells))
	for i, c := range l.Cells {
		queries := make([]*models.Proxy, len(c.Queries))
		for j, q := range c.Queries {
			queries[j] = &models.Proxy{
				Query: &q.Command,
				Db:    q.DB,
				Rp:    q.RP,
			}
		}

		x := c.X
		y := c.Y
		w := c.W
		h := c.H

		cells[i] = &models.Cell{
			X:       &x,
			Y:       &y,
			W:       &w,
			H:       &h,
			Queries: queries,
		}
	}

	return &models.Layout{
		Link: &models.Link{
			Href: &href,
			Rel:  &rel,
		},
		Cells:       cells,
		Measurement: &l.Measurement,
		App:         &l.Application,
	}
}

func requestedLayout(filtered map[string]bool, layout mrfusion.Layout) bool {
	// If the length of the filter is zero then all values are acceptable.
	if len(filtered) == 0 {
		return true
	}

	// If filter contains either measurement or application
	return filtered[layout.Measurement] || filtered[layout.Application]
}

func (h *Store) Layouts(ctx context.Context, params op.GetLayoutsParams) middleware.Responder {
	// Construct a filter sieve for both applications and measurements
	filtered := map[string]bool{}
	for _, a := range params.Apps {
		filtered[a] = true
	}

	for _, m := range params.TelegrafMeasurements {
		filtered[m] = true
	}

	mrLays, err := h.LayoutStore.All(ctx)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: "Error loading layouts"}
		return op.NewGetLayoutsDefault(500).WithPayload(errMsg)
	}

	lays := []*models.Layout{}
	for _, layout := range mrLays {
		if requestedLayout(filtered, layout) {
			lays = append(lays, layoutToModel(layout))
		}
	}

	res := &models.Layouts{
		Layouts: lays,
	}

	return op.NewGetLayoutsOK().WithPayload(res)
}

func (h *Store) LayoutsID(ctx context.Context, params op.GetLayoutsIDParams) middleware.Responder {
	id, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewGetLayoutsIDDefault(500).WithPayload(errMsg)
	}

	layout, err := h.LayoutStore.Get(ctx, id)
	if err != nil {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.ID)}
		return op.NewGetLayoutsIDNotFound().WithPayload(errMsg)
	}

	return op.NewGetLayoutsIDOK().WithPayload(layoutToModel(layout))
}

func (h *Store) RemoveLayout(ctx context.Context, params op.DeleteLayoutsIDParams) middleware.Responder {
	id, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewDeleteLayoutsIDDefault(500).WithPayload(errMsg)
	}
	layout := mrfusion.Layout{
		ID: id,
	}
	if err = h.LayoutStore.Delete(ctx, layout); err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Unknown error deleting layout %s", params.ID)}
		return op.NewDeleteLayoutsIDDefault(500).WithPayload(errMsg)
	}

	return op.NewDeleteLayoutsIDNoContent()
}

func (h *Store) UpdateLayout(ctx context.Context, params op.PutLayoutsIDParams) middleware.Responder {
	id, err := strconv.Atoi(params.ID)
	if err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error converting ID %s", params.ID)}
		return op.NewPutLayoutsIDDefault(500).WithPayload(errMsg)
	}
	layout, err := h.LayoutStore.Get(ctx, id)
	if err != nil {
		errMsg := &models.Error{Code: 404, Message: fmt.Sprintf("Unknown ID %s", params.ID)}
		return op.NewPutLayoutsIDNotFound().WithPayload(errMsg)
	}
	layout = layoutToMrF(params.Config)
	layout.ID = id
	if err := h.LayoutStore.Update(ctx, layout); err != nil {
		errMsg := &models.Error{Code: 500, Message: fmt.Sprintf("Error updating layout ID %s", params.ID)}
		return op.NewPutLayoutsIDDefault(500).WithPayload(errMsg)
	}
	return op.NewPutLayoutsIDOK().WithPayload(layoutToModel(layout))
}
