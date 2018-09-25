package inmem

import (
	"context"
	"fmt"

	"github.com/influxdata/platform"
)

func (s *Service) loadView(ctx context.Context, id platform.ID) (*platform.View, error) {
	i, ok := s.viewKV.Load(id.String())
	if !ok {
		return nil, fmt.Errorf("View not found")
	}

	d, ok := i.(*platform.View)
	if !ok {
		return nil, fmt.Errorf("type %T is not a view", i)
	}
	return d, nil
}

// FindViewByID returns a single view by ID.
func (s *Service) FindViewByID(ctx context.Context, id platform.ID) (*platform.View, error) {
	return s.loadView(ctx, id)
}

func filterViewFn(filter platform.ViewFilter) func(d *platform.View) bool {
	if filter.ID != nil {
		return func(d *platform.View) bool {
			return d.ID == *filter.ID
		}
	}

	return func(d *platform.View) bool { return true }
}

// FindViews implements platform.ViewService interface.
func (s *Service) FindViews(ctx context.Context, filter platform.ViewFilter) ([]*platform.View, int, error) {
	if filter.ID != nil {
		d, err := s.FindViewByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.View{d}, 1, nil
	}

	var ds []*platform.View
	var err error
	filterF := filterViewFn(filter)
	s.viewKV.Range(func(k, v interface{}) bool {
		d, ok := v.(*platform.View)
		if !ok {
			return false
		}

		if filterF(d) {
			ds = append(ds, d)
		}
		return true
	})
	return ds, len(ds), err
}

// CreateView implements platform.ViewService interface.
func (s *Service) CreateView(ctx context.Context, c *platform.View) error {
	c.ID = s.IDGenerator.ID()
	return s.PutView(ctx, c)
}

// PutView implements platform.ViewService interface.
func (s *Service) PutView(ctx context.Context, c *platform.View) error {
	if c.Properties == nil {
		c.Properties = platform.EmptyViewProperties{}
	}
	s.viewKV.Store(c.ID.String(), c)
	return nil
}

// UpdateView implements platform.ViewService interface.
func (s *Service) UpdateView(ctx context.Context, id platform.ID, upd platform.ViewUpdate) (*platform.View, error) {
	c, err := s.FindViewByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		c.Name = *upd.Name
	}

	if upd.Properties != nil {
		c.Properties = upd.Properties
	}

	s.viewKV.Store(c.ID.String(), c)

	return c, nil
}

// DeleteView implements platform.ViewService interface.
func (s *Service) DeleteView(ctx context.Context, id platform.ID) error {
	if _, err := s.FindViewByID(ctx, id); err != nil {
		return err
	}
	s.viewKV.Delete(id.String())
	return nil
}

func (s *Service) createViewIfNotExists(ctx context.Context, cell *platform.Cell, opts platform.AddDashboardCellOptions) error {
	if opts.UsingView.Valid() {
		// Creates a hard copy of a view
		v, err := s.FindViewByID(ctx, opts.UsingView)
		if err != nil {
			return err
		}
		view, err := s.copyView(ctx, v.ID)
		if err != nil {
			return err
		}
		cell.ViewID = view.ID
		return nil
	} else if cell.ViewID.Valid() {
		// Creates a soft copy of a view
		_, err := s.FindViewByID(ctx, cell.ViewID)
		if err != nil {
			return err
		}
		return nil
	}

	// If not view exists create the view
	view := &platform.View{}
	if err := s.CreateView(ctx, view); err != nil {
		return err
	}
	cell.ViewID = view.ID

	return nil
}

func (s *Service) copyView(ctx context.Context, id platform.ID) (*platform.View, error) {
	v, err := s.FindViewByID(ctx, id)
	if err != nil {
		return nil, err
	}

	view := &platform.View{
		ViewContents: platform.ViewContents{
			Name: v.Name,
		},
		Properties: v.Properties,
	}

	if err := s.CreateView(ctx, view); err != nil {
		return nil, err
	}

	return view, nil
}
