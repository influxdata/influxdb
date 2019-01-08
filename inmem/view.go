package inmem

import (
	"context"
	"fmt"

	platform "github.com/influxdata/influxdb"
)

func (s *Service) loadView(ctx context.Context, id platform.ID) (*platform.View, *platform.Error) {
	i, ok := s.viewKV.Load(id.String())
	if !ok {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "view not found",
		}
	}

	d, ok := i.(*platform.View)
	if !ok {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  fmt.Sprintf("type %T is not a view", i),
		}
	}
	return d, nil
}

// FindViewByID returns a single view by ID.
func (s *Service) FindViewByID(ctx context.Context, id platform.ID) (*platform.View, error) {
	v, pe := s.loadView(ctx, id)
	if pe != nil {
		return nil, &platform.Error{
			Err: pe,
			Op:  OpPrefix + platform.OpFindViewByID,
		}
	}
	return v, nil
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
	var ds []*platform.View
	if filter.ID != nil {
		d, err := s.FindViewByID(ctx, *filter.ID)
		if err != nil && platform.ErrorCode(err) != platform.ENotFound {
			return nil, 0, &platform.Error{
				Err: err,
				Op:  OpPrefix + platform.OpFindViews,
			}
		}
		if d != nil {
			ds = append(ds, d)
		}

		return ds, len(ds), nil
	}

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
	if err := s.PutView(ctx, c); err != nil {
		return &platform.Error{
			Err: err,
			Op:  OpPrefix + platform.OpCreateView,
		}
	}
	return nil
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
		return nil, &platform.Error{
			Err: err,
			Op:  OpPrefix + platform.OpUpdateView,
		}
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
		return &platform.Error{
			Err: err,
			Op:  OpPrefix + platform.OpDeleteView,
		}
	}
	s.viewKV.Delete(id.String())
	return nil
}
