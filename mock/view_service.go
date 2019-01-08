package mock

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

var _ platform.ViewService = &ViewService{}

type ViewService struct {
	CreateViewF   func(context.Context, *platform.View) error
	FindViewByIDF func(context.Context, platform.ID) (*platform.View, error)
	FindViewsF    func(context.Context, platform.ViewFilter) ([]*platform.View, int, error)
	UpdateViewF   func(context.Context, platform.ID, platform.ViewUpdate) (*platform.View, error)
	DeleteViewF   func(context.Context, platform.ID) error
}

func (s *ViewService) FindViewByID(ctx context.Context, id platform.ID) (*platform.View, error) {
	return s.FindViewByIDF(ctx, id)
}

func (s *ViewService) FindViews(ctx context.Context, filter platform.ViewFilter) ([]*platform.View, int, error) {
	return s.FindViewsF(ctx, filter)
}

func (s *ViewService) CreateView(ctx context.Context, b *platform.View) error {
	return s.CreateViewF(ctx, b)
}

func (s *ViewService) UpdateView(ctx context.Context, id platform.ID, upd platform.ViewUpdate) (*platform.View, error) {
	return s.UpdateViewF(ctx, id, upd)
}

func (s *ViewService) DeleteView(ctx context.Context, id platform.ID) error {
	return s.DeleteViewF(ctx, id)
}
