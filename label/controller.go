package label

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/feature"
)

var _ influxdb.LabelService = (*LabelController)(nil)

// LabelController is a temporary system for switching the label backend between
// the old and refactored versions with a feature flag
type LabelController struct {
	flagger         feature.Flagger
	oldLabelService influxdb.LabelService
	newLabelService influxdb.LabelService
}

func NewLabelController(flagger feature.Flagger, oldLabelService, newLabelService influxdb.LabelService) *LabelController {
	return &LabelController{
		flagger:         flagger,
		oldLabelService: oldLabelService,
		newLabelService: newLabelService,
	}
}

func (s *LabelController) useNew(ctx context.Context) bool {
	return feature.NewLabelPackage().Enabled(ctx, s.flagger)
}

func (s *LabelController) CreateLabel(ctx context.Context, l *influxdb.Label) error {
	if s.useNew(ctx) {
		return s.newLabelService.CreateLabel(ctx, l)
	}
	return s.oldLabelService.CreateLabel(ctx, l)

}

func (s *LabelController) FindLabelByID(ctx context.Context, id influxdb.ID) (*influxdb.Label, error) {
	if s.useNew(ctx) {
		return s.newLabelService.FindLabelByID(ctx, id)
	}
	return s.oldLabelService.FindLabelByID(ctx, id)

}

func (s *LabelController) FindLabels(ctx context.Context, filter influxdb.LabelFilter, opt ...influxdb.FindOptions) ([]*influxdb.Label, error) {
	if s.useNew(ctx) {
		return s.newLabelService.FindLabels(ctx, filter, opt...)
	}
	return s.oldLabelService.FindLabels(ctx, filter, opt...)

}

func (s *LabelController) FindResourceLabels(ctx context.Context, filter influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
	if s.useNew(ctx) {
		return s.newLabelService.FindResourceLabels(ctx, filter)
	}
	return s.oldLabelService.FindResourceLabels(ctx, filter)

}

func (s *LabelController) UpdateLabel(ctx context.Context, id influxdb.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
	if s.useNew(ctx) {
		return s.newLabelService.UpdateLabel(ctx, id, upd)
	}
	return s.oldLabelService.UpdateLabel(ctx, id, upd)

}

func (s *LabelController) DeleteLabel(ctx context.Context, id influxdb.ID) error {
	if s.useNew(ctx) {
		return s.newLabelService.DeleteLabel(ctx, id)
	}
	return s.oldLabelService.DeleteLabel(ctx, id)

}

func (s *LabelController) CreateLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	if s.useNew(ctx) {
		return s.newLabelService.CreateLabelMapping(ctx, m)
	}
	return s.oldLabelService.CreateLabelMapping(ctx, m)

}

func (s *LabelController) DeleteLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	if s.useNew(ctx) {
		return s.newLabelService.DeleteLabelMapping(ctx, m)
	}
	return s.oldLabelService.DeleteLabelMapping(ctx, m)

}
