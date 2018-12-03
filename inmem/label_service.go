package inmem

import (
	"context"
	"fmt"
	"path"

	"github.com/influxdata/platform"
)

func encodeLabelKey(resourceID platform.ID, name string) string {
	return path.Join(resourceID.String(), name)
}

func (s *Service) loadLabel(ctx context.Context, resourceID platform.ID, name string) (*platform.Label, error) {
	i, ok := s.labelKV.Load(encodeLabelKey(resourceID, name))
	if !ok {
		return nil, fmt.Errorf("label not found")
	}

	l, ok := i.(platform.Label)
	if !ok {
		return nil, fmt.Errorf("type %T is not a label", i)
	}

	return &l, nil
}

func (s *Service) FindLabelBy(ctx context.Context, resourceID platform.ID, name string) (*platform.Label, error) {
	return s.loadLabel(ctx, resourceID, name)
}

func (s *Service) forEachLabel(ctx context.Context, fn func(m *platform.Label) bool) error {
	var err error
	s.labelKV.Range(func(k, v interface{}) bool {
		l, ok := v.(platform.Label)
		if !ok {
			err = fmt.Errorf("type %T is not a label", v)
			return false
		}
		return fn(&l)
	})

	return err
}

func (s *Service) filterLabels(ctx context.Context, fn func(m *platform.Label) bool) ([]*platform.Label, error) {
	labels := []*platform.Label{}
	err := s.forEachLabel(ctx, func(l *platform.Label) bool {
		if fn(l) {
			labels = append(labels, l)
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return labels, nil
}

func (s *Service) FindLabels(ctx context.Context, filter platform.LabelFilter, opt ...platform.FindOptions) ([]*platform.Label, error) {
	if filter.ResourceID.Valid() && filter.Name != "" {
		l, err := s.FindLabelBy(ctx, filter.ResourceID, filter.Name)
		if err != nil {
			return nil, err
		}
		return []*platform.Label{l}, nil
	}

	filterFunc := func(label *platform.Label) bool {
		return (!filter.ResourceID.Valid() || (filter.ResourceID == label.ResourceID)) &&
			(filter.Name == "" || (filter.Name == label.Name))
	}

	labels, err := s.filterLabels(ctx, filterFunc)
	if err != nil {
		return nil, err
	}

	return labels, nil
}

func (s *Service) CreateLabel(ctx context.Context, l *platform.Label) error {
	label, _ := s.FindLabelBy(ctx, l.ResourceID, l.Name)
	if label != nil {
		return fmt.Errorf("label %s already exists", l.Name)
	}

	s.labelKV.Store(encodeLabelKey(l.ResourceID, l.Name), *l)
	return nil
}

func (s *Service) PutLabel(ctx context.Context, l *platform.Label) error {
	s.labelKV.Store(encodeLabelKey(l.ResourceID, l.Name), *l)
	return nil
}

func (s *Service) DeleteLabel(ctx context.Context, l platform.Label) error {
	label, err := s.FindLabelBy(ctx, l.ResourceID, l.Name)
	if label == nil && err != nil {
		return err
	}

	s.labelKV.Delete(encodeLabelKey(l.ResourceID, l.Name))
	return nil
}

func (s *Service) deleteLabel(ctx context.Context, filter platform.LabelFilter) error {
	labels, err := s.FindLabels(ctx, filter)
	if labels == nil && err != nil {
		return err
	}
	for _, l := range labels {
		s.labelKV.Delete(encodeLabelKey(l.ResourceID, l.Name))
	}

	return nil
}
