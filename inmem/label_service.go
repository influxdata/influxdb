package inmem

import (
	"context"
	"fmt"
	"path"

	platform "github.com/influxdata/influxdb"
)

func (s *Service) loadLabel(ctx context.Context, id platform.ID) (*platform.Label, error) {
	i, ok := s.labelKV.Load(id.String())
	if !ok {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "label not found",
		}
	}

	l, ok := i.(platform.Label)
	if !ok {
		return nil, fmt.Errorf("type %T is not a label", i)
	}

	return &l, nil
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

func (s *Service) forEachLabelMapping(ctx context.Context, fn func(m *platform.LabelMapping) bool) error {
	var err error
	s.labelMappingKV.Range(func(k, v interface{}) bool {
		m, ok := v.(platform.LabelMapping)
		if !ok {
			err = fmt.Errorf("type %T is not a label mapping", v)
			return false
		}
		return fn(&m)
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

func (s *Service) filterLabelMappings(ctx context.Context, fn func(m *platform.LabelMapping) bool) ([]*platform.LabelMapping, error) {
	mappings := []*platform.LabelMapping{}
	err := s.forEachLabelMapping(ctx, func(m *platform.LabelMapping) bool {
		if fn(m) {
			mappings = append(mappings, m)
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return mappings, nil
}

func encodeLabelMappingKey(m *platform.LabelMapping) string {
	return path.Join(m.ResourceID.String(), m.LabelID.String())
}

// FindLabelByID returns a single user by ID.
func (s *Service) FindLabelByID(ctx context.Context, id platform.ID) (*platform.Label, error) {
	return s.loadLabel(ctx, id)
}

// FindLabels will retrieve a list of labels from storage.
func (s *Service) FindLabels(ctx context.Context, filter platform.LabelFilter, opt ...platform.FindOptions) ([]*platform.Label, error) {
	if filter.ID.Valid() {
		l, err := s.FindLabelByID(ctx, filter.ID)
		if err != nil {
			return nil, err
		}
		return []*platform.Label{l}, nil
	}

	filterFunc := func(label *platform.Label) bool {
		return (filter.Name == "" || (filter.Name == label.Name))
	}

	labels, err := s.filterLabels(ctx, filterFunc)
	if err != nil {
		return nil, err
	}

	return labels, nil
}

// FindResourceLabels returns a list of labels that are mapped to a resource.
func (s *Service) FindResourceLabels(ctx context.Context, filter platform.LabelMappingFilter) ([]*platform.Label, error) {
	filterFunc := func(mapping *platform.LabelMapping) bool {
		return (filter.ResourceID.String() == mapping.ResourceID.String())
	}

	mappings, err := s.filterLabelMappings(ctx, filterFunc)
	if err != nil {
		return nil, err
	}

	ls := []*platform.Label{}
	for _, m := range mappings {
		l, err := s.FindLabelByID(ctx, *m.LabelID)
		if err != nil {
			return nil, err
		}

		ls = append(ls, l)
	}

	return ls, nil
}

// CreateLabel creates a new label.
func (s *Service) CreateLabel(ctx context.Context, l *platform.Label) error {
	l.ID = s.IDGenerator.ID()
	s.labelKV.Store(l.ID, *l)
	return nil
}

// CreateLabelMapping creates a mapping that associates a label to a resource.
func (s *Service) CreateLabelMapping(ctx context.Context, m *platform.LabelMapping) error {
	_, err := s.FindLabelByID(ctx, *m.LabelID)
	if err != nil {
		return &platform.Error{
			Err: err,
			Op:  platform.OpCreateLabel,
		}
	}

	s.labelMappingKV.Store(encodeLabelMappingKey(m), *m)
	return nil
}

// UpdateLabel updates a label.
func (s *Service) UpdateLabel(ctx context.Context, id platform.ID, upd platform.LabelUpdate) (*platform.Label, error) {
	label, err := s.FindLabelByID(ctx, id)
	if err != nil {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Op:   OpPrefix + platform.OpUpdateLabel,
			Err:  err,
			Msg:  "label not found",
		}
	}

	if label.Properties == nil {
		label.Properties = make(map[string]string)
	}

	for k, v := range upd.Properties {
		if v == "" {
			delete(label.Properties, k)
		} else {
			label.Properties[k] = v
		}
	}

	if err := label.Validate(); err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Op:   OpPrefix + platform.OpUpdateLabel,
			Err:  err,
		}
	}

	s.labelKV.Store(label.ID.String(), *label)

	return label, nil
}

// PutLabel writes a label directly to the database without generating IDs
// or making checks.
func (s *Service) PutLabel(ctx context.Context, l *platform.Label) error {
	s.labelKV.Store(l.ID.String(), *l)
	return nil
}

// DeleteLabel deletes a label.
func (s *Service) DeleteLabel(ctx context.Context, id platform.ID) error {
	label, err := s.FindLabelByID(ctx, id)
	if label == nil && err != nil {
		return &platform.Error{
			Code: platform.ENotFound,
			Op:   OpPrefix + platform.OpDeleteLabel,
			Err:  platform.ErrLabelNotFound,
			Msg:  "label not found",
		}
	}

	s.labelKV.Delete(id.String())
	return nil
}

// DeleteLabelMapping deletes a label mapping.
func (s *Service) DeleteLabelMapping(ctx context.Context, m *platform.LabelMapping) error {
	s.labelMappingKV.Delete(encodeLabelMappingKey(m))
	return nil
}
