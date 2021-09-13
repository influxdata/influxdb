package label

import (
	"context"
	"strings"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv"
)

type Service struct {
	store *Store
}

func NewService(st *Store) influxdb.LabelService {
	return &Service{
		store: st,
	}
}

// CreateLabel creates a new label.
func (s *Service) CreateLabel(ctx context.Context, l *influxdb.Label) error {
	if err := l.Validate(); err != nil {
		return &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		}
	}

	l.Name = strings.TrimSpace(l.Name)

	err := s.store.Update(ctx, func(tx kv.Tx) error {
		if err := uniqueLabelName(ctx, tx, l); err != nil {
			return err
		}

		if err := s.store.CreateLabel(ctx, tx, l); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	return err
}

// FindLabelByID finds a label by its ID
func (s *Service) FindLabelByID(ctx context.Context, id platform.ID) (*influxdb.Label, error) {
	var l *influxdb.Label

	err := s.store.View(ctx, func(tx kv.Tx) error {
		label, e := s.store.GetLabel(ctx, tx, id)
		if e != nil {
			return e
		}
		l = label
		return nil
	})

	if err != nil {
		return nil, &errors.Error{
			Err: err,
		}
	}

	return l, nil
}

// FindLabels returns a list of labels that match a filter.
func (s *Service) FindLabels(ctx context.Context, filter influxdb.LabelFilter, opt ...influxdb.FindOptions) ([]*influxdb.Label, error) {
	ls := []*influxdb.Label{}
	err := s.store.View(ctx, func(tx kv.Tx) error {
		labels, err := s.store.ListLabels(ctx, tx, filter)
		if err != nil {
			return err
		}
		ls = labels
		return nil
	})

	if err != nil {
		return nil, err
	}

	return ls, nil
}

func (s *Service) FindResourceLabels(ctx context.Context, filter influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
	ls := []*influxdb.Label{}
	if err := s.store.View(ctx, func(tx kv.Tx) error {
		return s.store.FindResourceLabels(ctx, tx, filter, &ls)
	}); err != nil {
		return nil, err
	}

	return ls, nil
}

// UpdateLabel updates a label.
func (s *Service) UpdateLabel(ctx context.Context, id platform.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
	var label *influxdb.Label
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		l, e := s.store.UpdateLabel(ctx, tx, id, upd)
		if e != nil {
			return &errors.Error{
				Err: e,
			}
		}
		label = l
		return nil
	})

	return label, err
}

// DeleteLabel deletes a label.
func (s *Service) DeleteLabel(ctx context.Context, id platform.ID) error {
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		return s.store.DeleteLabel(ctx, tx, id)
	})
	if err != nil {
		return &errors.Error{
			Err: err,
		}
	}
	return nil
}

//******* Label Mappings *******//

// CreateLabelMapping creates a new mapping between a resource and a label.
func (s *Service) CreateLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	err := s.store.View(ctx, func(tx kv.Tx) error {
		if _, err := s.store.GetLabel(ctx, tx, m.LabelID); err != nil {
			return err
		}

		ls := []*influxdb.Label{}
		err := s.store.FindResourceLabels(ctx, tx, influxdb.LabelMappingFilter{ResourceID: m.ResourceID, ResourceType: m.ResourceType}, &ls)
		if err != nil {
			return err
		}
		for i := 0; i < len(ls); i++ {
			if ls[i].ID == m.LabelID {
				return influxdb.ErrLabelExistsOnResource
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return s.store.Update(ctx, func(tx kv.Tx) error {
		return s.store.CreateLabelMapping(ctx, tx, m)
	})
}

// DeleteLabelMapping deletes a label mapping.
func (s *Service) DeleteLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		return s.store.DeleteLabelMapping(ctx, tx, m)
	})
	if err != nil {
		return &errors.Error{
			Err: err,
		}
	}
	return nil
}

//******* helper functions *******//

func unique(ctx context.Context, tx kv.Tx, indexBucket, indexKey []byte) error {
	bucket, err := tx.Bucket(indexBucket)
	if err != nil {
		return kv.UnexpectedIndexError(err)
	}

	_, err = bucket.Get(indexKey)
	// if not found then this is  _unique_.
	if kv.IsNotFound(err) {
		return nil
	}

	// no error means this is not unique
	if err == nil {
		return kv.NotUniqueError
	}

	// any other error is some sort of internal server error
	return kv.UnexpectedIndexError(err)
}

func uniqueLabelName(ctx context.Context, tx kv.Tx, lbl *influxdb.Label) error {
	key, err := labelIndexKey(lbl)
	if err != nil {
		return err
	}

	// labels are unique by `organization:label_name`
	err = unique(ctx, tx, labelIndex, key)
	if err == kv.NotUniqueError {
		return labelAlreadyExistsError(lbl)
	}
	return err
}
