package kv

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	influxdb "github.com/influxdata/influxdb/v2"
)

var (
	sourceBucket = []byte("sourcesv1")
)

// DefaultSource is the default source.
var DefaultSource = influxdb.Source{
	Default: true,
	Name:    "autogen",
	Type:    influxdb.SelfSourceType,
}

const (
	// DefaultSourceID it the default source identifier
	DefaultSourceID = "020f755c3c082000"
	// DefaultSourceOrganizationID is the default source's organization identifier
	DefaultSourceOrganizationID = "50616e67652c206c"
)

func init() {
	if err := DefaultSource.ID.DecodeFromString(DefaultSourceID); err != nil {
		panic(fmt.Sprintf("failed to decode default source id: %v", err))
	}

	if err := DefaultSource.OrganizationID.DecodeFromString(DefaultSourceOrganizationID); err != nil {
		panic(fmt.Sprintf("failed to decode default source organization id: %v", err))
	}
}

// DefaultSource retrieves the default source.
func (s *Service) DefaultSource(ctx context.Context) (*influxdb.Source, error) {
	var sr *influxdb.Source

	err := s.kv.View(ctx, func(tx Tx) error {
		// TODO(desa): make this faster by putting the default source in an index.
		srcs, err := s.findSources(ctx, tx, influxdb.FindOptions{})
		if err != nil {
			return err
		}
		for _, src := range srcs {
			if src.Default {
				sr = src
				return nil
			}
		}
		return &errors.Error{
			Code: errors.ENotFound,
			Msg:  "no default source found",
		}
	})

	if err != nil {
		return nil, &errors.Error{
			Err: err,
		}
	}

	return sr, nil
}

// FindSourceByID retrieves a source by id.
func (s *Service) FindSourceByID(ctx context.Context, id platform.ID) (*influxdb.Source, error) {
	var sr *influxdb.Source

	err := s.kv.View(ctx, func(tx Tx) error {
		src, pe := s.findSourceByID(ctx, tx, id)
		if pe != nil {
			return &errors.Error{
				Err: pe,
			}
		}
		sr = src
		return nil
	})
	return sr, err
}

func (s *Service) findSourceByID(ctx context.Context, tx Tx, id platform.ID) (*influxdb.Source, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, &errors.Error{
			Err: err,
		}
	}

	b, err := tx.Bucket(sourceBucket)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(encodedID)
	if IsNotFound(err) {
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Msg:  influxdb.ErrSourceNotFound,
		}
	}

	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	var sr influxdb.Source
	if err := json.Unmarshal(v, &sr); err != nil {
		return nil, &errors.Error{
			Err: err,
		}
	}

	return &sr, nil
}

// FindSources retrieves all sources that match an arbitrary source filter.
// Filters using ID, or OrganizationID and source Name should be efficient.
// Other filters will do a linear scan across all sources searching for a match.
func (s *Service) FindSources(ctx context.Context, opt influxdb.FindOptions) ([]*influxdb.Source, int, error) {
	ss := []*influxdb.Source{}
	err := s.kv.View(ctx, func(tx Tx) error {
		srcs, err := s.findSources(ctx, tx, opt)
		if err != nil {
			return err
		}
		ss = srcs
		return nil
	})

	if err != nil {
		return nil, 0, &errors.Error{
			Op:  influxdb.OpFindSources,
			Err: err,
		}
	}

	return ss, len(ss), nil
}

func (s *Service) findSources(ctx context.Context, tx Tx, opt influxdb.FindOptions) ([]*influxdb.Source, error) {
	ss := []*influxdb.Source{}

	err := s.forEachSource(ctx, tx, func(s *influxdb.Source) bool {
		ss = append(ss, s)
		return true
	})

	if err != nil {
		return nil, err
	}

	return ss, nil
}

// CreateSource creates a influxdb source and sets s.ID.
func (s *Service) CreateSource(ctx context.Context, src *influxdb.Source) error {
	err := s.kv.Update(ctx, func(tx Tx) error {
		src.ID = s.IDGenerator.ID()

		// Generating an organization id if it missing or invalid
		if !src.OrganizationID.Valid() {
			src.OrganizationID = s.IDGenerator.ID()
		}

		return s.putSource(ctx, tx, src)
	})
	if err != nil {
		return &errors.Error{
			Err: err,
		}
	}
	return nil
}

// PutSource will put a source without setting an ID.
func (s *Service) PutSource(ctx context.Context, src *influxdb.Source) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.putSource(ctx, tx, src)
	})
}

func (s *Service) putSource(ctx context.Context, tx Tx, src *influxdb.Source) error {
	v, err := json.Marshal(src)
	if err != nil {
		return err
	}

	encodedID, err := src.ID.Encode()
	if err != nil {
		return err
	}

	b, err := tx.Bucket(sourceBucket)
	if err != nil {
		return err
	}

	if err := b.Put(encodedID, v); err != nil {
		return err
	}

	return nil
}

// forEachSource will iterate through all sources while fn returns true.
func (s *Service) forEachSource(ctx context.Context, tx Tx, fn func(*influxdb.Source) bool) error {
	b, err := tx.Bucket(sourceBucket)
	if err != nil {
		return err
	}

	cur, err := b.ForwardCursor(nil)
	if err != nil {
		return err
	}

	for k, v := cur.Next(); k != nil; k, v = cur.Next() {
		s := &influxdb.Source{}
		if err := json.Unmarshal(v, s); err != nil {
			return err
		}
		if !fn(s) {
			break
		}
	}

	return nil
}

// UpdateSource updates a source according the parameters set on upd.
func (s *Service) UpdateSource(ctx context.Context, id platform.ID, upd influxdb.SourceUpdate) (*influxdb.Source, error) {
	var sr *influxdb.Source
	err := s.kv.Update(ctx, func(tx Tx) error {
		src, err := s.updateSource(ctx, tx, id, upd)
		if err != nil {
			return &errors.Error{
				Err: err,
			}
		}
		sr = src
		return nil
	})

	return sr, err
}

func (s *Service) updateSource(ctx context.Context, tx Tx, id platform.ID, upd influxdb.SourceUpdate) (*influxdb.Source, error) {
	src, pe := s.findSourceByID(ctx, tx, id)
	if pe != nil {
		return nil, pe
	}

	if err := upd.Apply(src); err != nil {
		return nil, err
	}

	if err := s.putSource(ctx, tx, src); err != nil {
		return nil, err
	}

	return src, nil
}

// DeleteSource deletes a source and prunes it from the index.
func (s *Service) DeleteSource(ctx context.Context, id platform.ID) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		pe := s.deleteSource(ctx, tx, id)
		if pe != nil {
			return &errors.Error{
				Err: pe,
			}
		}
		return nil
	})
}

func (s *Service) deleteSource(ctx context.Context, tx Tx, id platform.ID) error {
	if id == DefaultSource.ID {
		return &errors.Error{
			Code: errors.EForbidden,
			Msg:  "cannot delete autogen source",
		}
	}
	_, pe := s.findSourceByID(ctx, tx, id)
	if pe != nil {
		return pe
	}

	encodedID, err := id.Encode()
	if err != nil {
		return &errors.Error{
			Err: err,
		}
	}

	b, err := tx.Bucket(sourceBucket)
	if err != nil {
		return err
	}

	if err = b.Delete(encodedID); err != nil {
		return &errors.Error{
			Err: err,
		}
	}
	return nil
}
