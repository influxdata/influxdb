package kv

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb"
)

// UnexpectedScrapersBucketError is used when the error comes from an internal system.
func UnexpectedScrapersBucketError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "unexpected error retrieving scrapers bucket",
		Err:  err,
		Op:   "kv/scrapersBucket",
	}
}

var (
	scrapersBucket = []byte("scraperv2")
)

var _ influxdb.ScraperTargetStoreService = (*Service)(nil)

func (s *Service) initializeScraperTargets(ctx context.Context, tx Tx) error {
	_, err := s.scrapersBucket(tx)
	return err
}

func (s *Service) scrapersBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket([]byte(scrapersBucket))
	if err != nil {
		return nil, UnexpectedScrapersBucketError(err)
	}

	return b, nil
}

// ListTargets will list all scrape targets.
func (s *Service) ListTargets(ctx context.Context) (list []influxdb.ScraperTarget, err error) {
	list = make([]influxdb.ScraperTarget, 0)
	err = s.kv.View(func(tx Tx) (err error) {
		bucket, err := s.scrapersBucket(tx)
		if err != nil {
			return err
		}

		cur, err := bucket.Cursor()
		if err != nil {
			return err
		}

		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			target := new(influxdb.ScraperTarget)
			if err = json.Unmarshal(v, target); err != nil {
				return err
			}
			list = append(list, *target)
		}
		return err
	})
	if err != nil {
		return nil, &influxdb.Error{
			Op:  "kv/" + influxdb.OpListTargets,
			Err: err,
		}
	}
	return list, err
}

// AddTarget add a new scraper target into storage.
func (s *Service) AddTarget(ctx context.Context, target *influxdb.ScraperTarget, userID influxdb.ID) (err error) {
	if !target.OrgID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "org id is invalid",
			Op:   OpPrefix + influxdb.OpAddTarget,
		}
	}
	if !target.BucketID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "bucket id is invalid",
			Op:   OpPrefix + influxdb.OpAddTarget,
		}
	}
	err = s.kv.Update(func(tx Tx) error {
		target.ID = s.IDGenerator.ID()
		if err := s.putTarget(ctx, tx, target); err != nil {
			return err
		}

		urm := &influxdb.UserResourceMapping{
			ResourceID:   target.ID,
			UserID:       userID,
			UserType:     influxdb.Owner,
			ResourceType: influxdb.ScraperResourceType,
		}
		return s.createUserResourceMapping(ctx, tx, urm)
	})
	if err != nil {
		return &influxdb.Error{
			Err: err,
			Op:  OpPrefix + influxdb.OpAddTarget,
		}
	}
	return nil
}

// RemoveTarget removes a scraper target from the bucket.
func (s *Service) RemoveTarget(ctx context.Context, id influxdb.ID) error {
	err := s.kv.Update(func(tx Tx) error {
		_, pe := s.findTargetByID(ctx, tx, id)
		if pe != nil {
			return pe
		}
		encID, err := id.Encode()
		if err != nil {
			return &influxdb.Error{
				Code: influxdb.EInvalid,
				Err:  err,
			}
		}

		bucket, err := s.scrapersBucket(tx)
		if err != nil {
			return err
		}

		if err := bucket.Delete(encID); err != nil {
			return err // TODO(goller): previously this returned nil... why?
		}

		return s.deleteUserResourceMappings(ctx, tx, influxdb.UserResourceMappingFilter{
			ResourceID:   id,
			ResourceType: influxdb.ScraperResourceType,
		})
	})
	if err != nil {
		return &influxdb.Error{
			Err: err,
			Op:  OpPrefix + influxdb.OpRemoveTarget,
		}
	}
	return nil
}

// UpdateTarget updates a scraper target.
func (s *Service) UpdateTarget(ctx context.Context, update *influxdb.ScraperTarget, userID influxdb.ID) (*influxdb.ScraperTarget, error) {
	op := "kv/" + influxdb.OpUpdateTarget
	if !update.ID.Valid() {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Op:   op,
			Msg:  "id is invalid",
		}
	}

	var target *influxdb.ScraperTarget
	err := s.kv.Update(func(tx Tx) error {
		var err error
		target, err = s.findTargetByID(ctx, tx, update.ID)
		if err != nil {
			return err
		}
		if !update.BucketID.Valid() {
			update.BucketID = target.BucketID
		}
		if !update.OrgID.Valid() {
			update.OrgID = target.OrgID
		}
		target = update
		return s.putTarget(ctx, tx, target)
	})

	if err != nil {
		return nil, &influxdb.Error{
			Op:  op,
			Err: err,
		}
	}

	return target, nil
}

// GetTargetByID retrieves a scraper target by id.
func (s *Service) GetTargetByID(ctx context.Context, id influxdb.ID) (*influxdb.ScraperTarget, error) {
	var target *influxdb.ScraperTarget
	err := s.kv.View(func(tx Tx) error {
		var err error
		target, err = s.findTargetByID(ctx, tx, id)
		if err != nil {
			return &influxdb.Error{
				Op:  "kv/" + influxdb.OpGetTargetByID,
				Err: err,
			}
		}
		return nil
	})

	return target, err
}

func (s *Service) findTargetByID(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.ScraperTarget, error) {
	target := new(influxdb.ScraperTarget)
	encID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	bucket, err := s.scrapersBucket(tx)
	if err != nil {
		return nil, err
	}

	v, err := bucket.Get(encID)
	if err != nil {
		return nil, err
	}

	if len(v) == 0 {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "scraper target is not found",
		}
	}

	if err := json.Unmarshal(v, target); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}
	return target, nil
}

func (s *Service) putTarget(ctx context.Context, tx Tx, target *influxdb.ScraperTarget) error {
	v, err := json.Marshal(target)
	if err != nil {
		return err
	}
	encID, err := target.ID.Encode()
	if err != nil {
		return err
	}

	bucket, err := s.scrapersBucket(tx)
	if err != nil {
		return err
	}

	return bucket.Put(encID, v)
}

// PutTarget will put a scraper target without setting an ID.
func (s *Service) PutTarget(ctx context.Context, target *influxdb.ScraperTarget) error {
	return s.kv.Update(func(tx Tx) error {
		return s.putTarget(ctx, tx, target)
	})
}
