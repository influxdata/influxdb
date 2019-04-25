package inmem

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb"
)

const (
	errScraperTargetNotFound = "scraper target is not found"
)

var _ influxdb.ScraperTargetStoreService = (*Service)(nil)

func (s *Service) loadScraperTarget(id influxdb.ID) (*influxdb.ScraperTarget, *influxdb.Error) {
	i, ok := s.scraperTargetKV.Load(id.String())
	if !ok {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  errScraperTargetNotFound,
		}
	}

	b, ok := i.(influxdb.ScraperTarget)
	if !ok {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("type %T is not a scraper target", i),
		}
	}
	return &b, nil
}

// ListTargets will list all scrape targets.
func (s *Service) ListTargets(ctx context.Context, filter influxdb.ScraperTargetFilter) (list []influxdb.ScraperTarget, err error) {
	list = make([]influxdb.ScraperTarget, 0)
	s.scraperTargetKV.Range(func(_, v interface{}) bool {
		target, ok := v.(influxdb.ScraperTarget)
		if !ok {
			err = &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  fmt.Sprintf("type %T is not a scraper target", v),
			}
			return false
		}
		if filter.IDs != nil {
			if _, ok := filter.IDs[target.ID]; !ok {
				return true
			}
		}
		if filter.Name != nil && target.Name != *filter.Name {
			return true
		}
		if filter.Org != nil {
			o, orgErr := s.findOrganizationByName(ctx, *filter.Org)
			if orgErr != nil {
				err = orgErr
				return false
			}
			if target.OrgID != o.ID {
				return true
			}
		}
		if filter.OrgID != nil {
			o, orgErr := s.FindOrganizationByID(ctx, *filter.OrgID)
			if orgErr != nil {
				err = orgErr
				return true
			}
			if target.OrgID != o.ID {
				return true
			}
		}
		list = append(list, target)
		return true
	})
	return list, err
}

// AddTarget add a new scraper target into storage.
func (s *Service) AddTarget(ctx context.Context, target *influxdb.ScraperTarget, userID influxdb.ID) (err error) {
	target.ID = s.IDGenerator.ID()
	if !target.OrgID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "provided organization ID has invalid format",
			Op:   OpPrefix + influxdb.OpAddTarget,
		}
	}
	if !target.BucketID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "provided bucket ID has invalid format",
			Op:   OpPrefix + influxdb.OpAddTarget,
		}
	}
	if err := s.PutTarget(ctx, target); err != nil {
		return &influxdb.Error{
			Op:  OpPrefix + influxdb.OpAddTarget,
			Err: err,
		}
	}
	urm := &influxdb.UserResourceMapping{
		ResourceID:   target.ID,
		UserID:       userID,
		UserType:     influxdb.Owner,
		ResourceType: influxdb.ScraperResourceType,
	}
	if err := s.CreateUserResourceMapping(ctx, urm); err != nil {
		return err
	}
	return nil
}

// RemoveTarget removes a scraper target from the bucket.
func (s *Service) RemoveTarget(ctx context.Context, id influxdb.ID) error {
	if _, pe := s.loadScraperTarget(id); pe != nil {
		return &influxdb.Error{
			Err: pe,
			Op:  OpPrefix + influxdb.OpRemoveTarget,
		}
	}
	s.scraperTargetKV.Delete(id.String())
	err := s.deleteUserResourceMapping(ctx, influxdb.UserResourceMappingFilter{
		ResourceID:   id,
		ResourceType: influxdb.ScraperResourceType,
	})
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.ErrorCode(err),
			Op:   OpPrefix + influxdb.OpRemoveTarget,
			Err:  err,
		}
	}

	return nil
}

// UpdateTarget updates a scraper target.
func (s *Service) UpdateTarget(ctx context.Context, update *influxdb.ScraperTarget, userID influxdb.ID) (target *influxdb.ScraperTarget, err error) {
	op := OpPrefix + influxdb.OpUpdateTarget
	if !update.ID.Valid() {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Op:   op,
			Msg:  "provided scraper target ID has invalid format",
		}
	}
	oldTarget, pe := s.loadScraperTarget(update.ID)
	if pe != nil {
		return nil, &influxdb.Error{
			Op:  op,
			Err: pe,
		}
	}
	if !update.OrgID.Valid() {
		update.OrgID = oldTarget.OrgID
	}
	if !update.BucketID.Valid() {
		update.BucketID = oldTarget.BucketID
	}
	if err = s.PutTarget(ctx, update); err != nil {
		return nil, &influxdb.Error{
			Op:  op,
			Err: pe,
		}
	}

	return update, nil
}

// GetTargetByID retrieves a scraper target by id.
func (s *Service) GetTargetByID(ctx context.Context, id influxdb.ID) (target *influxdb.ScraperTarget, err error) {
	var pe *influxdb.Error
	if target, pe = s.loadScraperTarget(id); pe != nil {
		return nil, &influxdb.Error{
			Op:  OpPrefix + influxdb.OpGetTargetByID,
			Err: pe,
		}
	}
	return target, nil
}

// PutTarget will put a scraper target without setting an ID.
func (s *Service) PutTarget(ctx context.Context, target *influxdb.ScraperTarget) error {
	s.scraperTargetKV.Store(target.ID.String(), *target)
	return nil
}
