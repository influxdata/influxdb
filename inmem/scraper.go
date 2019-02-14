package inmem

import (
	"context"
	"fmt"

	platform "github.com/influxdata/influxdb"
)

const (
	errScraperTargetNotFound = "scraper target is not found"
)

var _ platform.ScraperTargetStoreService = (*Service)(nil)

func (s *Service) loadScraperTarget(id platform.ID) (*platform.ScraperTarget, *platform.Error) {
	i, ok := s.scraperTargetKV.Load(id.String())
	if !ok {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  errScraperTargetNotFound,
		}
	}

	b, ok := i.(platform.ScraperTarget)
	if !ok {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  fmt.Sprintf("type %T is not a scraper target", i),
		}
	}
	return &b, nil
}

// ListTargets will list all scrape targets.
func (s *Service) ListTargets(ctx context.Context) (list []platform.ScraperTarget, err error) {
	list = make([]platform.ScraperTarget, 0)
	s.scraperTargetKV.Range(func(_, v interface{}) bool {
		b, ok := v.(platform.ScraperTarget)
		if !ok {
			err = &platform.Error{
				Code: platform.EInvalid,
				Msg:  fmt.Sprintf("type %T is not a scraper target", v),
			}
			return false
		}
		list = append(list, b)
		return true
	})
	return list, err
}

// AddTarget add a new scraper target into storage.
func (s *Service) AddTarget(ctx context.Context, target *platform.ScraperTarget, userID platform.ID) (err error) {
	target.ID = s.IDGenerator.ID()
	if !target.OrgID.Valid() {
		return &platform.Error{
			Code: platform.EInvalid,
			Msg:  "provided organization ID has invalid format",
			Op:   OpPrefix + platform.OpAddTarget,
		}
	}
	if !target.BucketID.Valid() {
		return &platform.Error{
			Code: platform.EInvalid,
			Msg:  "provided bucket ID has invalid format",
			Op:   OpPrefix + platform.OpAddTarget,
		}
	}
	if err := s.PutTarget(ctx, target); err != nil {
		return &platform.Error{
			Op:  OpPrefix + platform.OpAddTarget,
			Err: err,
		}
	}
	urm := &platform.UserResourceMapping{
		ResourceID:   target.ID,
		UserID:       userID,
		UserType:     platform.Owner,
		ResourceType: platform.ScraperResourceType,
	}
	if err := s.CreateUserResourceMapping(ctx, urm); err != nil {
		return err
	}
	return nil
}

// RemoveTarget removes a scraper target from the bucket.
func (s *Service) RemoveTarget(ctx context.Context, id platform.ID) error {
	if _, pe := s.loadScraperTarget(id); pe != nil {
		return &platform.Error{
			Err: pe,
			Op:  OpPrefix + platform.OpRemoveTarget,
		}
	}
	s.scraperTargetKV.Delete(id.String())
	err := s.deleteUserResourceMapping(ctx, platform.UserResourceMappingFilter{
		ResourceID:   id,
		ResourceType: platform.ScraperResourceType,
	})
	if err != nil {
		return &platform.Error{
			Code: platform.ErrorCode(err),
			Op:   OpPrefix + platform.OpRemoveTarget,
			Err:  err,
		}
	}

	return nil
}

// UpdateTarget updates a scraper target.
func (s *Service) UpdateTarget(ctx context.Context, update *platform.ScraperTarget, userID platform.ID) (target *platform.ScraperTarget, err error) {
	op := OpPrefix + platform.OpUpdateTarget
	if !update.ID.Valid() {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Op:   op,
			Msg:  "provided scraper target ID has invalid format",
		}
	}
	oldTarget, pe := s.loadScraperTarget(update.ID)
	if pe != nil {
		return nil, &platform.Error{
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
		return nil, &platform.Error{
			Op:  op,
			Err: pe,
		}
	}

	return update, nil
}

// GetTargetByID retrieves a scraper target by id.
func (s *Service) GetTargetByID(ctx context.Context, id platform.ID) (target *platform.ScraperTarget, err error) {
	var pe *platform.Error
	if target, pe = s.loadScraperTarget(id); pe != nil {
		return nil, &platform.Error{
			Op:  OpPrefix + platform.OpGetTargetByID,
			Err: pe,
		}
	}
	return target, nil
}

// PutTarget will put a scraper target without setting an ID.
func (s *Service) PutTarget(ctx context.Context, target *platform.ScraperTarget) error {
	s.scraperTargetKV.Store(target.ID.String(), *target)
	return nil
}
