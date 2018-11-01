package inmem

import (
	"context"
	"errors"
	"fmt"

	"github.com/influxdata/platform"
)

var (
	errScraperTargetNotFound = fmt.Errorf("scraper target is not found")
)

var _ platform.ScraperTargetStoreService = (*Service)(nil)

func (s *Service) loadScraperTarget(id platform.ID) (*platform.ScraperTarget, error) {
	i, ok := s.scraperTargetKV.Load(id.String())
	if !ok {
		return nil, errScraperTargetNotFound
	}

	b, ok := i.(platform.ScraperTarget)
	if !ok {
		return nil, fmt.Errorf("type %T is not a scraper target", i)
	}
	return &b, nil
}

// ListTargets will list all scrape targets.
func (s *Service) ListTargets(ctx context.Context) (list []platform.ScraperTarget, err error) {
	list = make([]platform.ScraperTarget, 0)
	s.scraperTargetKV.Range(func(_, v interface{}) bool {
		b, ok := v.(platform.ScraperTarget)
		if !ok {
			err = fmt.Errorf("type %T is not a scraper target", v)
			return false
		}
		list = append(list, b)
		return true
	})
	return list, err
}

// AddTarget add a new scraper target into storage.
func (s *Service) AddTarget(ctx context.Context, target *platform.ScraperTarget) (err error) {
	target.ID = s.IDGenerator.ID()
	return s.PutTarget(ctx, target)
}

// RemoveTarget removes a scraper target from the bucket.
func (s *Service) RemoveTarget(ctx context.Context, id platform.ID) error {
	if _, err := s.loadScraperTarget(id); err != nil {
		return err
	}
	s.scraperTargetKV.Delete(id.String())
	return nil
}

// UpdateTarget updates a scraper target.
func (s *Service) UpdateTarget(ctx context.Context, update *platform.ScraperTarget) (target *platform.ScraperTarget, err error) {
	if !update.ID.Valid() {
		return nil, errors.New("update scraper: id is invalid")
	}
	_, err = s.loadScraperTarget(update.ID)
	if err != nil {
		return nil, err
	}
	err = s.PutTarget(ctx, update)
	return update, err
}

// GetTargetByID retrieves a scraper target by id.
func (s *Service) GetTargetByID(ctx context.Context, id platform.ID) (target *platform.ScraperTarget, err error) {
	return s.loadScraperTarget(id)
}

// PutTarget will put a scraper target without setting an ID.
func (s *Service) PutTarget(ctx context.Context, target *platform.ScraperTarget) error {
	s.scraperTargetKV.Store(target.ID.String(), *target)
	return nil
}
