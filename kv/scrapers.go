package kv

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb/v2"
)

var (
	// ErrScraperNotFound is used when the scraper configuration is not found.
	ErrScraperNotFound = &influxdb.Error{
		Msg:  "scraper target is not found",
		Code: influxdb.ENotFound,
	}

	// ErrInvalidScraperID is used when the service was provided
	// an invalid ID format.
	ErrInvalidScraperID = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "provided scraper target ID has invalid format",
	}

	// ErrInvalidScrapersBucketID is used when the service was provided
	// an invalid ID format.
	ErrInvalidScrapersBucketID = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "provided bucket ID has invalid format",
	}

	// ErrInvalidScrapersOrgID is used when the service was provided
	// an invalid ID format.
	ErrInvalidScrapersOrgID = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "provided organization ID has invalid format",
	}
)

// UnexpectedScrapersBucketError is used when the error comes from an internal system.
func UnexpectedScrapersBucketError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "unexpected error retrieving scrapers bucket",
		Err:  err,
		Op:   "kv/scraper",
	}
}

// CorruptScraperError is used when the config cannot be unmarshalled from the
// bytes stored in the kv.
func CorruptScraperError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("Unknown internal scraper data error; Err: %v", err),
		Op:   "kv/scraper",
	}
}

// ErrUnprocessableScraper is used when a scraper is not able to be converted to JSON.
func ErrUnprocessableScraper(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EUnprocessableEntity,
		Msg:  fmt.Sprintf("unable to convert scraper target into JSON; Err %v", err),
	}
}

// InternalScraperServiceError is used when the error comes from an
// internal system.
func InternalScraperServiceError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("Unknown internal scraper data error; Err: %v", err),
		Op:   "kv/scraper",
	}
}

var (
	scrapersBucket = []byte("scraperv2")
)

var _ influxdb.ScraperTargetStoreService = (*Service)(nil)

func (s *Service) scrapersBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket([]byte(scrapersBucket))
	if err != nil {
		return nil, UnexpectedScrapersBucketError(err)
	}

	return b, nil
}

// ListTargets will list all scrape targets.
func (s *Service) ListTargets(ctx context.Context, filter influxdb.ScraperTargetFilter) ([]influxdb.ScraperTarget, error) {
	if filter.Org != nil {
		org, err := s.orgs.FindOrganization(ctx, influxdb.OrganizationFilter{
			Name: filter.Org,
		})
		if err != nil {
			return nil, err
		}

		filter.OrgID = &org.ID
	}

	targets := []influxdb.ScraperTarget{}
	err := s.kv.View(ctx, func(tx Tx) error {
		var err error
		targets, err = s.listTargets(ctx, tx, filter)
		return err
	})
	return targets, err
}

func (s *Service) listTargets(ctx context.Context, tx Tx, filter influxdb.ScraperTargetFilter) ([]influxdb.ScraperTarget, error) {
	targets := []influxdb.ScraperTarget{}
	bucket, err := s.scrapersBucket(tx)
	if err != nil {
		return nil, err
	}

	cur, err := bucket.ForwardCursor(nil)
	if err != nil {
		return nil, UnexpectedScrapersBucketError(err)
	}

	for k, v := cur.Next(); k != nil; k, v = cur.Next() {
		target, err := unmarshalScraper(v)
		if err != nil {
			return nil, err
		}
		if filter.IDs != nil {
			if _, ok := filter.IDs[target.ID]; !ok {
				continue
			}
		}
		if filter.Name != nil && target.Name != *filter.Name {
			continue
		}

		if filter.OrgID != nil && target.OrgID != *filter.OrgID {
			continue
		}

		targets = append(targets, *target)
	}
	return targets, nil
}

// AddTarget add a new scraper target into storage.
func (s *Service) AddTarget(ctx context.Context, target *influxdb.ScraperTarget, userID influxdb.ID) (err error) {
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.addTarget(ctx, tx, target, userID)
	})
}

func (s *Service) addTarget(ctx context.Context, tx Tx, target *influxdb.ScraperTarget, userID influxdb.ID) error {
	if !target.OrgID.Valid() {
		return ErrInvalidScrapersOrgID
	}

	if !target.BucketID.Valid() {
		return ErrInvalidScrapersBucketID
	}

	target.ID = s.IDGenerator.ID()
	if err := s.putTarget(ctx, tx, target); err != nil {
		return err
	}

	return nil
}

// RemoveTarget removes a scraper target from the bucket.
func (s *Service) RemoveTarget(ctx context.Context, id influxdb.ID) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.removeTarget(ctx, tx, id)
	})
}

func (s *Service) removeTarget(ctx context.Context, tx Tx, id influxdb.ID) error {
	_, pe := s.findTargetByID(ctx, tx, id)
	if pe != nil {
		return pe
	}
	encID, err := id.Encode()
	if err != nil {
		return ErrInvalidScraperID
	}

	bucket, err := s.scrapersBucket(tx)
	if err != nil {
		return err
	}

	_, err = bucket.Get(encID)
	if IsNotFound(err) {
		return ErrScraperNotFound
	}
	if err != nil {
		return InternalScraperServiceError(err)
	}

	if err := bucket.Delete(encID); err != nil {
		return InternalScraperServiceError(err)
	}

	return nil
}

// UpdateTarget updates a scraper target.
func (s *Service) UpdateTarget(ctx context.Context, update *influxdb.ScraperTarget, userID influxdb.ID) (*influxdb.ScraperTarget, error) {
	var target *influxdb.ScraperTarget
	err := s.kv.Update(ctx, func(tx Tx) error {
		var err error
		target, err = s.updateTarget(ctx, tx, update, userID)
		return err
	})

	return target, err
}

func (s *Service) updateTarget(ctx context.Context, tx Tx, update *influxdb.ScraperTarget, userID influxdb.ID) (*influxdb.ScraperTarget, error) {
	if !update.ID.Valid() {
		return nil, ErrInvalidScraperID
	}

	target, err := s.findTargetByID(ctx, tx, update.ID)
	if err != nil {
		return nil, err
	}

	// If the bucket or org are invalid, just use the ids from the original.
	if !update.BucketID.Valid() {
		update.BucketID = target.BucketID
	}
	if !update.OrgID.Valid() {
		update.OrgID = target.OrgID
	}
	target = update
	return target, s.putTarget(ctx, tx, target)
}

// GetTargetByID retrieves a scraper target by id.
func (s *Service) GetTargetByID(ctx context.Context, id influxdb.ID) (*influxdb.ScraperTarget, error) {
	var target *influxdb.ScraperTarget
	err := s.kv.View(ctx, func(tx Tx) error {
		var err error
		target, err = s.findTargetByID(ctx, tx, id)
		return err
	})

	return target, err
}

func (s *Service) findTargetByID(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.ScraperTarget, error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, ErrInvalidScraperID
	}

	bucket, err := s.scrapersBucket(tx)
	if err != nil {
		return nil, err
	}

	v, err := bucket.Get(encID)
	if IsNotFound(err) {
		return nil, ErrScraperNotFound
	}
	if err != nil {
		return nil, InternalScraperServiceError(err)
	}

	target, err := unmarshalScraper(v)
	if err != nil {
		return nil, err
	}

	return target, nil
}

// PutTarget will put a scraper target without setting an ID.
func (s *Service) PutTarget(ctx context.Context, target *influxdb.ScraperTarget) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.putTarget(ctx, tx, target)
	})
}

func (s *Service) putTarget(ctx context.Context, tx Tx, target *influxdb.ScraperTarget) error {
	v, err := marshalScraper(target)
	if err != nil {
		return ErrUnprocessableScraper(err)
	}

	encID, err := target.ID.Encode()
	if err != nil {
		return ErrInvalidScraperID
	}

	bucket, err := s.scrapersBucket(tx)
	if err != nil {
		return err
	}

	if err := bucket.Put(encID, v); err != nil {
		return UnexpectedScrapersBucketError(err)
	}

	return nil
}

// unmarshalScraper turns the stored byte slice in the kv into a *influxdb.ScraperTarget.
func unmarshalScraper(v []byte) (*influxdb.ScraperTarget, error) {
	s := &influxdb.ScraperTarget{}
	if err := json.Unmarshal(v, s); err != nil {
		return nil, CorruptScraperError(err)
	}
	return s, nil
}

func marshalScraper(sc *influxdb.ScraperTarget) ([]byte, error) {
	v, err := json.Marshal(sc)
	if err != nil {
		return nil, ErrUnprocessableScraper(err)
	}
	return v, nil
}
