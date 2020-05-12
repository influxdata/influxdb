package session

import (
	"context"
	"encoding/json"
	"time"

	"github.com/influxdata/influxdb/v2"
)

type Store interface {
	Set(key, val string, expireAt time.Time) error
	Get(key string) (string, error)
	Delete(key string) error
	ExpireAt(key string, expireAt time.Time) error
}

var storePrefix = "sessionsv1/"
var storeIndex = "sessionsindexv1/"

// Storage is a store translation layer between the data storage unit and the
// service layer.
type Storage struct {
	store Store
}

// NewStorage creates a new storage system
func NewStorage(s Store) *Storage {
	return &Storage{s}
}

// FindSessionByKey use a given key to retrieve the stored session
func (s *Storage) FindSessionByKey(ctx context.Context, key string) (*influxdb.Session, error) {
	val, err := s.store.Get(sessionIndexKey(key))
	if err != nil {
		return nil, err
	}

	if val == "" {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  influxdb.ErrSessionNotFound,
		}
	}

	id, err := influxdb.IDFromString(val)
	if err != nil {
		return nil, err
	}
	return s.FindSessionByID(ctx, *id)
}

// FindSessionByID use a provided id to retrieve the stored session
func (s *Storage) FindSessionByID(ctx context.Context, id influxdb.ID) (*influxdb.Session, error) {
	val, err := s.store.Get(storePrefix + id.String())
	if err != nil {
		return nil, err
	}
	if val == "" {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  influxdb.ErrSessionNotFound,
		}
	}

	session := &influxdb.Session{}
	return session, json.Unmarshal([]byte(val), session)
}

// CreateSession creates a new session
func (s *Storage) CreateSession(ctx context.Context, session *influxdb.Session) error {
	// create session
	sessionBytes, err := json.Marshal(session)
	if err != nil {
		return err
	}

	// use a minute time just so the session will expire if we fail to set the expiration later
	sessionID := sessionID(session.ID)
	if err := s.store.Set(sessionID, string(sessionBytes), session.ExpiresAt); err != nil {
		return err
	}

	// create index
	indexKey := sessionIndexKey(session.Key)
	if err := s.store.Set(indexKey, session.ID.String(), session.ExpiresAt); err != nil {
		return err
	}

	return nil
}

// RefreshSession updates the expiration time of a session.
func (s *Storage) RefreshSession(ctx context.Context, id influxdb.ID, expireAt time.Time) error {
	session, err := s.FindSessionByID(ctx, id)
	if err != nil {
		return err
	}

	if expireAt.Before(session.ExpiresAt) {
		// no need to recreate the session if we aren't extending the expiration
		return nil
	}

	session.ExpiresAt = expireAt
	return s.CreateSession(ctx, session)
}

// DeleteSession removes the session and index from storage
func (s *Storage) DeleteSession(ctx context.Context, id influxdb.ID) error {
	session, err := s.FindSessionByID(ctx, id)
	if err != nil {
		return err
	}
	if session == nil {
		return nil
	}

	if err := s.store.Delete(sessionID(session.ID)); err != nil {
		return err
	}

	if err := s.store.Delete(sessionIndexKey(session.Key)); err != nil {
		return err
	}

	return nil
}

func sessionID(id influxdb.ID) string {
	return storePrefix + id.String()
}

func sessionIndexKey(key string) string {
	return storeIndex + key
}
