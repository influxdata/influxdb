package kv

import (
	"context"
	"encoding/json"
	"time"

	"github.com/influxdata/influxdb"
)

var (
	sessionBucket = []byte("sessionsv1")
)

var _ influxdb.SessionService = (*Service)(nil)

func (s *Service) initializeSessions(ctx context.Context, tx Tx) error {
	if _, err := tx.Bucket([]byte(sessionBucket)); err != nil {
		return err
	}
	return nil
}

// RenewSession extends the expire time to newExpiration.
func (s *Service) RenewSession(ctx context.Context, session *influxdb.Session, newExpiration time.Time) error {
	if session == nil {
		return &influxdb.Error{
			Msg: "session is nil",
		}
	}
	return s.kv.Update(ctx, func(tx Tx) error {
		session.ExpiresAt = newExpiration
		if err := s.putSession(ctx, tx, session); err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}
		return nil
	})
}

// FindSession retrieves the session found at the provided key.
func (s *Service) FindSession(ctx context.Context, key string) (*influxdb.Session, error) {
	var sess *influxdb.Session
	err := s.kv.View(ctx, func(tx Tx) error {
		s, err := s.findSession(ctx, tx, key)
		if err != nil {
			return err
		}

		sess = s
		return nil
	})

	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	if err := sess.Expired(); err != nil {
		// todo(leodido) > do we want to return session also if expired?
		return sess, &influxdb.Error{
			Err: err,
		}
	}
	return sess, nil
}

func (s *Service) findSession(ctx context.Context, tx Tx, key string) (*influxdb.Session, error) {
	b, err := tx.Bucket(sessionBucket)
	if err != nil {
		return nil, err
	}

	v, err := b.Get([]byte(key))
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  influxdb.ErrSessionNotFound,
		}
	}

	if err != nil {
		return nil, err
	}

	sn := &influxdb.Session{}
	if err := json.Unmarshal(v, sn); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	// TODO(desa): these values should be cached so it's not so expensive to lookup each time.
	f := influxdb.UserResourceMappingFilter{UserID: sn.UserID}
	mappings, err := s.findUserResourceMappings(ctx, tx, f)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	ps := make([]influxdb.Permission, 0, len(mappings))
	for _, m := range mappings {
		p, err := m.ToPermissions()
		if err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}

		ps = append(ps, p...)
	}
	ps = append(ps, influxdb.MePermissions(sn.UserID)...)
	sn.Permissions = ps
	return sn, nil
}

// PutSession puts the session at key.
func (s *Service) PutSession(ctx context.Context, sn *influxdb.Session) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		if err := s.putSession(ctx, tx, sn); err != nil {
			return err
		}
		return nil
	})
}

func (s *Service) putSession(ctx context.Context, tx Tx, sn *influxdb.Session) error {
	v, err := json.Marshal(sn)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	b, err := tx.Bucket(sessionBucket)
	if err != nil {
		return err
	}

	if err := b.Put([]byte(sn.Key), v); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	return nil
}

// ExpireSession expires the session at the provided key.
func (s *Service) ExpireSession(ctx context.Context, key string) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		sn, err := s.findSession(ctx, tx, key)
		if err != nil {
			return err
		}

		sn.ExpiresAt = time.Now()

		if err := s.putSession(ctx, tx, sn); err != nil {
			return err
		}
		return nil
	})
}

// CreateSession creates a session for a user with the users maximal privileges.
func (s *Service) CreateSession(ctx context.Context, user string) (*influxdb.Session, error) {
	var sess *influxdb.Session
	err := s.kv.Update(ctx, func(tx Tx) error {
		sn, err := s.createSession(ctx, tx, user)
		if err != nil {
			return err
		}

		sess = sn

		return nil
	})

	if err != nil {
		return nil, err
	}

	return sess, nil
}

func (s *Service) createSession(ctx context.Context, tx Tx, user string) (*influxdb.Session, error) {
	u, pe := s.findUserByName(ctx, tx, user)
	if pe != nil {
		return nil, pe
	}

	sn := &influxdb.Session{}
	sn.ID = s.IDGenerator.ID()
	k, err := s.TokenGenerator.Token()
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}
	sn.Key = k
	sn.UserID = u.ID
	sn.CreatedAt = time.Now()
	// TODO(desa): make this configurable
	sn.ExpiresAt = sn.CreatedAt.Add(time.Hour)
	// TODO(desa): not totally sure what to do here. Possibly we should have a maximal privilege permission.
	sn.Permissions = []influxdb.Permission{}

	if err := s.putSession(ctx, tx, sn); err != nil {
		return nil, err
	}

	return sn, nil
}
