package inmem

import (
	"context"
	"time"

	platform "github.com/influxdata/influxdb"
)

var (
	sessionBucket = []byte("sessionsv1")
)

// RenewSession extends the expire time to newExpiration.
func (s *Service) RenewSession(ctx context.Context, session *platform.Session, newExpiration time.Time) error {
	if session == nil {
		return &platform.Error{
			Msg: "session is nil",
		}
	}
	session.ExpiresAt = newExpiration
	return s.PutSession(ctx, session)
}

// FindSession retrieves the session found at the provided key.
func (s *Service) FindSession(ctx context.Context, key string) (*platform.Session, error) {
	result, found := s.sessionKV.Load(key)
	if !found {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  platform.ErrSessionNotFound,
		}
	}

	sess := new(platform.Session)
	*sess = result.(platform.Session)

	// TODO(desa): these values should be cached so it's not so expensive to lookup each time.
	f := platform.UserResourceMappingFilter{UserID: sess.UserID}
	mappings, _, err := s.FindUserResourceMappings(ctx, f)
	if err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	ps := make([]platform.Permission, 0, len(mappings))
	for _, m := range mappings {
		p, err := m.ToPermissions()
		if err != nil {
			return nil, &platform.Error{
				Err: err,
			}
		}

		ps = append(ps, p...)
	}
	ps = append(ps, platform.MePermissions(sess.UserID)...)
	sess.Permissions = ps
	return sess, nil
}

func (s *Service) PutSession(ctx context.Context, sess *platform.Session) error {
	s.sessionKV.Store(sess.Key, *sess)
	return nil
}

// ExpireSession expires the session at the provided key.
func (s *Service) ExpireSession(ctx context.Context, key string) error {
	return nil
}

// CreateSession creates a session for a user with the users maximal privileges.
func (s *Service) CreateSession(ctx context.Context, user string) (*platform.Session, error) {
	u, pe := s.findUserByName(ctx, user)
	if pe != nil {
		return nil, &platform.Error{
			Err: pe,
		}
	}

	sess := &platform.Session{}
	sess.ID = s.IDGenerator.ID()
	k, err := s.TokenGenerator.Token()
	if err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}
	sess.Key = k
	sess.UserID = u.ID
	sess.CreatedAt = time.Now()
	// TODO(desa): make this configurable
	sess.ExpiresAt = sess.CreatedAt.Add(time.Hour)
	// TODO(desa): not totally sure what to do here. Possibly we should have a maximal privilege permission.
	sess.Permissions = []platform.Permission{}

	if err := s.PutSession(ctx, sess); err != nil {
		return nil, err
	}

	return sess, nil
}
