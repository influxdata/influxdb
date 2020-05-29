package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/v2"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
)

const (
	sessionOneID = "020f755c3c082000"
	sessionTwoID = "020f755c3c082001"
)

var sessionCmpOptions = sessionCompareOptions("CreatedAt", "ExpiresAt", "Permissions")

func sessionCompareOptions(ignore ...string) cmp.Options {
	return cmp.Options{
		cmp.Comparer(func(x, y []byte) bool {
			return bytes.Equal(x, y)
		}),
		cmp.Transformer("Sort", func(in []*platform.Session) []*platform.Session {
			out := append([]*platform.Session(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() > out[j].ID.String()
			})
			return out
		}),
		cmpopts.IgnoreFields(platform.Session{}, ignore...),
		cmpopts.EquateEmpty(),
	}
}

// SessionFields will include the IDGenerator, TokenGenerator, Sessions, and Users
type SessionFields struct {
	IDGenerator    platform.IDGenerator
	TokenGenerator platform.TokenGenerator
	Sessions       []*platform.Session
	Users          []*platform.User
}

type sessionServiceFunc func(
	init func(SessionFields, *testing.T) (platform.SessionService, string, func()),
	t *testing.T,
)

// SessionService tests all the service functions.
func SessionService(
	init func(SessionFields, *testing.T) (platform.SessionService, string, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   sessionServiceFunc
	}{
		{
			name: "CreateSession",
			fn:   CreateSession,
		},
		{
			name: "FindSession",
			fn:   FindSession,
		},
		{
			name: "ExpireSession",
			fn:   ExpireSession,
		},
		{
			name: "RenewSession",
			fn:   RenewSession,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// CreateSession testing
func CreateSession(
	init func(SessionFields, *testing.T) (platform.SessionService, string, func()),
	t *testing.T,
) {
	type args struct {
		user string
	}
	type wants struct {
		err     error
		session *platform.Session
	}

	tests := []struct {
		name   string
		fields SessionFields
		args   args
		wants  wants
	}{
		{
			name: "create sessions with empty set",
			fields: SessionFields{
				IDGenerator:    mock.NewIDGenerator(sessionTwoID, t),
				TokenGenerator: mock.NewTokenGenerator("abc123xyz", nil),
				Users: []*platform.User{
					{
						ID:   MustIDBase16(sessionOneID),
						Name: "user1",
					},
				},
			},
			args: args{
				user: "user1",
			},
			wants: wants{
				session: &platform.Session{
					ID:     MustIDBase16(sessionTwoID),
					UserID: MustIDBase16(sessionOneID),
					Key:    "abc123xyz",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			session, err := s.CreateSession(ctx, tt.args.user)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(session, tt.wants.session, sessionCmpOptions...); diff != "" {
				t.Errorf("sessions are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindSession testing
func FindSession(
	init func(SessionFields, *testing.T) (platform.SessionService, string, func()),
	t *testing.T,
) {
	type args struct {
		key string
	}
	type wants struct {
		err     error
		session *platform.Session
	}

	tests := []struct {
		name   string
		fields SessionFields
		args   args
		wants  wants
	}{
		{
			name: "basic find session",
			fields: SessionFields{
				IDGenerator:    mock.NewIDGenerator(sessionTwoID, t),
				TokenGenerator: mock.NewTokenGenerator("abc123xyz", nil),
				Sessions: []*platform.Session{
					{
						ID:        MustIDBase16(sessionOneID),
						UserID:    MustIDBase16(sessionTwoID),
						Key:       "abc123xyz",
						ExpiresAt: time.Date(2030, 9, 26, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key: "abc123xyz",
			},
			wants: wants{
				session: &platform.Session{
					ID:        MustIDBase16(sessionOneID),
					UserID:    MustIDBase16(sessionTwoID),
					Key:       "abc123xyz",
					ExpiresAt: time.Date(2030, 9, 26, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "look for not existing session",
			args: args{
				key: "abc123xyz",
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpFindSession,
					Msg:  platform.ErrSessionNotFound,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			session, err := s.FindSession(ctx, tt.args.key)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(session, tt.wants.session, sessionCmpOptions...); diff != "" {
				t.Errorf("session is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// ExpireSession testing
func ExpireSession(
	init func(SessionFields, *testing.T) (platform.SessionService, string, func()),
	t *testing.T,
) {
	type args struct {
		key string
	}
	type wants struct {
		err     error
		session *platform.Session
	}

	tests := []struct {
		name   string
		fields SessionFields
		args   args
		wants  wants
	}{
		{
			name: "basic find session",
			fields: SessionFields{
				IDGenerator:    mock.NewIDGenerator(sessionTwoID, t),
				TokenGenerator: mock.NewTokenGenerator("abc123xyz", nil),
				Sessions: []*platform.Session{
					{
						ID:        MustIDBase16(sessionOneID),
						UserID:    MustIDBase16(sessionTwoID),
						Key:       "abc123xyz",
						ExpiresAt: time.Date(2030, 9, 26, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key: "abc123xyz",
			},
			wants: wants{
				session: &platform.Session{
					ID:        MustIDBase16(sessionOneID),
					UserID:    MustIDBase16(sessionTwoID),
					Key:       "abc123xyz",
					ExpiresAt: time.Date(2030, 9, 26, 0, 0, 0, 0, time.UTC),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			err := s.ExpireSession(ctx, tt.args.key)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			session, err := s.FindSession(ctx, tt.args.key)
			if err.Error() != influxdb.ErrSessionExpired && err.Error() != influxdb.ErrSessionNotFound {
				t.Errorf("expected session to be expired got %v", err)
			}

			if session != nil {
				t.Errorf("expected a nil session but got: %v", session)
			}
		})
	}
}

// RenewSession testing
func RenewSession(
	init func(SessionFields, *testing.T) (platform.SessionService, string, func()),
	t *testing.T,
) {
	type args struct {
		session  *platform.Session
		key      string
		expireAt time.Time
	}

	type wants struct {
		err     error
		session *platform.Session
	}

	tests := []struct {
		name   string
		fields SessionFields
		args   args
		wants  wants
	}{
		{
			name: "basic renew session",
			fields: SessionFields{
				IDGenerator:    mock.NewIDGenerator(sessionTwoID, t),
				TokenGenerator: mock.NewTokenGenerator("abc123xyz", nil),
				Sessions: []*platform.Session{
					{
						ID:        MustIDBase16(sessionOneID),
						UserID:    MustIDBase16(sessionTwoID),
						Key:       "abc123xyz",
						ExpiresAt: time.Date(2030, 9, 26, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				session: &platform.Session{
					ID:        MustIDBase16(sessionOneID),
					UserID:    MustIDBase16(sessionTwoID),
					Key:       "abc123xyz",
					ExpiresAt: time.Date(2030, 9, 26, 0, 0, 0, 0, time.UTC),
				},
				key:      "abc123xyz",
				expireAt: time.Date(2031, 9, 26, 0, 0, 10, 0, time.UTC),
			},
			wants: wants{
				session: &platform.Session{
					ID:        MustIDBase16(sessionOneID),
					UserID:    MustIDBase16(sessionTwoID),
					Key:       "abc123xyz",
					ExpiresAt: time.Date(2031, 9, 26, 0, 0, 10, 0, time.UTC),
				},
			},
		},
		{
			name: "renew session with an earlier time than existing expiration",
			fields: SessionFields{
				IDGenerator:    mock.NewIDGenerator(sessionTwoID, t),
				TokenGenerator: mock.NewTokenGenerator("abc123xyz", nil),
				Sessions: []*platform.Session{
					{
						ID:        MustIDBase16(sessionOneID),
						UserID:    MustIDBase16(sessionTwoID),
						Key:       "abc123xyz",
						ExpiresAt: time.Date(2031, 9, 26, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				session: &platform.Session{
					ID:        MustIDBase16(sessionOneID),
					UserID:    MustIDBase16(sessionTwoID),
					Key:       "abc123xyz",
					ExpiresAt: time.Date(2031, 9, 26, 0, 0, 0, 0, time.UTC),
				},
				key:      "abc123xyz",
				expireAt: time.Date(2030, 9, 26, 0, 0, 0, 0, time.UTC),
			},
			wants: wants{
				session: &platform.Session{
					ID:        MustIDBase16(sessionOneID),
					UserID:    MustIDBase16(sessionTwoID),
					Key:       "abc123xyz",
					ExpiresAt: time.Date(2031, 9, 26, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "renew nil session",
			fields: SessionFields{
				IDGenerator:    mock.NewIDGenerator(sessionTwoID, t),
				TokenGenerator: mock.NewTokenGenerator("abc123xyz", nil),
				Sessions: []*platform.Session{
					{
						ID:        MustIDBase16(sessionOneID),
						UserID:    MustIDBase16(sessionTwoID),
						Key:       "abc123xyz",
						ExpiresAt: time.Date(2030, 9, 26, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			args: args{
				key:      "abc123xyz",
				expireAt: time.Date(2031, 9, 26, 0, 0, 10, 0, time.UTC),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.EInternal,
					Msg:  "session is nil",
					Op:   platform.OpRenewSession,
				},
				session: &platform.Session{
					ID:        MustIDBase16(sessionOneID),
					UserID:    MustIDBase16(sessionTwoID),
					Key:       "abc123xyz",
					ExpiresAt: time.Date(2030, 9, 26, 0, 0, 0, 0, time.UTC),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			err := s.RenewSession(ctx, tt.args.session, tt.args.expireAt)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			session, err := s.FindSession(ctx, tt.args.key)
			if err != nil {
				t.Errorf("err in find session %v", err)
			}

			cmpOptions := sessionCompareOptions("CreatedAt", "Permissions")
			if diff := cmp.Diff(session, tt.wants.session, cmpOptions...); diff != "" {
				t.Errorf("session is different -got/+want\ndiff %s", diff)
			}
		})
	}
}
