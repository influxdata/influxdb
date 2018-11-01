package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
)

const (
	sessionOneID = "020f755c3c082000"
	sessionTwoID = "020f755c3c082001"
)

var sessionCmpOptions = cmp.Options{
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
	cmpopts.IgnoreFields(platform.Session{}, "CreatedAt", "ExpiresAt"),
	cmpopts.EquateEmpty(),
}

// SessionFields will include the IDGenerator, TokenGenerator, Sessions, and Users
type SessionFields struct {
	IDGenerator    platform.IDGenerator
	TokenGenerator platform.TokenGenerator
	Sessions       []*platform.Session
	Users          []*platform.User
}

// SessionService tests all the service functions.
func SessionService(
	init func(SessionFields, *testing.T) (platform.SessionService, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(SessionFields, *testing.T) (platform.SessionService, func()),
			t *testing.T)
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// CreateSession testing
func CreateSession(
	init func(SessionFields, *testing.T) (platform.SessionService, func()),
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
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			session, err := s.CreateSession(ctx, tt.args.user)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(session, tt.wants.session, sessionCmpOptions...); diff != "" {
				t.Errorf("sessions are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindSession testing
func FindSession(
	init func(SessionFields, *testing.T) (platform.SessionService, func()),
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
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			session, err := s.FindSession(ctx, tt.args.key)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(session, tt.wants.session, sessionCmpOptions...); diff != "" {
				t.Errorf("session is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// ExpireSession testing
func ExpireSession(
	init func(SessionFields, *testing.T) (platform.SessionService, func()),
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
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			err := s.ExpireSession(ctx, tt.args.key)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			session, err := s.FindSession(ctx, tt.args.key)
			if err == nil {
				t.Errorf("expected session to be expired got %v", err)
			}

			if diff := cmp.Diff(session, tt.wants.session, sessionCmpOptions...); diff != "" {
				t.Errorf("session is different -got/+want\ndiff %s", diff)
			}
		})
	}
}
