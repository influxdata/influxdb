package bolt_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initUserService(f platformtesting.UserFields, t *testing.T) (platform.UserService, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, u := range f.Users {
		if err := c.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}
	return c, func() {
		defer closeFn()
		for _, u := range f.Users {
			if err := c.DeleteUser(ctx, u.ID); err != nil {
				t.Logf("failed to remove users: %v", err)
			}
		}
	}
}

func TestUserService_CreateUser(t *testing.T) {
	platformtesting.CreateUser(initUserService, t)
}

func TestUserService_FindUserByID(t *testing.T) {
	platformtesting.FindUserByID(initUserService, t)
}

func TestUserService_FindUsers(t *testing.T) {
	platformtesting.FindUsers(initUserService, t)
}

func TestUserService_DeleteUser(t *testing.T) {
	platformtesting.DeleteUser(initUserService, t)
}

func TestUserService_FindUser(t *testing.T) {
	platformtesting.FindUser(initUserService, t)
}

func TestUserService_UpdateUser(t *testing.T) {
	platformtesting.UpdateUser(initUserService, t)
}

func TestBasicAuth(t *testing.T) {
	type fields struct {
		users []*platform.User
	}
	type args struct {
		name            string
		user            string
		setPassword     string
		comparePassword string
	}
	type wants struct {
		setErr     error
		compareErr error
	}
	tests := []struct {
		fields fields
		args   args
		wants  wants
	}{
		{
			fields: fields{
				users: []*platform.User{
					{
						Name: "user1",
						ID:   platformtesting.MustIDBase16("aaaaaaaaaaaaaaaa"),
					},
				},
			},
			args: args{
				name:            "happy path",
				user:            "user1",
				setPassword:     "hello",
				comparePassword: "hello",
			},
			wants: wants{},
		},
		{
			fields: fields{
				users: []*platform.User{
					{
						Name: "user1",
						ID:   platformtesting.MustIDBase16("aaaaaaaaaaaaaaaa"),
					},
				},
			},
			args: args{
				name:            "happy path dont match",
				user:            "user1",
				setPassword:     "hello",
				comparePassword: "world",
			},
			wants: wants{
				compareErr: fmt.Errorf("crypto/bcrypt: hashedPassword is not the hash of the given password"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.args.name, func(t *testing.T) {
			c, closeFn, err := NewTestClient()
			if err != nil {
				t.Fatalf("failed to create new bolt client: %v", err)
			}
			defer closeFn()
			ctx := context.Background()

			for _, user := range tt.fields.users {
				if err := c.PutUser(ctx, user); err != nil {
					t.Fatal(err)
					return
				}
			}

			err = c.SetPassword(ctx, tt.args.user, tt.args.setPassword)

			if (err != nil && tt.wants.setErr == nil) || (err == nil && tt.wants.setErr != nil) {
				t.Fatalf("expected SetPassword error %v got %v", tt.wants.setErr, err)
				return
			}

			if err != nil {
				if want, got := tt.wants.setErr.Error(), err.Error(); want != got {
					t.Fatalf("expected SetPassword error %v got %v", want, got)
				}
				return
			}

			err = c.ComparePassword(ctx, tt.args.user, tt.args.comparePassword)

			if (err != nil && tt.wants.compareErr == nil) || (err == nil && tt.wants.compareErr != nil) {
				t.Fatalf("expected ComparePassword error %v got %v", tt.wants.compareErr, err)
				return
			}

			if err != nil {
				if want, got := tt.wants.compareErr.Error(), err.Error(); want != got {
					t.Fatalf("expected ComparePassword error %v got %v", tt.wants.compareErr, err)
				}
				return
			}

		})
	}

}

func TestBasicAuth_CompareAndSet(t *testing.T) {
	type fields struct {
		users []*platform.User
	}
	type args struct {
		name string
		user string
		old  string
		new  string
	}
	type wants struct {
		err error
	}
	tests := []struct {
		fields fields
		args   args
		wants  wants
	}{
		{
			fields: fields{
				users: []*platform.User{
					{
						Name: "user1",
						ID:   platformtesting.MustIDBase16("aaaaaaaaaaaaaaaa"),
					},
				},
			},
			args: args{
				name: "happy path",
				user: "user1",
				old:  "hello",
				new:  "hello",
			},
			wants: wants{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.args.name, func(t *testing.T) {
			c, closeFn, err := NewTestClient()
			if err != nil {
				t.Fatalf("failed to create new bolt client: %v", err)
			}
			defer closeFn()
			ctx := context.Background()

			for _, user := range tt.fields.users {
				if err := c.PutUser(ctx, user); err != nil {
					t.Fatal(err)
					return
				}
			}

			if err := c.SetPassword(ctx, tt.args.user, tt.args.old); err != nil {
				t.Fatalf("unexpected error %v", err)
				return
			}

			err = c.CompareAndSetPassword(ctx, tt.args.user, tt.args.old, tt.args.new)

			if (err != nil && tt.wants.err == nil) || (err == nil && tt.wants.err != nil) {
				t.Fatalf("expected CompareAndSetPassword error %v got %v", tt.wants.err, err)
				return
			}

			if err != nil {
				if want, got := tt.wants.err.Error(), err.Error(); want != got {
					t.Fatalf("expected CompareAndSetPassword error %v got %v", tt.wants.err, err)
				}
				return
			}

		})
	}

}
