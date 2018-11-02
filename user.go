package platform

import "context"

// User is a user. 🎉
type User struct {
	ID   ID     `json:"id,omitempty"`
	Name string `json:"name"`
}

// UserService represents a service for managing user data.
type UserService interface {

	// Returns a single user by ID.
	FindUserByID(ctx context.Context, id ID) (*User, error)

	// Returns the first user that matches filter.
	FindUser(ctx context.Context, filter UserFilter) (*User, error)

	// Returns a list of users that match filter and the total count of matching users.
	// Additional options provide pagination & sorting.
	FindUsers(ctx context.Context, filter UserFilter, opt ...FindOptions) ([]*User, int, error)

	// Creates a new user and sets u.ID with the new identifier.
	CreateUser(ctx context.Context, u *User) error

	// Updates a single user with changeset.
	// Returns the new user state after update.
	UpdateUser(ctx context.Context, id ID, upd UserUpdate) (*User, error)

	// Removes a user by ID.
	DeleteUser(ctx context.Context, id ID) error
}

// BasicAuthService is the service for managing basic auth.
type BasicAuthService interface {
	SetPassword(ctx context.Context, name string, password string) error
	ComparePassword(ctx context.Context, name string, password string) error
	CompareAndSetPassword(ctx context.Context, name string, old string, new string) error
}

// UserUpdate represents updates to a user.
// Only fields which are set are updated.
type UserUpdate struct {
	Name *string `json:"name"`
}

// UserFilter represents a set of filter that restrict the returned results.
type UserFilter struct {
	ID   *ID
	Name *string
}
