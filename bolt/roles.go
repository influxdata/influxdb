package bolt

import (
	"sync"

	"github.com/influxdata/chronograf"
)

// Ensure RolesStore implements chronograf.RolesStore.
var _ chronograf.RolesStore = &RolesStore{}

// RolesStore uses bolt to store and retrieve roles
type RolesStore struct {
	mu         sync.RWMutex
	roles      map[string]chronograf.Role
	usersStore *UsersStore
}
