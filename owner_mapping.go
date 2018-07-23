package platform

import (
	"context"
	"errors"
)

// OwnerMappingService provides a mapping between resources and their owners
type OwnerMappingService interface {
	CreateOwnerMapping(ctx context.Context, m *OwnerMapping) error
	DeleteOwnerMapping(ctx context.Context, resourceID ID, owner Owner) error
}

// OwnerMapping represents a mapping of a resource to its owner
type OwnerMapping struct {
	ResourceID ID    `json:"resource_id"`
	Owner      Owner `json:"owner_id"`
}

// Validate reports any validation errors for the mapping.
func (m OwnerMapping) Validate() error {
	if len(m.ResourceID) == 0 {
		return errors.New("ResourceID is required")
	}
	if len(m.Owner.ID) == 0 {
		return errors.New("An Owner with an ID is required")
	}
	return nil
}

// OwnerMappingFilter represents a set of filters that restrict the returned results.
type OwnerMappingFilter struct {
	ResourceID ID
	Owner      *Owner
}
