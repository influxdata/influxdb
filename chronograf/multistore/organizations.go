package multistore

import (
	"context"
	"fmt"
	"strings"

	"github.com/influxdata/chronograf"
)

// Ensure OrganizationsStore implements chronograf.OrganizationsStore.
var _ chronograf.OrganizationsStore = &OrganizationsStore{}

// OrganizationsStore implements the chronograf.OrganizationsStore interface, and
// delegates to all contained OrganizationsStores
type OrganizationsStore struct {
	Stores []chronograf.OrganizationsStore
}

// All concatenates the Organizations of all contained Stores
func (multi *OrganizationsStore) All(ctx context.Context) ([]chronograf.Organization, error) {
	all := []chronograf.Organization{}
	orgSet := map[string]struct{}{}

	ok := false
	var err error
	for _, store := range multi.Stores {
		var orgs []chronograf.Organization
		orgs, err = store.All(ctx)
		if err != nil {
			// If this Store is unable to return an array of orgs, skip to the
			// next Store.
			continue
		}
		ok = true // We've received a response from at least one Store
		for _, org := range orgs {
			// Enforce that the org has a unique ID
			// If the ID has been seen before, ignore the org
			if _, okay := orgSet[org.ID]; !okay { // We have a new org
				orgSet[org.ID] = struct{}{} // We just care that the ID is unique
				all = append(all, org)
			}
		}
	}
	if !ok {
		return nil, err
	}
	return all, nil
}

// Add the org to the first responsive Store
func (multi *OrganizationsStore) Add(ctx context.Context, org *chronograf.Organization) (*chronograf.Organization, error) {
	errors := []string{}
	for _, store := range multi.Stores {
		var o *chronograf.Organization
		o, err := store.Add(ctx, org)
		if err == nil {
			return o, nil
		}
		errors = append(errors, err.Error())
	}
	return nil, fmt.Errorf("Unknown error while adding organization: %s", strings.Join(errors, " "))
}

// Delete delegates to all Stores, returns success if one Store is successful
func (multi *OrganizationsStore) Delete(ctx context.Context, org *chronograf.Organization) error {
	errors := []string{}
	for _, store := range multi.Stores {
		err := store.Delete(ctx, org)
		if err == nil {
			return nil
		}
		errors = append(errors, err.Error())
	}
	return fmt.Errorf("Unknown error while deleting organization: %s", strings.Join(errors, " "))
}

// Get finds the Organization by id among all contained Stores
func (multi *OrganizationsStore) Get(ctx context.Context, query chronograf.OrganizationQuery) (*chronograf.Organization, error) {
	var err error
	for _, store := range multi.Stores {
		var o *chronograf.Organization
		o, err = store.Get(ctx, query)
		if err == nil {
			return o, nil
		}
	}
	return nil, chronograf.ErrOrganizationNotFound
}

// Update the first responsive Store
func (multi *OrganizationsStore) Update(ctx context.Context, org *chronograf.Organization) error {
	errors := []string{}
	for _, store := range multi.Stores {
		err := store.Update(ctx, org)
		if err == nil {
			return nil
		}
		errors = append(errors, err.Error())
	}
	return fmt.Errorf("Unknown error while updating organization: %s", strings.Join(errors, " "))
}

// CreateDefault makes a default organization in the first responsive Store
func (multi *OrganizationsStore) CreateDefault(ctx context.Context) error {
	errors := []string{}
	for _, store := range multi.Stores {
		err := store.CreateDefault(ctx)
		if err == nil {
			return nil
		}
		errors = append(errors, err.Error())
	}
	return fmt.Errorf("Unknown error while creating default organization: %s", strings.Join(errors, " "))
}

// DefaultOrganization returns the first successful DefaultOrganization
func (multi *OrganizationsStore) DefaultOrganization(ctx context.Context) (*chronograf.Organization, error) {
	errors := []string{}
	for _, store := range multi.Stores {
		org, err := store.DefaultOrganization(ctx)
		if err == nil {
			return org, nil
		}
		errors = append(errors, err.Error())
	}
	return nil, fmt.Errorf("Unknown error while getting default organization: %s", strings.Join(errors, " "))

}
