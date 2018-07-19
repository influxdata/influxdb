package filestore

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/influxdata/platform/chronograf"
)

// OrgExt is the the file extension searched for in the directory for org files
const OrgExt = ".org"

var _ chronograf.OrganizationsStore = &Organizations{}

// Organizations are JSON orgs stored in the filesystem
type Organizations struct {
	Dir     string                                      // Dir is the directory containing the orgs.
	Load    func(string, interface{}) error             // Load loads string name and org passed in as interface
	ReadDir func(dirname string) ([]os.FileInfo, error) // ReadDir reads the directory named by dirname and returns a list of directory entries sorted by filename.
	Logger  chronograf.Logger
}

// NewOrganizations constructs a org store wrapping a file system directory
func NewOrganizations(dir string, logger chronograf.Logger) chronograf.OrganizationsStore {
	return &Organizations{
		Dir:     dir,
		Load:    load,
		ReadDir: ioutil.ReadDir,
		Logger:  logger,
	}
}

func orgFile(dir string, org chronograf.Organization) string {
	base := fmt.Sprintf("%s%s", org.Name, OrgExt)
	return path.Join(dir, base)
}

// All returns all orgs from the directory
func (o *Organizations) All(ctx context.Context) ([]chronograf.Organization, error) {
	files, err := o.ReadDir(o.Dir)
	if err != nil {
		return nil, err
	}

	orgs := []chronograf.Organization{}
	for _, file := range files {
		if path.Ext(file.Name()) != OrgExt {
			continue
		}
		var org chronograf.Organization
		if err := o.Load(path.Join(o.Dir, file.Name()), &org); err != nil {
			continue // We want to load all files we can.
		} else {
			orgs = append(orgs, org)
		}
	}
	return orgs, nil
}

// Get returns a org file from the org directory
func (o *Organizations) Get(ctx context.Context, query chronograf.OrganizationQuery) (*chronograf.Organization, error) {
	org, _, err := o.findOrg(query)
	return org, err
}

// Add is not allowed for the filesystem organization store
func (o *Organizations) Add(ctx context.Context, org *chronograf.Organization) (*chronograf.Organization, error) {
	return nil, fmt.Errorf("unable to add organizations to the filesystem")
}

// Delete is not allowed for the filesystem organization store
func (o *Organizations) Delete(ctx context.Context, org *chronograf.Organization) error {
	return fmt.Errorf("unable to delete an organization from the filesystem")
}

// Update is not allowed for the filesystem organization store
func (o *Organizations) Update(ctx context.Context, org *chronograf.Organization) error {
	return fmt.Errorf("unable to update organizations on the filesystem")
}

// CreateDefault is not allowed for the filesystem organization store
func (o *Organizations) CreateDefault(ctx context.Context) error {
	return fmt.Errorf("unable to create default organizations on the filesystem")
}

// DefaultOrganization is not allowed for the filesystem organization store
func (o *Organizations) DefaultOrganization(ctx context.Context) (*chronograf.Organization, error) {
	return nil, fmt.Errorf("unable to get default organizations from the filestore")
}

// findOrg takes an OrganizationQuery and finds the associated filename
func (o *Organizations) findOrg(query chronograf.OrganizationQuery) (*chronograf.Organization, string, error) {
	// Because the entire org information is not known at this point, we need
	// to try to find the name of the file through matching the ID or name in the org
	// content with the ID passed.
	files, err := o.ReadDir(o.Dir)
	if err != nil {
		return nil, "", err
	}

	for _, f := range files {
		if path.Ext(f.Name()) != OrgExt {
			continue
		}
		file := path.Join(o.Dir, f.Name())
		var org chronograf.Organization
		if err := o.Load(file, &org); err != nil {
			return nil, "", err
		}
		if query.ID != nil && org.ID == *query.ID {
			return &org, file, nil
		}
		if query.Name != nil && org.Name == *query.Name {
			return &org, file, nil
		}
	}

	return nil, "", chronograf.ErrOrganizationNotFound
}
