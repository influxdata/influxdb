package influxdb

import (
	"context"
	"strconv"
	"strings"
	"unicode"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

// DBRPMappingServiceV2 provides CRUD to DBRPMappingV2s.
type DBRPMappingServiceV2 interface {
	// FindBy returns the dbrp mapping for the specified ID.
	// Requires orgID because every resource will be org-scoped.
	FindByID(ctx context.Context, orgID, id platform.ID) (*DBRPMappingV2, error)
	// FindMany returns a list of dbrp mappings that match filter and the total count of matching dbrp mappings.
	FindMany(ctx context.Context, dbrp DBRPMappingFilterV2, opts ...FindOptions) ([]*DBRPMappingV2, int, error)
	// Create creates a new dbrp mapping, if a different mapping exists an error is returned.
	Create(ctx context.Context, dbrp *DBRPMappingV2) error
	// Update a new dbrp mapping
	Update(ctx context.Context, dbrp *DBRPMappingV2) error
	// Delete removes a dbrp mapping.
	// Deleting a mapping that does not exists is not an error.
	// Requires orgID because every resource will be org-scoped.
	Delete(ctx context.Context, orgID, id platform.ID) error
}

// DBRPMappingV2 represents a mapping of a database and retention policy to an organization ID and bucket ID.
type DBRPMappingV2 struct {
	ID              platform.ID `json:"id"`
	Database        string      `json:"database"`
	RetentionPolicy string      `json:"retention_policy"`

	// Default indicates if this mapping is the default for the cluster and database.
	Default bool `json:"default"`

	OrganizationID platform.ID `json:"orgID"`
	BucketID       platform.ID `json:"bucketID"`
}

// Validate reports any validation errors for the mapping.
func (m DBRPMappingV2) Validate() error {
	if !validName(m.Database) {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "database must contain at least one character and only be letters, numbers, '_', '-', and '.'",
		}
	}
	if !validName(m.RetentionPolicy) {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "retentionPolicy must contain at least one character and only be letters, numbers, '_', '-', and '.'",
		}
	}
	if !m.OrganizationID.Valid() {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "organizationID is required",
		}
	}
	if !m.BucketID.Valid() {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "bucketID is required",
		}
	}
	return nil
}

// Equal checks if the two mappings are identical.
func (m *DBRPMappingV2) Equal(o *DBRPMappingV2) bool {
	if m == o {
		return true
	}
	if m == nil || o == nil {
		return false
	}
	return m.Database == o.Database &&
		m.RetentionPolicy == o.RetentionPolicy &&
		m.Default == o.Default &&
		m.OrganizationID.Valid() &&
		o.OrganizationID.Valid() &&
		m.BucketID.Valid() &&
		o.BucketID.Valid() &&
		o.ID.Valid() &&
		m.ID == o.ID &&
		m.OrganizationID == o.OrganizationID &&
		m.BucketID == o.BucketID
}

// DBRPMappingFilterV2 represents a set of filters that restrict the returned results.
type DBRPMappingFilterV2 struct {
	ID       *platform.ID
	OrgID    *platform.ID
	BucketID *platform.ID

	Database        *string
	RetentionPolicy *string
	Default         *bool
}

func (f DBRPMappingFilterV2) String() string {
	var s strings.Builder

	s.WriteString("{ id:")
	if f.ID != nil {
		s.WriteString(f.ID.String())
	} else {
		s.WriteString("<nil>")
	}

	s.WriteString(" org_id:")
	if f.ID != nil {
		s.WriteString(f.OrgID.String())
	} else {
		s.WriteString("<nil>")
	}

	s.WriteString(" bucket_id:")
	if f.ID != nil {
		s.WriteString(f.OrgID.String())
	} else {
		s.WriteString("<nil>")
	}

	s.WriteString(" db:")
	if f.Database != nil {
		s.WriteString(*f.Database)
	} else {
		s.WriteString("<nil>")
	}

	s.WriteString(" rp:")
	if f.RetentionPolicy != nil {
		s.WriteString(*f.RetentionPolicy)
	} else {
		s.WriteString("<nil>")
	}

	s.WriteString(" default:")
	if f.Default != nil {
		s.WriteString(strconv.FormatBool(*f.Default))
	} else {
		s.WriteString("<nil>")
	}
	s.WriteString("}")
	return s.String()
}

// DBRPMappingService provides a mapping of cluster, database and retention policy to an organization ID and bucket ID.
type DBRPMappingService interface {
	// FindBy returns the dbrp mapping the for cluster, db and rp.
	FindBy(ctx context.Context, cluster, db, rp string) (*DBRPMapping, error)
	// Find returns the first dbrp mapping the matches the filter.
	Find(ctx context.Context, filter DBRPMappingFilter) (*DBRPMapping, error)
	// FindMany returns a list of dbrp mappings that match filter and the total count of matching dbrp mappings.
	FindMany(ctx context.Context, filter DBRPMappingFilter, opt ...FindOptions) ([]*DBRPMapping, int, error)
	// Create creates a new dbrp mapping, if a different mapping exists an error is returned.
	Create(ctx context.Context, dbrpMap *DBRPMapping) error
	// Delete removes a dbrp mapping.
	// Deleting a mapping that does not exists is not an error.
	Delete(ctx context.Context, cluster, db, rp string) error
}

// DBRPMapping represents a mapping of a cluster, database and retention policy to an organization ID and bucket ID.
type DBRPMapping struct {
	Cluster         string `json:"cluster"`
	Database        string `json:"database"`
	RetentionPolicy string `json:"retention_policy"`

	// Default indicates if this mapping is the default for the cluster and database.
	Default bool `json:"default"`

	OrganizationID platform.ID `json:"organization_id"`
	BucketID       platform.ID `json:"bucket_id"`
}

// Validate reports any validation errors for the mapping.
func (m DBRPMapping) Validate() error {
	if !validName(m.Cluster) {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "cluster must contain at least one character and only be letters, numbers, '_', '-', and '.'",
		}
	}
	if !validName(m.Database) {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "database must contain at least one character and only be letters, numbers, '_', '-', and '.'",
		}
	}
	if !validName(m.RetentionPolicy) {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "retentionPolicy must contain at least one character and only be letters, numbers, '_', '-', and '.'",
		}
	}
	if !m.OrganizationID.Valid() {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "organizationID is required",
		}
	}
	if !m.BucketID.Valid() {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "bucketID is required",
		}
	}
	return nil
}

// validName checks to see if the given name can would be valid for DB/RP name
func validName(name string) bool {
	for _, r := range name {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return name != "" &&
		name != "." &&
		name != ".." &&
		!strings.ContainsAny(name, `/\`)
}

// Equal checks if the two mappings are identical.
func (m *DBRPMapping) Equal(o *DBRPMapping) bool {
	if m == o {
		return true
	}
	if m == nil || o == nil {
		return false
	}
	return m.Cluster == o.Cluster &&
		m.Database == o.Database &&
		m.RetentionPolicy == o.RetentionPolicy &&
		m.Default == o.Default &&
		m.OrganizationID.Valid() &&
		o.OrganizationID.Valid() &&
		m.BucketID.Valid() &&
		o.BucketID.Valid() &&
		m.OrganizationID == o.OrganizationID &&
		m.BucketID == o.BucketID
}

// DBRPMappingFilter represents a set of filters that restrict the returned results by cluster, database and retention policy.
type DBRPMappingFilter struct {
	Cluster         *string
	Database        *string
	RetentionPolicy *string
	Default         *bool
}

func (f DBRPMappingFilter) String() string {
	var s strings.Builder
	s.WriteString("{")

	s.WriteString("cluster:")
	if f.Cluster != nil {
		s.WriteString(*f.Cluster)
	} else {
		s.WriteString("<nil>")
	}
	s.WriteString(" db:")
	if f.Database != nil {
		s.WriteString(*f.Database)
	} else {
		s.WriteString("<nil>")
	}

	s.WriteString(" rp:")
	if f.RetentionPolicy != nil {
		s.WriteString(*f.RetentionPolicy)
	} else {
		s.WriteString("<nil>")
	}

	s.WriteString(" default:")
	if f.Default != nil {
		s.WriteString(strconv.FormatBool(*f.Default))
	} else {
		s.WriteString("<nil>")
	}
	s.WriteString("}")
	return s.String()
}
