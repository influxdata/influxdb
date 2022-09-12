package influxdb

import (
	"context"
	"strconv"
	"strings"
	"unicode"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

// DBRPMappingService provides CRUD to DBRPMappingV2s.
type DBRPMappingService interface {
	// FindBy returns the dbrp mapping for the specified ID.
	// Requires orgID because every resource will be org-scoped.
	FindByID(ctx context.Context, orgID, id platform.ID) (*DBRPMapping, error)
	// FindMany returns a list of dbrp mappings that match filter and the total count of matching dbrp mappings.
	FindMany(ctx context.Context, dbrp DBRPMappingFilter, opts ...FindOptions) ([]*DBRPMapping, int, error)
	// Create creates a new dbrp mapping, if a different mapping exists an error is returned.
	Create(ctx context.Context, dbrp *DBRPMapping) error
	// Update a new dbrp mapping
	Update(ctx context.Context, dbrp *DBRPMapping) error
	// Delete removes a dbrp mapping.
	// Deleting a mapping that does not exists is not an error.
	// Requires orgID because every resource will be org-scoped.
	Delete(ctx context.Context, orgID, id platform.ID) error
}

// DBRPMapping represents a mapping of a database and retention policy to an organization ID and bucket ID.
type DBRPMapping struct {
	ID              platform.ID `json:"id"`
	Database        string      `json:"database"`
	RetentionPolicy string      `json:"retention_policy"`

	// Default indicates if this mapping is the default for the cluster and database.
	Default bool `json:"default"`
	// Virtual indicates if this is a virtual mapping (tied to bucket name) or physical
	Virtual bool `json:"virtual"`

	OrganizationID platform.ID `json:"orgID"`
	BucketID       platform.ID `json:"bucketID"`
}

// Validate reports any validation errors for the mapping.
func (m DBRPMapping) Validate() error {
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
func (m *DBRPMapping) Equal(o *DBRPMapping) bool {
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

// DBRPMappingFilter represents a set of filters that restrict the returned results.
type DBRPMappingFilter struct {
	ID       *platform.ID
	OrgID    *platform.ID
	BucketID *platform.ID

	Database        *string
	RetentionPolicy *string
	Default         *bool
	Virtual         *bool
}

func (f DBRPMappingFilter) String() string {
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
