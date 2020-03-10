package kv

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb"
)

// DocumentIndex implements influxdb.DocumentIndex. It is used to access labels/owners of documents.
type DocumentIndex struct {
	service   *Service
	namespace string

	ctx      context.Context
	tx       Tx
	writable bool
}

// FindLabelByID retrieves a label by id.
func (i *DocumentIndex) FindLabelByID(id influxdb.ID) error {
	_, err := i.service.findLabelByID(i.ctx, i.tx, id)
	return err
}

// UsersOrgs retrieves a list of all orgs that a user is an accessor of.
func (i *DocumentIndex) UsersOrgs(userID influxdb.ID) ([]influxdb.ID, error) {
	f := influxdb.UserResourceMappingFilter{
		UserID:       userID,
		ResourceType: influxdb.OrgsResourceType,
	}

	ms, err := i.service.findUserResourceMappings(i.ctx, i.tx, f)
	if err != nil {
		return nil, err
	}

	ids := make([]influxdb.ID, 0, len(ms))
	for _, m := range ms {
		ids = append(ids, m.ResourceID)
	}

	return ids, nil
}

// IsOrgAccessor checks to see if the user is an accessor of the org provided. If the operation
// is writable it ensures that the user is owner.
func (i *DocumentIndex) IsOrgAccessor(userID influxdb.ID, orgID influxdb.ID) error {
	f := influxdb.UserResourceMappingFilter{
		UserID:       userID,
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   orgID,
	}

	if i.writable {
		f.UserType = influxdb.Owner
	}

	ms, err := i.service.findUserResourceMappings(i.ctx, i.tx, f)
	if err != nil {
		return err
	}

	for _, m := range ms {
		switch m.UserType {
		case influxdb.Owner, influxdb.Member:
			return nil
		default:
			continue
		}
	}

	return &influxdb.Error{
		Code: influxdb.EUnauthorized,
		Msg:  "user is not org member",
	}
}

func (i *DocumentIndex) ownerExists(ownerType string, ownerID influxdb.ID) error {
	switch ownerType {
	case "org":
		if _, err := i.service.findOrganizationByID(i.ctx, i.tx, ownerID); err != nil {
			return err
		}
	case "user":
		if _, err := i.service.findUserByID(i.ctx, i.tx, ownerID); err != nil {
			return err
		}
	default:
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("unknown owner type %q", ownerType),
		}
	}

	return nil
}

// FindOrganizationByName retrieves the organization ID of the org provided.
func (i *DocumentIndex) FindOrganizationByName(org string) (influxdb.ID, error) {
	o, err := i.service.findOrganizationByName(i.ctx, i.tx, org)
	if err != nil {
		return influxdb.InvalidID(), err
	}
	return o.ID, nil
}

// FindOrganizationByID checks if the org existence by the org id provided.
func (i *DocumentIndex) FindOrganizationByID(id influxdb.ID) error {
	_, err := i.service.findOrganizationByID(i.ctx, i.tx, id)
	if err != nil {
		return err
	}
	return nil
}

// GetDocumentsAccessors retrieves the list of accessors of a document.
func (i *DocumentIndex) GetDocumentsAccessors(docID influxdb.ID) ([]influxdb.ID, error) {
	f := influxdb.UserResourceMappingFilter{
		ResourceType: influxdb.DocumentsResourceType,
		ResourceID:   docID,
	}
	if i.writable {
		f.UserType = influxdb.Owner
	}
	ms, err := i.service.findUserResourceMappings(i.ctx, i.tx, f)
	if err != nil {
		return nil, err
	}

	ids := make([]influxdb.ID, 0, len(ms))
	for _, m := range ms {
		if m.MappingType == influxdb.UserMappingType {
			continue
		}
		// TODO(desa): this is really an orgID, eventually we should support users and org as owners of documents
		ids = append(ids, m.UserID)
	}

	return ids, nil
}

// GetAccessorsDocuments retrieves the list of documents a user is allowed to access.
func (i *DocumentIndex) GetAccessorsDocuments(ownerType string, ownerID influxdb.ID) ([]influxdb.ID, error) {
	if err := i.ownerExists(ownerType, ownerID); err != nil {
		return nil, err
	}

	f := influxdb.UserResourceMappingFilter{
		UserID:       ownerID,
		ResourceType: influxdb.DocumentsResourceType,
	}
	if i.writable {
		f.UserType = influxdb.Owner
	}
	ms, err := i.service.findUserResourceMappings(i.ctx, i.tx, f)
	if err != nil {
		return nil, err
	}

	ids := make([]influxdb.ID, 0, len(ms))
	for _, m := range ms {
		ids = append(ids, m.ResourceID)
	}

	return ids, nil
}

// DocumentDecorator is used to communication the decoration of documents to the
// document store.
type DocumentDecorator struct {
	data   bool
	labels bool
	orgs   bool

	writable bool
}

// IncludeContent signals that the document should include its content when returned.
func (d *DocumentDecorator) IncludeContent() error {
	if d.writable {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  "cannot include data in document",
		}
	}

	d.data = true
	return nil
}

// IncludeLabels signals that the document should include its labels when returned.
func (d *DocumentDecorator) IncludeLabels() error {
	if d.writable {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  "cannot include labels in document",
		}
	}

	d.labels = true
	return nil
}

// IncludeOwner signals that the document should include its owner.
func (d *DocumentDecorator) IncludeOrganizations() error {
	if d.writable {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  "cannot include labels in document",
		}
	}

	d.orgs = true
	return nil
}
