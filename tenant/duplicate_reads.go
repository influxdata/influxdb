package tenant

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"go.uber.org/zap"
)

var _ influxdb.TenantService = (*tenantService)(nil)

var bucketCmpOptions = cmp.Options{
	cmp.Transformer("Sort", func(in []*influxdb.Bucket) []*influxdb.Bucket {
		out := append([]*influxdb.Bucket(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].Name > out[j].Name
		})
		return out
	}),
}

// readOnlyStore is a wrapper for kv.Store that ensures that updates are not applied.
type readOnlyStore struct {
	kv.Store
}

func (r readOnlyStore) Update(context.Context, func(kv.Tx) error) error {
	return nil
}

type tenantService struct {
	log *zap.Logger

	oldBucketSvc influxdb.BucketService
	newBucketSvc influxdb.BucketService
	oldOrgSvc    influxdb.OrganizationService
	newOrgSvc    influxdb.OrganizationService
	oldURMSvc    influxdb.UserResourceMappingService
	newURMSvc    influxdb.UserResourceMappingService
	oldUserSvc   influxdb.UserService
	newUserSvc   influxdb.UserService
	oldPwdSvc    influxdb.PasswordsService
	newPwdSvc    influxdb.PasswordsService
}

// NewReadOnlyStore returns a Store that cannot update the underlying kv.Store.
func NewReadOnlyStore(store kv.Store) *Store {
	return NewStore(readOnlyStore{Store: store})
}

// NewDuplicateReadTenantService returns a tenant service that duplicates the reads to oldSvc and newSvc.
// The foreseen use case is to compare two service versions, an old one and a new one.
// The resulting influxdb.TenantService:
//  - forwards writes to the old service;
//  - reads from the old one, if no error is encountered, it reads from the new one;
//  - compares the results obtained and logs the difference, if any.
func NewDuplicateReadTenantService(log *zap.Logger, oldSvc, newSvc influxdb.TenantService) influxdb.TenantService {
	return tenantService{
		log: log,

		oldBucketSvc: oldSvc,
		oldOrgSvc:    oldSvc,
		oldURMSvc:    oldSvc,
		oldUserSvc:   oldSvc,
		oldPwdSvc:    oldSvc,

		newBucketSvc: newSvc,
		newOrgSvc:    newSvc,
		newURMSvc:    newSvc,
		newUserSvc:   newSvc,
		newPwdSvc:    newSvc,
	}
}

// NewDuplicateReadBucketService returns a service that duplicates the reads to oldSvc and newSvc.
func NewDuplicateReadBucketService(log *zap.Logger, oldSvc, newSvc influxdb.BucketService) influxdb.BucketService {
	return tenantService{
		log: log,

		oldBucketSvc: oldSvc,
		newBucketSvc: newSvc,
	}
}

// NewDuplicateReadOrganizationService returns a service that duplicates the reads to oldSvc and newSvc.
func NewDuplicateReadOrganizationService(log *zap.Logger, oldSvc, newSvc influxdb.OrganizationService) influxdb.OrganizationService {
	return tenantService{
		log: log,

		oldOrgSvc: oldSvc,
		newOrgSvc: newSvc,
	}
}

// NewDuplicateReadUserResourceMappingService returns a service that duplicates the reads to oldSvc and newSvc.
func NewDuplicateReadUserResourceMappingService(log *zap.Logger, oldSvc, newSvc influxdb.UserResourceMappingService) influxdb.UserResourceMappingService {
	return tenantService{
		log: log,

		oldURMSvc: oldSvc,
		newURMSvc: newSvc,
	}
}

// NewDuplicateReadUserService returns a service that duplicates the reads to oldSvc and newSvc.
func NewDuplicateReadUserService(log *zap.Logger, oldSvc, newSvc influxdb.UserService) influxdb.UserService {
	return tenantService{
		log: log,

		oldUserSvc: oldSvc,
		newUserSvc: newSvc,
	}
}

// NewDuplicateReadPasswordsService returns a service that duplicates the reads to oldSvc and newSvc.
func NewDuplicateReadPasswordsService(log *zap.Logger, oldSvc, newSvc influxdb.PasswordsService) influxdb.PasswordsService {
	return tenantService{
		log: log,

		oldPwdSvc: oldSvc,
		newPwdSvc: newSvc,
	}
}

func (s tenantService) FindBucketByID(ctx context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
	o, err := s.oldBucketSvc.FindBucketByID(ctx, id)
	if err != nil {
		return o, err
	}
	n, err := s.newBucketSvc.FindBucketByID(ctx, id)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindBucketByID"))
	}
	return o, nil
}

func (s tenantService) FindBucketByName(ctx context.Context, orgID influxdb.ID, name string) (*influxdb.Bucket, error) {
	o, err := s.oldBucketSvc.FindBucketByName(ctx, orgID, name)
	if err != nil {
		return o, err
	}
	n, err := s.newBucketSvc.FindBucketByName(ctx, orgID, name)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindBucketByName"))
	}
	return o, nil
}

func (s tenantService) FindBucket(ctx context.Context, filter influxdb.BucketFilter) (*influxdb.Bucket, error) {
	o, err := s.oldBucketSvc.FindBucket(ctx, filter)
	if err != nil {
		return o, err
	}
	n, err := s.newBucketSvc.FindBucket(ctx, filter)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindBucket"))
	}
	return o, nil
}

func (s tenantService) FindBuckets(ctx context.Context, filter influxdb.BucketFilter, opt ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
	o, no, err := s.oldBucketSvc.FindBuckets(ctx, filter, opt...)
	if err != nil {
		return o, no, err
	}
	n, _, err := s.newBucketSvc.FindBuckets(ctx, filter, opt...)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n, bucketCmpOptions); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindBuckets"))
	}
	return o, no, nil
}

func (s tenantService) CreateBucket(ctx context.Context, b *influxdb.Bucket) error {
	return s.oldBucketSvc.CreateBucket(ctx, b)
}

func (s tenantService) UpdateBucket(ctx context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	return s.oldBucketSvc.UpdateBucket(ctx, id, upd)
}

func (s tenantService) DeleteBucket(ctx context.Context, id influxdb.ID) error {
	return s.oldBucketSvc.DeleteBucket(ctx, id)
}

func (s tenantService) FindOrganizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
	o, err := s.oldOrgSvc.FindOrganizationByID(ctx, id)
	if err != nil {
		return o, err
	}
	n, err := s.newOrgSvc.FindOrganizationByID(ctx, id)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindOrganizationByID"))
	}
	return o, nil
}

func (s tenantService) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	o, err := s.oldOrgSvc.FindOrganization(ctx, filter)
	if err != nil {
		return o, err
	}
	n, err := s.newOrgSvc.FindOrganization(ctx, filter)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindOrganization"))
	}
	return o, nil
}

func (s tenantService) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	o, no, err := s.oldOrgSvc.FindOrganizations(ctx, filter, opt...)
	if err != nil {
		return o, no, err
	}
	n, _, err := s.newOrgSvc.FindOrganizations(ctx, filter, opt...)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindOrganizations"))
	}
	return o, no, nil
}

func (s tenantService) CreateOrganization(ctx context.Context, b *influxdb.Organization) error {
	return s.oldOrgSvc.CreateOrganization(ctx, b)
}

func (s tenantService) UpdateOrganization(ctx context.Context, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	return s.oldOrgSvc.UpdateOrganization(ctx, id, upd)
}

func (s tenantService) DeleteOrganization(ctx context.Context, id influxdb.ID) error {
	return s.oldOrgSvc.DeleteOrganization(ctx, id)
}

func (s tenantService) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	o, no, err := s.oldURMSvc.FindUserResourceMappings(ctx, filter, opt...)
	if err != nil {
		return o, no, err
	}
	n, _, err := s.newURMSvc.FindUserResourceMappings(ctx, filter, opt...)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindUserResourceMappings"))
	}
	return o, no, nil
}

func (s tenantService) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	return s.oldURMSvc.CreateUserResourceMapping(ctx, m)
}

func (s tenantService) DeleteUserResourceMapping(ctx context.Context, resourceID, userID influxdb.ID) error {
	return s.oldURMSvc.DeleteUserResourceMapping(ctx, resourceID, userID)
}

func (s tenantService) FindUserByID(ctx context.Context, id influxdb.ID) (*influxdb.User, error) {
	o, err := s.oldUserSvc.FindUserByID(ctx, id)
	if err != nil {
		return o, err
	}
	n, err := s.newUserSvc.FindUserByID(ctx, id)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindUserByID"))
	}
	return o, nil
}

func (s tenantService) FindUser(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
	o, err := s.oldUserSvc.FindUser(ctx, filter)
	if err != nil {
		return o, err
	}
	n, err := s.newUserSvc.FindUser(ctx, filter)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindUser"))
	}
	return o, nil
}

func (s tenantService) FindUsers(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) ([]*influxdb.User, int, error) {
	o, no, err := s.oldUserSvc.FindUsers(ctx, filter, opt...)
	if err != nil {
		return o, no, err
	}
	n, _, err := s.newUserSvc.FindUsers(ctx, filter, opt...)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	} else if diff := cmp.Diff(o, n); diff != "" {
		s.log.Error(fmt.Sprintf("unexpected read result -old/+new:\n\t%s", diff), zap.String("diff", diff), zap.String("call", "FindUsers"))
	}
	return o, no, nil
}

func (s tenantService) CreateUser(ctx context.Context, u *influxdb.User) error {
	return s.oldUserSvc.CreateUser(ctx, u)
}

func (s tenantService) UpdateUser(ctx context.Context, id influxdb.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	return s.oldUserSvc.UpdateUser(ctx, id, upd)
}

func (s tenantService) DeleteUser(ctx context.Context, id influxdb.ID) error {
	return s.oldUserSvc.DeleteUser(ctx, id)
}

func (s tenantService) SetPassword(ctx context.Context, userID influxdb.ID, password string) error {
	return s.oldPwdSvc.SetPassword(ctx, userID, password)
}

func (s tenantService) ComparePassword(ctx context.Context, userID influxdb.ID, password string) error {
	if err := s.oldPwdSvc.ComparePassword(ctx, userID, password); err != nil {
		return err
	}
	err := s.newPwdSvc.ComparePassword(ctx, userID, password)
	if err != nil {
		s.log.Error("error in new meta store", zap.Error(err))
	}
	return nil
}

func (s tenantService) CompareAndSetPassword(ctx context.Context, userID influxdb.ID, old, new string) error {
	return s.oldPwdSvc.CompareAndSetPassword(ctx, userID, old, new)
}
