package authorizer

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
)

func isAllowedAll(a influxdb.Authorizer, permissions []influxdb.Permission) error {
	pset, err := a.PermissionSet()
	if err != nil {
		return err
	}

	for _, p := range permissions {
		if !pset.Allowed(p) {
			return &errors.Error{
				Code: errors.EUnauthorized,
				Msg:  fmt.Sprintf("%s is unauthorized", p),
			}
		}
	}
	return nil
}

func isAllowed(a influxdb.Authorizer, p influxdb.Permission) error {
	return isAllowedAll(a, []influxdb.Permission{p})
}

// IsAllowedAll checks to see if an action is authorized by ALL permissions.
// Also see IsAllowed.
func IsAllowedAll(ctx context.Context, permissions []influxdb.Permission) error {
	a, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		return err
	}
	return isAllowedAll(a, permissions)
}

// IsAllowed checks to see if an action is authorized by retrieving the authorizer
// off of context and authorizing the action appropriately.
func IsAllowed(ctx context.Context, p influxdb.Permission) error {
	return IsAllowedAll(ctx, []influxdb.Permission{p})
}

// IsAllowedAll checks to see if an action is authorized by ALL permissions.
// Also see IsAllowed.
func IsAllowedAny(ctx context.Context, permissions []influxdb.Permission) error {
	a, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		return err
	}
	pset, err := a.PermissionSet()
	if err != nil {
		return err
	}
	for _, p := range permissions {
		if pset.Allowed(p) {
			return nil
		}
	}
	return &errors.Error{
		Code: errors.EUnauthorized,
		Msg:  fmt.Sprintf("none of %v is authorized", permissions),
	}
}

func authorize(ctx context.Context, a influxdb.Action, rt influxdb.ResourceType, rid, oid *platform.ID) (influxdb.Authorizer, influxdb.Permission, error) {
	var p *influxdb.Permission
	var err error
	if rid != nil && oid != nil {
		p, err = influxdb.NewPermissionAtID(*rid, a, rt, *oid)
	} else if rid != nil {
		p, err = influxdb.NewResourcePermission(a, rt, *rid)
	} else if oid != nil {
		p, err = influxdb.NewPermission(a, rt, *oid)
	} else {
		p, err = influxdb.NewGlobalPermission(a, rt)
	}
	if err != nil {
		return nil, influxdb.Permission{}, err
	}
	auth, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		return nil, influxdb.Permission{}, err
	}
	return auth, *p, isAllowed(auth, *p)
}

func authorizeReadSystemBucket(ctx context.Context, bid, oid platform.ID) (influxdb.Authorizer, influxdb.Permission, error) {
	return AuthorizeReadOrg(ctx, oid)
}

// AuthorizeReadBucket exists because buckets are a special case and should use this method.
// I.e., instead of:
//  AuthorizeRead(ctx, influxdb.BucketsResourceType, b.ID, b.OrgID)
// use:
//  AuthorizeReadBucket(ctx, b.Type, b.ID, b.OrgID)
func AuthorizeReadBucket(ctx context.Context, bt influxdb.BucketType, bid, oid platform.ID) (influxdb.Authorizer, influxdb.Permission, error) {
	switch bt {
	case influxdb.BucketTypeSystem:
		return authorizeReadSystemBucket(ctx, bid, oid)
	default:
		return AuthorizeRead(ctx, influxdb.BucketsResourceType, bid, oid)
	}
}

// AuthorizeRead authorizes the user in the context to read the specified resource (identified by its type, ID, and orgID).
// NOTE: authorization will pass even if the user only has permissions for the resource type and organization ID only.
func AuthorizeRead(ctx context.Context, rt influxdb.ResourceType, rid, oid platform.ID) (influxdb.Authorizer, influxdb.Permission, error) {
	return authorize(ctx, influxdb.ReadAction, rt, &rid, &oid)
}

// AuthorizeWrite authorizes the user in the context to write the specified resource (identified by its type, ID, and orgID).
// NOTE: authorization will pass even if the user only has permissions for the resource type and organization ID only.
func AuthorizeWrite(ctx context.Context, rt influxdb.ResourceType, rid, oid platform.ID) (influxdb.Authorizer, influxdb.Permission, error) {
	return authorize(ctx, influxdb.WriteAction, rt, &rid, &oid)
}

// AuthorizeRead authorizes the user in the context to read the specified resource (identified by its type, ID).
// NOTE: authorization will pass only if the user has a specific permission for the given resource.
func AuthorizeReadResource(ctx context.Context, rt influxdb.ResourceType, rid platform.ID) (influxdb.Authorizer, influxdb.Permission, error) {
	return authorize(ctx, influxdb.ReadAction, rt, &rid, nil)
}

// AuthorizeWrite authorizes the user in the context to write the specified resource (identified by its type, ID).
// NOTE: authorization will pass only if the user has a specific permission for the given resource.
func AuthorizeWriteResource(ctx context.Context, rt influxdb.ResourceType, rid platform.ID) (influxdb.Authorizer, influxdb.Permission, error) {
	return authorize(ctx, influxdb.WriteAction, rt, &rid, nil)
}

// AuthorizeOrgReadResource authorizes the given org to read the resources of the given type.
// NOTE: this is pretty much the same as AuthorizeRead, in the case that the resource ID is ignored.
// Use it in the case that you do not know which resource in particular you want to give access to.
func AuthorizeOrgReadResource(ctx context.Context, rt influxdb.ResourceType, oid platform.ID) (influxdb.Authorizer, influxdb.Permission, error) {
	return authorize(ctx, influxdb.ReadAction, rt, nil, &oid)
}

// AuthorizeOrgWriteResource authorizes the given org to write the resources of the given type.
// NOTE: this is pretty much the same as AuthorizeWrite, in the case that the resource ID is ignored.
// Use it in the case that you do not know which resource in particular you want to give access to.
func AuthorizeOrgWriteResource(ctx context.Context, rt influxdb.ResourceType, oid platform.ID) (influxdb.Authorizer, influxdb.Permission, error) {
	return authorize(ctx, influxdb.WriteAction, rt, nil, &oid)
}

// AuthorizeCreate authorizes a user to create a resource of the given type for the given org.
func AuthorizeCreate(ctx context.Context, rt influxdb.ResourceType, oid platform.ID) (influxdb.Authorizer, influxdb.Permission, error) {
	return AuthorizeOrgWriteResource(ctx, rt, oid)
}

// AuthorizeReadOrg authorizes the user to read the given org.
func AuthorizeReadOrg(ctx context.Context, oid platform.ID) (influxdb.Authorizer, influxdb.Permission, error) {
	return authorize(ctx, influxdb.ReadAction, influxdb.OrgsResourceType, &oid, nil)
}

// AuthorizeWriteOrg authorizes the user to write the given org.
func AuthorizeWriteOrg(ctx context.Context, oid platform.ID) (influxdb.Authorizer, influxdb.Permission, error) {
	return authorize(ctx, influxdb.WriteAction, influxdb.OrgsResourceType, &oid, nil)
}

// AuthorizeReadGlobal authorizes to read resources of the given type.
func AuthorizeReadGlobal(ctx context.Context, rt influxdb.ResourceType) (influxdb.Authorizer, influxdb.Permission, error) {
	return authorize(ctx, influxdb.ReadAction, rt, nil, nil)
}

// AuthorizeWriteGlobal authorizes to write resources of the given type.
func AuthorizeWriteGlobal(ctx context.Context, rt influxdb.ResourceType) (influxdb.Authorizer, influxdb.Permission, error) {
	return authorize(ctx, influxdb.WriteAction, rt, nil, nil)
}
