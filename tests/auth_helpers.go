package tests

import (
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

func mergePerms(orgID platform.ID, in ...[]influxdb.Permission) []influxdb.Permission {
	var perms []influxdb.Permission
	for i := range in {
		perms = append(perms, in[i]...)
	}
	for i := range perms {
		perms[i].Resource.OrgID = &orgID
	}
	return perms
}

func MakeBucketPerm(bucketID platform.ID, actions ...influxdb.Action) []influxdb.Permission {
	var perms []influxdb.Permission
	for i := range actions {
		perms = append(perms, influxdb.Permission{Action: actions[i], Resource: influxdb.Resource{ID: &bucketID, Type: influxdb.BucketsResourceType}})
	}
	return perms
}

func MakeBucketRWPerm(bucketID platform.ID) []influxdb.Permission {
	return MakeBucketPerm(bucketID, []influxdb.Action{influxdb.ReadAction, influxdb.WriteAction}...)
}

func MakeAuthorization(org, userID platform.ID, perms ...[]influxdb.Permission) *influxdb.Authorization {
	return &influxdb.Authorization{
		OrgID:       org,
		UserID:      userID,
		Permissions: mergePerms(org, perms...),
		Description: "foo user auth",
		Status:      influxdb.Active,
	}
}
