package all

import (
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var Migration0017_AddAnnotationsNotebooksToAllAccessTokens = UpOnlyMigration(
	"add annotations and notebooks resource types to all-access tokens",
	migrateTokensMigration(
		func(t influxdb.Authorization) bool {
			return permListsMatch(oldAllAccessPerms(t.OrgID, t.UserID), t.Permissions)
		},
		func(t *influxdb.Authorization) {
			t.Permissions = append(t.Permissions, extraAllAccessPerms(t.OrgID)...)
		},
	),
)

// extraAllAccessPerms returns the list of additional permissions that need added for
// annotations and notebooks.
func extraAllAccessPerms(orgId platform.ID) []influxdb.Permission {
	perms := extraPerms()
	for i := range perms {
		perms[i].Resource.OrgID = &orgId
	}
	return perms
}

// oldAllAccessPerms is the list of permissions from an "old" all-access token - prior to
// the addition of the notebooks an annotations resource type.
func oldAllAccessPerms(orgId platform.ID, userId platform.ID) []influxdb.Permission {
	opPerms := oldOpPerms()
	perms := make([]influxdb.Permission, 0, len(opPerms)-1) // -1 because write-org permission isn't included.
	for _, p := range opPerms {
		if p.Resource.Type == influxdb.OrgsResourceType {
			// All-access grants read-only access to the enclosing org.
			if p.Action == influxdb.WriteAction {
				continue
			}
			p.Resource.ID = &orgId
		} else if p.Resource.Type == influxdb.UsersResourceType {
			// It grants read and write access to the associated user.
			p.Resource.ID = &userId
		} else {
			// It grants read and write access to all other resources in the enclosing org.
			p.Resource.OrgID = &orgId
		}
		perms = append(perms, p)
	}
	return perms
}
