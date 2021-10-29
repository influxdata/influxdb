package all

import (
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var Migration0017_AddAnnotationsNotebooksToAllAccessTokens = &Migration{
	name: "add annotations and notebooks resource types to all-access tokens",
	up: migrateTokensMigration(
		func(t influxdb.Authorization) bool {
			return permListsMatch(preNotebooksAnnotationsAllAccessPerms(t.OrgID, t.UserID), t.Permissions)
		},
		func(t *influxdb.Authorization) {
			t.Permissions = append(t.Permissions, notebooksAndAnnotationsPerms(t.OrgID)...)
		},
	),
	down: migrateTokensMigration(
		func(t influxdb.Authorization) bool {
			return permListsMatch(append(preNotebooksAnnotationsAllAccessPerms(t.OrgID, t.UserID), notebooksAndAnnotationsPerms(t.OrgID)...), t.Permissions)
		},
		func(t *influxdb.Authorization) {
			newPerms := t.Permissions[:0]
			for _, p := range t.Permissions {
				switch p.Resource.Type {
				case influxdb.AnnotationsResourceType:
				case influxdb.NotebooksResourceType:
				default:
					newPerms = append(newPerms, p)
				}
			}
			t.Permissions = newPerms
		},
	),
}

// preNotebooksAnnotationsAllAccessPerms is the list of permissions from a 2.0.x all-access token,
// prior to the addition of the notebooks and annotations resource types.
func preNotebooksAnnotationsAllAccessPerms(orgId platform.ID, userId platform.ID) []influxdb.Permission {
	opPerms := preNotebooksAnnotationsOpPerms()
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
