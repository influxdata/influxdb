package all

import (
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var Migration0019_AddRemotesReplicationsToTokens = UpOnlyMigration(
	"add remotes and replications resource types to operator and all-access tokens",
	migrateTokensMigration(
		func(t influxdb.Authorization) bool {
			return permListsMatch(preReplicationOpPerms(), t.Permissions) ||
				permListsMatch(preReplicationAllAccessPerms(t.OrgID, t.UserID), t.Permissions)
		},
		func(t *influxdb.Authorization) {
			if permListsMatch(preReplicationOpPerms(), t.Permissions) {
				t.Permissions = append(t.Permissions, remotesAndReplicationsPerms(0)...)
			} else {
				t.Permissions = append(t.Permissions, remotesAndReplicationsPerms(t.OrgID)...)
			}
		},
	),
)

func preReplicationOpPerms() []influxdb.Permission {
	return append(preNotebooksAnnotationsOpPerms(), notebooksAndAnnotationsPerms(0)...)
}

func preReplicationAllAccessPerms(orgID platform.ID, userID platform.ID) []influxdb.Permission {
	return append(preNotebooksAnnotationsAllAccessPerms(orgID, userID), notebooksAndAnnotationsPerms(orgID)...)
}

func remotesAndReplicationsPerms(orgID platform.ID) []influxdb.Permission {
	resTypes := []influxdb.Resource{
		{
			Type: influxdb.RemotesResourceType,
		},
		{
			Type: influxdb.ReplicationsResourceType,
		},
	}
	perms := permListFromResources(resTypes)
	if orgID.Valid() {
		for i := range perms {
			perms[i].Resource.OrgID = &orgID
		}
	}
	return perms
}
