package all

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
)

var Migration0016_AddAnnotationsNotebooksToOperToken = &Migration{
	name: "add annotations and notebooks resource types to operator token",
	up: migrateTokensMigration(
		func(t influxdb.Authorization) bool {
			return permListsMatch(preNotebooksAnnotationsOpPerms(), t.Permissions)
		},
		func(t *influxdb.Authorization) {
			t.Permissions = append(t.Permissions, notebooksAndAnnotationsPerms(0)...)
		},
	),
	down: migrateTokensMigration(
		func(t influxdb.Authorization) bool {
			return permListsMatch(append(preNotebooksAnnotationsOpPerms(), notebooksAndAnnotationsPerms(0)...), t.Permissions)
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

func migrateTokensMigration(
	checkToken func(influxdb.Authorization) bool,
	updateToken func(*influxdb.Authorization),
) func(context.Context, kv.SchemaStore) error {
	return func(ctx context.Context, store kv.SchemaStore) error {
		authBucket := []byte("authorizationsv1")

		// First find all tokens matching the predicate.
		var tokens []influxdb.Authorization
		if err := store.View(ctx, func(tx kv.Tx) error {
			bkt, err := tx.Bucket(authBucket)
			if err != nil {
				return err
			}
			cursor, err := bkt.ForwardCursor(nil)
			if err != nil {
				return err
			}

			return kv.WalkCursor(ctx, cursor, func(_, v []byte) (bool, error) {
				var t influxdb.Authorization
				if err := json.Unmarshal(v, &t); err != nil {
					return false, err
				}
				if checkToken(t) {
					tokens = append(tokens, t)
				}
				return true, nil
			})
		}); err != nil {
			return err
		}

		// Next, update all the extracted tokens.
		for i := range tokens {
			updateToken(&tokens[i])
		}

		// Finally, persist the updated tokens back to the DB.
		if err := store.Update(ctx, func(tx kv.Tx) error {
			bkt, err := tx.Bucket(authBucket)
			if err != nil {
				return err
			}
			for _, t := range tokens {
				encodedID, err := t.ID.Encode()
				if err != nil {
					return err
				}
				v, err := json.Marshal(t)
				if err != nil {
					return err
				}
				if err := bkt.Put(encodedID, v); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}

		return nil
	}
}

// notebooksAndAnnotationsPerms returns the list of additional permissions that need added for
// annotations and notebooks.
func notebooksAndAnnotationsPerms(orgID platform.ID) []influxdb.Permission {
	resTypes := []influxdb.Resource{
		{
			Type: influxdb.AnnotationsResourceType,
		},
		{
			Type: influxdb.NotebooksResourceType,
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

// preNotebooksAnnotationsOpPerms is the list of permissions from a 2.0.x operator token,
// prior to the addition of the notebooks and annotations resource types.
func preNotebooksAnnotationsOpPerms() []influxdb.Permission {
	resTypes := []influxdb.Resource{
		{
			Type: influxdb.AuthorizationsResourceType,
		},
		{
			Type: influxdb.BucketsResourceType,
		},
		{
			Type: influxdb.DashboardsResourceType,
		},
		{
			Type: influxdb.OrgsResourceType,
		},
		{
			Type: influxdb.SourcesResourceType,
		},
		{
			Type: influxdb.TasksResourceType,
		},
		{
			Type: influxdb.TelegrafsResourceType,
		},
		{
			Type: influxdb.UsersResourceType,
		},
		{
			Type: influxdb.VariablesResourceType,
		},
		{
			Type: influxdb.ScraperResourceType,
		},
		{
			Type: influxdb.SecretsResourceType,
		},
		{
			Type: influxdb.LabelsResourceType,
		},
		{
			Type: influxdb.ViewsResourceType,
		},
		{
			Type: influxdb.DocumentsResourceType,
		},
		{
			Type: influxdb.NotificationRuleResourceType,
		},
		{
			Type: influxdb.NotificationEndpointResourceType,
		},
		{
			Type: influxdb.ChecksResourceType,
		},
		{
			Type: influxdb.DBRPResourceType,
		},
	}

	return permListFromResources(resTypes)
}

func permListFromResources(l []influxdb.Resource) []influxdb.Permission {
	output := make([]influxdb.Permission, 0, len(l)*2)
	for _, r := range l {
		output = append(output, []influxdb.Permission{
			{
				Action:   influxdb.ReadAction,
				Resource: r,
			},
			{
				Action:   influxdb.WriteAction,
				Resource: r,
			},
		}...)
	}

	return output
}

func sortPermList(l []influxdb.Permission) {
	sort.Slice(l, func(i, j int) bool {
		if l[i].Resource.String() < l[j].Resource.String() {
			return true
		}
		if l[i].Resource.String() > l[j].Resource.String() {
			return false
		}
		return l[i].Action < l[j].Action
	})
}

func permListsMatch(l1, l2 []influxdb.Permission) bool {
	if len(l1) != len(l2) {
		return false
	}

	sortPermList(l1)
	sortPermList(l2)

	for i := 0; i < len(l1); i++ {
		if !l1[i].Matches(l2[i]) {
			return false
		}
	}

	return true
}
