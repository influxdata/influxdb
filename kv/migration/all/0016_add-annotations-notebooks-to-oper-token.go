package all

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

var Migration0016_AddAnnotationsNotebooksToOperToken = UpOnlyMigration(
	"add annotations and notebooks resource types to operator token",
	func(ctx context.Context, store kv.SchemaStore) error {
		authBucket := []byte("authorizationsv1")

		// Find the operator token that needs updated

		// There will usually be 1 operator token. If somebody has deleted their
		// operator token, we don't necessarily want to make it so that influxdb
		// won't start, so store a list of the found operator tokens so that it can
		// be iterated on later.
		opTokens := []influxdb.Authorization{}
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

				// Add any tokens to the list that match the list of permission from an
				// "old" operator token
				if permListsMatch(oldOpPerms(), t.Permissions) {
					opTokens = append(opTokens, t)
				}

				return true, nil
			})
		}); err != nil {
			return err
		}

		// Go through the list of operator tokens found and update their permissions
		// list. There should be only 1, but if there are somehow more this will
		// update all of them.
		for _, t := range opTokens {
			encodedID, err := t.ID.Encode()
			if err != nil {
				return err
			}

			t.Permissions = append(t.Permissions, extraPerms()...)

			v, err := json.Marshal(t)
			if err != nil {
				return err
			}

			if err := store.Update(ctx, func(tx kv.Tx) error {
				bkt, err := tx.Bucket(authBucket)
				if err != nil {
					return err
				}

				return bkt.Put(encodedID, v)
			}); err != nil {
				return err
			}
		}

		return nil
	},
)

// extraPerms returns the list of additional permissions that need added for
// annotations and notebooks.
func extraPerms() []influxdb.Permission {
	resTypes := []influxdb.Resource{
		{
			Type: influxdb.AnnotationsResourceType,
		},
		{
			Type: influxdb.NotebooksResourceType,
		},
	}

	return permListFromResources(resTypes)
}

// oldOpPerms is the list of permissions from an "old" operator token - prior to
// the addition of the notebooks an annotations resource type.
func oldOpPerms() []influxdb.Permission {
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
