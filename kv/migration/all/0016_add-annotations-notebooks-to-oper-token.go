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

		oprPerms := influxdb.OperPermissions()

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

				// If the permissions list of this token + the extra permissions an
				// operator token for annotations and notebooks would have matches the
				// full list of operator permissions for a 2.1 operator token, we must
				// be dealing with a 2.0.x operator token, so add it to the list.
				if permListsMatch(oprPerms, append(t.Permissions, extraPerms()...)) {
					opTokens = append(opTokens, t)
				}

				return false, nil
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

			t.Permissions = oprPerms

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
	return []influxdb.Permission{
		{
			Action: influxdb.ReadAction,
			Resource: influxdb.Resource{
				Type: influxdb.NotebooksResourceType,
			},
		},
		{
			Action: influxdb.WriteAction,
			Resource: influxdb.Resource{
				Type: influxdb.NotebooksResourceType,
			},
		},
		{
			Action: influxdb.ReadAction,
			Resource: influxdb.Resource{
				Type: influxdb.AnnotationsResourceType,
			},
		},
		{
			Action: influxdb.WriteAction,
			Resource: influxdb.Resource{
				Type: influxdb.AnnotationsResourceType,
			},
		},
	}
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
