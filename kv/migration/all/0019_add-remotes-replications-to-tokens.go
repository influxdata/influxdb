package all

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
)

var Migration0019_AddRemotesReplicationsToTokens = UpOnlyMigration(
	"add remotes and replications resource types to operator and all-access tokens",
	func(ctx context.Context, store kv.SchemaStore) error {
		authBucket := []byte("authorizationsv1")

		// Find operator and all-access tokens that need to be updated.
		var opTokens []influxdb.Authorization
		var allAccessTokens []influxdb.Authorization
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

				if permListsMatch(preReplicationOpPerms(), t.Permissions) {
					opTokens = append(opTokens, t)
				}
				if permListsMatch(preReplicationAllAccessPerms(t.OrgID, t.UserID), t.Permissions) {
					allAccessTokens = append(allAccessTokens, t)
				}
				return true, nil
			})
		}); err != nil {
			return err
		}

		updatedTokens := make([]influxdb.Authorization, len(opTokens)+len(allAccessTokens))
		for i, t := range opTokens {
			updatedTokens[i] = t
			updatedTokens[i].Permissions =
				append(updatedTokens[i].Permissions, remotesAndReplicationsPerms(0)...)
		}
		for i, t := range allAccessTokens {
			idx := i + len(opTokens)
			updatedTokens[idx] = t
			updatedTokens[idx].Permissions =
				append(updatedTokens[idx].Permissions, remotesAndReplicationsPerms(t.OrgID)...)
		}

		if err := store.Update(ctx, func(tx kv.Tx) error {
			bkt, err := tx.Bucket(authBucket)
			if err != nil {
				return err
			}

			for _, t := range updatedTokens {
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
	})

func preReplicationOpPerms() []influxdb.Permission {
	return append(oldOpPerms(), extraPerms()...)
}

func preReplicationAllAccessPerms(orgID platform.ID, userID platform.ID) []influxdb.Permission {
	return append(oldAllAccessPerms(orgID, userID), extraAllAccessPerms(orgID)...)
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
