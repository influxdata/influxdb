package all

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
)

var Migration0017_AddAnnotationsNotebooksToAllAccessTokens = UpOnlyMigration(
	"add annotations and notebooks resource types to all-access tokens",
	func(ctx context.Context, store kv.SchemaStore) error {
		authBucket := []byte("authorizationsv1")

		// Find all-access tokens that need to be updated.

		tokens := []influxdb.Authorization{}
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
				// "old" all-access token
				if permListsMatch(oldAllAccessPerms(t.OrgID, t.UserID), t.Permissions) {
					tokens = append(tokens, t)
				}

				return true, nil
			})
		}); err != nil {
			return err
		}

		// Go through the list of all-access tokens found and update their permissions list.
		for _, t := range tokens {
			encodedID, err := t.ID.Encode()
			if err != nil {
				return err
			}

			t.Permissions = append(t.Permissions, extraAllAccessPerms(t.OrgID)...)

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
			if p.Action == influxdb.WriteAction {
				continue
			}
			p.Resource.ID = &orgId
		} else if p.Resource.Type == influxdb.UsersResourceType {
			p.Resource.ID = &userId
		} else {
			p.Resource.OrgID = &orgId
		}
		perms = append(perms, p)
	}
	return perms
}
