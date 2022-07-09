package all

import (
	"context"
	"encoding/json"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/snowflake"
)

var (
	bucketBucket       = []byte("bucketsv1")
	byOrgIDIndexBucket = []byte("dbrpbyorgv1")
)

type bucket struct {
	ID                  platform.ID   `json:"id,omitempty"`
	OrgID               platform.ID   `json:"orgID,omitempty"`
	Type                int           `json:"type"`
	Name                string        `json:"name"`
	Description         string        `json:"description"`
	RetentionPolicyName string        `json:"rp,omitempty"` // This to support v1 sources
	RetentionPeriod     time.Duration `json:"retentionPeriod"`
	CreatedAt           time.Time     `json:"createdAt"`
	UpdatedAt           time.Time     `json:"updatedAt"`

	// This is expected to be 0 for all buckets created before
	// we began tracking it in metadata.
	ShardGroupDuration time.Duration `json:"shardGroupDuration"`
}
type dbrpMapping struct {
	ID              platform.ID `json:"id"`
	Database        string      `json:"database"`
	RetentionPolicy string      `json:"retention_policy"`
	Default         bool        `json:"default"`

	OrganizationID platform.ID `json:"orgID"`
	BucketID       platform.ID `json:"bucketID"`
}

func createDbrpForExistingBuckets(ctx context.Context, store kv.SchemaStore) error {
	// Collect names of all buckets
	var buckets []*bucket
	if err := store.View(ctx, func(tx kv.Tx) error {
		bkt, err := tx.Bucket(bucketBucket)
		if err != nil {
			return err
		}

		cursor, err := bkt.ForwardCursor(nil)
		if err != nil {
			return err
		}

		return kv.WalkCursor(ctx, cursor, func(_, v []byte) (bool, error) {
			var b bucket
			if err := json.Unmarshal(v, &b); err != nil {
				return false, err
			}
			buckets = append(buckets, &b)
			return true, nil
		})
	}); err != nil {
		return err
	}
	// collect list of all existing DBRP mappings
	var mappings []*dbrpMapping
	if err := store.View(ctx, func(tx kv.Tx) error {
		bkt, err := tx.Bucket(dbrpBucket)
		if err != nil {
			return err
		}

		cursor, err := bkt.ForwardCursor(nil)
		if err != nil {
			return err
		}

		return kv.WalkCursor(ctx, cursor, func(_, v []byte) (bool, error) {
			var mapping dbrpMapping
			if err := json.Unmarshal(v, &mapping); err != nil {
				return false, err
			}

			if mapping.OrganizationID.Valid() && mapping.BucketID.Valid() {
				mappings = append(mappings, &mapping)
			}

			return true, nil
		})
	}); err != nil {
		return err
	}

	idgen := snowflake.NewDefaultIDGenerator()
	byOrgAndDatabase := kv.NewIndex(kv.NewIndexMapping(dbrpBucket, dbrpIndexBucket, func(v []byte) ([]byte, error) {
		var dbrp influxdb.DBRPMapping
		if err := json.Unmarshal(v, &dbrp); err != nil {
			return nil, err
		}
		return indexForeignKey(dbrp), nil
	}), kv.WithIndexReadPathEnabled)
	byOrg := kv.NewIndex(dbrp.ByOrgIDIndexMapping, kv.WithIndexReadPathEnabled)
	// Create DBRP mapping for each bucket (if it doesn't already exist)
	return store.Update(ctx, func(tx kv.Tx) error {
		bkt, err := tx.Bucket(dbrpBucket)
		if err != nil {
			return err
		}
	OUTER:
		for _, b := range buckets {
			db, rp := dbrp.ParseDBRP(b.Name)
			mapping := dbrpMapping{
				ID:              idgen.ID(),
				Database:        db,
				RetentionPolicy: rp,
				OrganizationID:  b.OrgID,
				BucketID:        b.ID,
			}
			for _, mappingPtr := range mappings {
				realMapping := *mappingPtr
				// set Default and ID to same, so only comparison of other fields matters (bucketID, orgID, db, rp)
				realMapping.Default = mapping.Default
				realMapping.ID = mapping.ID
				// if they are the same, this mapping exists. Move to next bucket
				if mapping == realMapping {
					continue OUTER
				}
			}
			encodedDbrpID, err := mapping.ID.Encode()
			if err != nil {
				return err
			}
			mappingBytes, err := json.Marshal(mapping)
			if err != nil {
				return err
			}
			compID := composeForeignKey(mapping.OrganizationID, mapping.Database)
			if err := byOrgAndDatabase.Insert(tx, compID, encodedDbrpID); err != nil {
				return err
			}
			encodedOrgID, err := mapping.OrganizationID.Encode()
			if err != nil {
				return err
			}
			if err := byOrg.Insert(tx, encodedOrgID, encodedDbrpID); err != nil {
				return err
			}
			if err := bkt.Put(encodedDbrpID, mappingBytes); err != nil {
				return err
			}
		}
		return nil
	})
}

func indexForeignKey(dbrp influxdb.DBRPMapping) []byte {
	return composeForeignKey(dbrp.OrganizationID, dbrp.Database)
}

func composeForeignKey(orgID platform.ID, db string) []byte {
	encID, _ := orgID.Encode()
	key := make([]byte, len(encID)+len(db))
	copy(key, encID)
	copy(key[len(encID):], db)
	return key
}

var Migration0021_Create_dbrp_for_existing_buckets = UpOnlyMigration(
	"create DBRP mappings for each existing bucket",
	createDbrpForExistingBuckets,
)
