package all

import (
	"context"
	"encoding/json"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
)

var Migration0015_RecordShardGroupDurationsInBucketMetadata = UpOnlyMigration(
	"record shard group durations in bucket metadata",
	func(ctx context.Context, store kv.SchemaStore) error {
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
		bucketBucket := []byte("bucketsv1")

		// Collect buckets that need to be updated
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
				if b.ShardGroupDuration == 0 {
					buckets = append(buckets, &b)
				}

				return true, nil
			})
		}); err != nil {
			return err
		}

		batchSize := 100
		writeBatch := func(batch []*bucket) (err error) {
			ids := make([][]byte, len(batch))
			for i, b := range batch {
				ids[i], err = b.ID.Encode()
				if err != nil {
					return err
				}
			}

			return store.Update(ctx, func(tx kv.Tx) error {
				bkt, err := tx.Bucket(bucketBucket)
				if err != nil {
					return err
				}

				values, err := bkt.GetBatch(ids...)
				if err != nil {
					return err
				}

				for i, value := range values {
					var b bucket
					if err := json.Unmarshal(value, &b); err != nil {
						return err
					}

					if b.ShardGroupDuration == 0 {
						// Backfill the duration using the same method used
						// to dervie the value within the storage engine.
						b.ShardGroupDuration = meta.NormalisedShardDuration(0, b.RetentionPeriod)
					}

					updated, err := json.Marshal(b)
					if err != nil {
						return err
					}
					if err := bkt.Put(ids[i], updated); err != nil {
						return err
					}
				}

				return nil
			})
		}

		for i := 0; i < len(buckets); i += batchSize {
			end := i + batchSize
			if end > len(buckets) {
				end = len(buckets)
			}
			if err := writeBatch(buckets[i:end]); err != nil {
				return err
			}
		}

		return nil
	},
)
