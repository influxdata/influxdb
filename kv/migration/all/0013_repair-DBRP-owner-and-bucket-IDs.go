package all

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
)

var Migration0013_RepairDBRPOwnerAndBucketIDs = UpOnlyMigration(
	"repair DBRP owner and bucket IDs",
	func(ctx context.Context, store kv.SchemaStore) error {
		type oldStyleMapping struct {
			ID              platform.ID `json:"id"`
			Database        string      `json:"database"`
			RetentionPolicy string      `json:"retention_policy"`
			Default         bool        `json:"default"`

			// These 2 fields were renamed.
			OrganizationID platform.ID `json:"organization_id"`
			BucketID       platform.ID `json:"bucket_id"`
		}

		// Collect DBRPs that are using the old schema.
		var mappings []*oldStyleMapping
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
				var mapping oldStyleMapping
				if err := json.Unmarshal(v, &mapping); err != nil {
					return false, err
				}

				// DBRPs that are already stored in the new schema will end up with
				// invalid (zero) values for the 2 ID fields when unmarshalled using
				// the old JSON schema.
				if mapping.OrganizationID.Valid() && mapping.BucketID.Valid() {
					mappings = append(mappings, &mapping)
				}

				return true, nil
			})
		}); err != nil {
			return err
		}

		type newStyleDbrpMapping struct {
			ID              platform.ID `json:"id"`
			Database        string      `json:"database"`
			RetentionPolicy string      `json:"retention_policy"`
			Default         bool        `json:"default"`

			// New names for the 2 renamed fields.
			OrganizationID platform.ID `json:"orgID"`
			BucketID       platform.ID `json:"bucketID"`
		}
		batchSize := 100
		writeBatch := func(batch []*oldStyleMapping) (err error) {
			ids := make([][]byte, len(batch))
			for i, mapping := range batch {
				ids[i], err = mapping.ID.Encode()
				if err != nil {
					return
				}
			}

			return store.Update(ctx, func(tx kv.Tx) error {
				bkt, err := tx.Bucket(dbrpBucket)
				if err != nil {
					return err
				}

				values, err := bkt.GetBatch(ids...)
				if err != nil {
					return err
				}

				for i, value := range values {
					var mapping newStyleDbrpMapping
					if err := json.Unmarshal(value, &mapping); err != nil {
						return err
					}

					if !mapping.OrganizationID.Valid() {
						mapping.OrganizationID = batch[i].OrganizationID
					}
					if !mapping.BucketID.Valid() {
						mapping.BucketID = batch[i].BucketID
					}

					updated, err := json.Marshal(mapping)
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

		for i := 0; i < len(mappings); i += batchSize {
			end := i + batchSize
			if end > len(mappings) {
				end = len(mappings)
			}
			if err := writeBatch(mappings[i:end]); err != nil {
				return err
			}
		}

		return nil
	},
)
