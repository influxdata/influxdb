package all

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
)

// Migration0011_PopulateDashboardsOwnerId backfills owner IDs on dashboards based on the presence of user resource mappings
var Migration0011_PopulateDashboardsOwnerId = UpOnlyMigration("populate dashboards owner id", func(ctx context.Context, store kv.SchemaStore) error {
	var urmBucket = []byte("userresourcemappingsv1")
	type userResourceMapping struct {
		UserID       platform.ID           `json:"userID"`
		UserType     influxdb.UserType     `json:"userType"`
		MappingType  influxdb.MappingType  `json:"mappingType"`
		ResourceType influxdb.ResourceType `json:"resourceType"`
		ResourceID   platform.ID           `json:"resourceID"`
	}

	var mappings []*userResourceMapping
	if err := store.View(ctx, func(tx kv.Tx) error {
		bkt, err := tx.Bucket(urmBucket)
		if err != nil {
			return err
		}

		cursor, err := bkt.ForwardCursor(nil)
		if err != nil {
			return err
		}

		// collect all dashboard mappings
		return kv.WalkCursor(ctx, cursor, func(_, v []byte) (bool, error) {
			var mapping userResourceMapping
			if err := json.Unmarshal(v, &mapping); err != nil {
				return false, err
			}

			// we're interesting in dashboard owners
			if mapping.ResourceType == influxdb.DashboardsResourceType &&
				mapping.UserType == influxdb.Owner {
				mappings = append(mappings, &mapping)
			}

			return true, nil
		})
	}); err != nil {
		return err
	}

	var dashboardsBucket = []byte("dashboardsv2")
	// dashboard represents all visual and query data for a dashboard.
	type dashboard struct {
		ID             platform.ID            `json:"id,omitempty"`
		OrganizationID platform.ID            `json:"orgID,omitempty"`
		Name           string                 `json:"name"`
		Description    string                 `json:"description"`
		Cells          []*influxdb.Cell       `json:"cells"`
		Meta           influxdb.DashboardMeta `json:"meta"`
		OwnerID        *platform.ID           `json:"owner,omitempty"`
	}

	var (
		batchSize = 100
		flush     = func(batch []*userResourceMapping) (err error) {
			ids := make([][]byte, len(batch))
			for i, urm := range batch {
				ids[i], err = urm.ResourceID.Encode()
				if err != nil {
					return
				}
			}

			return store.Update(ctx, func(tx kv.Tx) error {
				bkt, err := tx.Bucket(dashboardsBucket)
				if err != nil {
					return err
				}

				values, err := bkt.GetBatch(ids...)
				if err != nil {
					return err
				}

				for i, value := range values {
					var dashboard dashboard
					if err := json.Unmarshal(value, &dashboard); err != nil {
						return err
					}

					if dashboard.OwnerID != nil {
						fmt.Printf("dashboard %q already has owner %q", dashboard.ID, dashboard.OwnerID)
						continue
					}

					// update bucket owner to owner dashboard urm mapping user target
					dashboard.OwnerID = &batch[i].UserID

					updated, err := json.Marshal(dashboard)
					if err != nil {
						return err
					}

					// update bucket entry
					return bkt.Put(ids[i], updated)
				}

				return nil
			})
		}
	)

	for i := 0; i < len(mappings); i += batchSize {
		end := i + batchSize
		if end > len(mappings) {
			end = len(mappings)
		}

		flush(mappings[i:end])
	}

	return nil
})
