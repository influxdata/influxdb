package backup

import (
	"context"
	"encoding/json"
	"io"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"go.uber.org/zap"
)

type BucketManifestWriter struct {
	ts     *tenant.Service
	mc     *meta.Client
	logger *zap.Logger
}

func NewBucketManifestWriter(ts *tenant.Service, mc *meta.Client) BucketManifestWriter {
	return BucketManifestWriter{
		ts: ts,
		mc: mc,
	}
}

func (b *BucketManifestWriter) WithLogger(logger *zap.Logger) {
	b.logger = logger
}

// WriteManifest writes a bucket manifest describing all of the buckets that exist in the database.
// It is intended to be used to write to an HTTP response after appropriate measures have been taken
// to ensure that the request is authorized.
func (b BucketManifestWriter) WriteManifest(ctx context.Context, w io.Writer) error {
	bkts, _, err := b.ts.FindBuckets(ctx, influxdb.BucketFilter{})
	if err != nil {
		return err
	}

	l := make([]influxdb.BucketMetadataManifest, 0, len(bkts))

	for _, bkt := range bkts {
		org, err := b.ts.OrganizationService.FindOrganizationByID(ctx, bkt.OrgID)
		if err != nil {
			return err
		}

		dbInfo := b.mc.Database(bkt.ID.String())
		if dbInfo == nil {
			b.logger.Error("Backup: could not find database", zap.String("id", bkt.ID.String()))
			continue
		}

		var description *string
		if bkt.Description != "" {
			description = &bkt.Description
		}
		rpManifests := retentionPolicyToManifest(dbInfo.RetentionPolicies)

		if b.logger != nil {
			for _, rpManifest := range rpManifests {
				for _, sg := range rpManifest.ShardGroups {
					if len(sg.Shards) <= 0 && sg.DeletedAt == nil {
						b.logger.Warn("Backup: ShardGroup has not been deleted and has no shards",
							zap.Uint64("shard-group-id", sg.ID), zap.Time("start-time", sg.StartTime),
							zap.Time("end-time", sg.EndTime))
					}
				}
			}
		}

		l = append(l, influxdb.BucketMetadataManifest{
			OrganizationID:         bkt.OrgID,
			OrganizationName:       org.Name,
			BucketID:               bkt.ID,
			BucketName:             bkt.Name,
			Description:            description,
			DefaultRetentionPolicy: dbInfo.DefaultRetentionPolicy,
			RetentionPolicies:      rpManifests,
		})
	}

	return json.NewEncoder(w).Encode(&l)
}

// retentionPolicyToManifest and the various similar functions that follow are for converting
// from the structs in the meta package to the manifest structs
func retentionPolicyToManifest(meta []meta.RetentionPolicyInfo) []influxdb.RetentionPolicyManifest {
	r := make([]influxdb.RetentionPolicyManifest, 0, len(meta))

	for _, m := range meta {
		r = append(r, influxdb.RetentionPolicyManifest{
			Name:               m.Name,
			ReplicaN:           m.ReplicaN,
			Duration:           m.Duration,
			ShardGroupDuration: m.ShardGroupDuration,
			ShardGroups:        shardGroupToManifest(m.ShardGroups),
			Subscriptions:      subscriptionInfosToManifest(m.Subscriptions),
		})
	}

	return r
}

func subscriptionInfosToManifest(subInfos []meta.SubscriptionInfo) []influxdb.SubscriptionManifest {
	r := make([]influxdb.SubscriptionManifest, 0, len(subInfos))

	for _, s := range subInfos {
		r = append(r, influxdb.SubscriptionManifest(s))
	}

	return r
}

func shardGroupToManifest(shardGroups []meta.ShardGroupInfo) []influxdb.ShardGroupManifest {
	r := make([]influxdb.ShardGroupManifest, 0, len(shardGroups))

	for _, s := range shardGroups {
		deletedAt := &s.DeletedAt
		truncatedAt := &s.TruncatedAt

		// set deletedAt and truncatedAt to nil rather than their zero values so that the fields
		// can be properly omitted from the JSON response if they are empty
		if deletedAt.IsZero() {
			deletedAt = nil
		}

		if truncatedAt.IsZero() {
			truncatedAt = nil
		}

		r = append(r, influxdb.ShardGroupManifest{
			ID:          s.ID,
			StartTime:   s.StartTime,
			EndTime:     s.EndTime,
			DeletedAt:   deletedAt,
			TruncatedAt: truncatedAt,
			Shards:      shardInfosToManifest(s.Shards),
		})
	}

	return r
}

func shardInfosToManifest(shards []meta.ShardInfo) []influxdb.ShardManifest {
	r := make([]influxdb.ShardManifest, 0, len(shards))

	for _, s := range shards {
		r = append(r, influxdb.ShardManifest{
			ID:          s.ID,
			ShardOwners: shardOwnersToManifest(s.Owners),
		})
	}

	return r
}

func shardOwnersToManifest(shardOwners []meta.ShardOwner) []influxdb.ShardOwner {
	r := make([]influxdb.ShardOwner, 0, len(shardOwners))

	for _, s := range shardOwners {
		r = append(r, influxdb.ShardOwner(s))
	}

	return r
}
