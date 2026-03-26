package backup

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
)

type CompressionLevel int

const (
	DefaultCompression CompressionLevel = iota
	FullCompression
	SpeedyCompression
	NoCompression
)

func NewCompressionLevelFromString(s string) (CompressionLevel, error) {
	switch s {
	case "default":
		return DefaultCompression, nil
	case "full":
		return FullCompression, nil
	case "speedy":
		return SpeedyCompression, nil
	case "none":
		return NoCompression, nil
	default:
		return -1, fmt.Errorf("unknown compression level: %q, required: [default, full, speedy, none]", s)
	}
}

// GzipLevel returns the compress/gzip constant corresponding to this CompressionLevel.
func (cl CompressionLevel) GzipLevel() int {
	switch cl {
	case FullCompression:
		return gzip.BestCompression
	case SpeedyCompression:
		return gzip.BestSpeed
	case NoCompression:
		return gzip.NoCompression
	default:
		return gzip.DefaultCompression
	}
}

type BucketManifestWriter struct {
	bs influxdb.BucketService
	os influxdb.OrganizationService
	mc *meta.Client
}

func NewBucketManifestWriter(bs influxdb.BucketService, os influxdb.OrganizationService, mc *meta.Client) BucketManifestWriter {
	return BucketManifestWriter{
		bs: bs,
		os: os,
		mc: mc,
	}
}

// WriteManifest writes a bucket manifest describing all of the buckets that exist in the database.
// It is intended to be used to write to an HTTP response after appropriate measures have been taken
// to ensure that the request is authorized.
func (b BucketManifestWriter) WriteManifest(ctx context.Context, w io.Writer) error {
	bkts, _, err := b.bs.FindBuckets(ctx, influxdb.BucketFilter{})
	if err != nil {
		return err
	}

	l := make([]influxdb.BucketMetadataManifest, 0, len(bkts))

	for _, bkt := range bkts {
		org, err := b.os.FindOrganizationByID(ctx, bkt.OrgID)
		if err != nil {
			return err
		}

		dbInfo := b.mc.Database(bkt.ID.String())

		var description *string
		if bkt.Description != "" {
			description = &bkt.Description
		}

		l = append(l, influxdb.BucketMetadataManifest{
			OrganizationID:         bkt.OrgID,
			OrganizationName:       org.Name,
			BucketID:               bkt.ID,
			BucketName:             bkt.Name,
			Description:            description,
			DefaultRetentionPolicy: dbInfo.DefaultRetentionPolicy,
			RetentionPolicies:      retentionPolicyToManifest(dbInfo.RetentionPolicies),
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
