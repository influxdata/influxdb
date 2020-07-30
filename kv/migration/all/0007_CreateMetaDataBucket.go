package all

import (
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
)

var Migration0007_CreateMetaDataBucket = migration.CreateBuckets(
	"Create TSM metadata buckets",
	meta.BucketName)
