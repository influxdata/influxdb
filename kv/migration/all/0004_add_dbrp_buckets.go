package all

import "github.com/influxdata/influxdb/v2/kv/migration"

var (
	dbrpBucket        = []byte("dbrpv1")
	dbrpIndexBucket   = []byte("dbrpbyorganddbindexv1")
	dbrpDefaultBucket = []byte("dbrpdefaultv1")
)

// Migration0004_AddDbrpBuckets creates the buckets necessary for the DBRP Service to operate.
var Migration0004_AddDbrpBuckets = migration.CreateBuckets(
	"create DBRP buckets",
	dbrpBucket,
	dbrpIndexBucket,
	dbrpDefaultBucket,
)
