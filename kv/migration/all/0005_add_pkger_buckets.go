package all

import "github.com/influxdata/influxdb/v2/kv/migration"

var (
	pkgerStacksBucket     = []byte("v1_pkger_stacks")
	pkgerStackIndexBucket = []byte("v1_pkger_stacks_index")
)

// Migration0005_AddPkgerBuckets creates the buckets necessary for the pkger service to operate.
var Migration0005_AddPkgerBuckets = migration.CreateBuckets(
	"create pkger stacks buckets",
	pkgerStacksBucket,
	pkgerStackIndexBucket,
)
