package all

import "github.com/influxdata/influxdb/v2/kv/migration"

var Migration0009_LegacyAuthPasswordBuckets = migration.CreateBuckets(
	"Create legacy auth password bucket",
	[]byte("legacy/authorizationPasswordv1"))
