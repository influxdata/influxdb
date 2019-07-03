package cli

import (
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/spf13/cobra"
)

// The ID type can be used to
type ID struct {
	influxdb.ID
}

// Set parses the hex string s and sets the value of i.
func (i *ID) Set(v string) error {
	return i.Decode([]byte(v))
}

func (i *ID) Type() string {
	return "ID"
}

type OrgBucket struct {
	Org    ID
	Bucket ID
}

func (o *OrgBucket) AddFlags(cmd *cobra.Command) {
	flagSet := cmd.Flags()
	flagSet.Var(&o.Org, "org-id", "organization id")
	flagSet.Var(&o.Bucket, "bucket-id", "bucket id")
}

func (o *OrgBucket) OrgBucketID() (orgID, bucketID influxdb.ID) {
	return o.Org.ID, o.Bucket.ID
}

func (o *OrgBucket) Name() [influxdb.IDLength]byte {
	return tsdb.EncodeName(o.OrgBucketID())
}
