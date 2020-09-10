package cli

import (
	"github.com/influxdata/influxdb/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Wrapper for influxdb.ID
type idValue influxdb.ID

func newIDValue(val influxdb.ID, p *influxdb.ID) *idValue {
	*p = val
	return (*idValue)(p)
}

func (i *idValue) String() string { return influxdb.ID(*i).String() }
func (i *idValue) Set(s string) error {
	id, err := influxdb.IDFromString(s)
	if err != nil {
		return err
	}
	*i = idValue(*id)
	return nil
}

func (i *idValue) Type() string {
	return "ID"
}

// IDVar defines an influxdb.ID flag with specified name, default value, and usage string.
// The argument p points to an influxdb.ID variable in which to store the value of the flag.
func IDVar(fs *pflag.FlagSet, p *influxdb.ID, name string, value influxdb.ID, usage string) {
	IDVarP(fs, p, name, "", value, usage)
}

// IDVarP is like IDVar, but accepts a shorthand letter that can be used after a single dash.
func IDVarP(fs *pflag.FlagSet, p *influxdb.ID, name, shorthand string, value influxdb.ID, usage string) {
	fs.VarP(newIDValue(value, p), name, shorthand, usage)
}

type OrgBucket struct {
	Org    influxdb.ID
	Bucket influxdb.ID
}

func (o *OrgBucket) AddFlags(cmd *cobra.Command) {
	fs := cmd.Flags()
	IDVar(fs, &o.Org, "org-id", influxdb.InvalidID(), "organization id")
	IDVar(fs, &o.Bucket, "bucket-id", influxdb.InvalidID(), "bucket id")
}

func (o *OrgBucket) OrgBucketID() (orgID, bucketID influxdb.ID) {
	return o.Org, o.Bucket
}

func (o *OrgBucket) Name() [influxdb.IDLength]byte {
	// TODO: FIX THIS
	panic("TODO: Fix")
}
