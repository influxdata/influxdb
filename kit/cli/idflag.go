package cli

import (
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Wrapper for influxdb.ID
type idValue platform.ID

func newIDValue(val platform.ID, p *platform.ID) *idValue {
	*p = val
	return (*idValue)(p)
}

func (i *idValue) String() string { return platform.ID(*i).String() }
func (i *idValue) Set(s string) error {
	id, err := platform.IDFromString(s)
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
func IDVar(fs *pflag.FlagSet, p *platform.ID, name string, value platform.ID, usage string) {
	IDVarP(fs, p, name, "", value, usage)
}

// IDVarP is like IDVar, but accepts a shorthand letter that can be used after a single dash.
func IDVarP(fs *pflag.FlagSet, p *platform.ID, name, shorthand string, value platform.ID, usage string) {
	fs.VarP(newIDValue(value, p), name, shorthand, usage)
}

type OrgBucket struct {
	Org    platform.ID
	Bucket platform.ID
}

func (o *OrgBucket) AddFlags(cmd *cobra.Command) {
	fs := cmd.Flags()
	IDVar(fs, &o.Org, "org-id", platform.InvalidID(), "organization id")
	IDVar(fs, &o.Bucket, "bucket-id", platform.InvalidID(), "bucket id")
}

func (o *OrgBucket) OrgBucketID() (orgID, bucketID platform.ID) {
	return o.Org, o.Bucket
}

func (o *OrgBucket) Name() [platform.IDLength]byte {
	// TODO: FIX THIS
	panic("TODO: Fix")
}
