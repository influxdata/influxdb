package store

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influxd/internal/profile"
	"github.com/influxdata/influxdb/internal/fs"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/spf13/cobra"
)

var RootCommand = &cobra.Command{
	Use: "store",
}

var storeFlags struct {
	enginePath string
	profile    profile.Config
}

func newEngine(ctx context.Context) (*storage.Engine, error) {
	engine := storage.NewEngine(storeFlags.enginePath, storage.NewConfig())
	if err := engine.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open engine: %v", err)
	}
	return engine, nil
}

func init() {
	dir, err := fs.InfluxDir()
	if err != nil {
		panic(fmt.Errorf("failed to determine influx directory: %v", err))
	}

	flagSet := RootCommand.PersistentFlags()
	flagSet.SortFlags = false

	flagSet.StringVar(&storeFlags.enginePath, "engine-path", filepath.Join(dir, "engine"), "path to persistent engine files")
	flagSet.StringVar(&storeFlags.profile.CPU, "cpuprofile", "", "Collect a CPU profile")
	flagSet.StringVar(&storeFlags.profile.Memory, "memprofile", "", "Collect a memory profile")
}

type orgBucket struct {
	orgID, bucketID string
}

func (o *orgBucket) AddFlags(cmd *cobra.Command) {
	flagSet := cmd.Flags()
	flagSet.StringVar(&o.orgID, "org-id", "", "organization id")
	flagSet.StringVar(&o.bucketID, "bucket-id", "", "bucket id")

	_ = cmd.MarkFlagRequired("org-id")
	_ = cmd.MarkFlagRequired("bucket-id")
}

func (o *orgBucket) OrgBucketID() (orgID, bucketID influxdb.ID, err error) {
	if id, err := influxdb.IDFromString(o.orgID); err != nil {
		return influxdb.InvalidID(), influxdb.InvalidID(), err
	} else {
		orgID = *id
	}

	if id, err := influxdb.IDFromString(o.bucketID); err != nil {
		return influxdb.InvalidID(), influxdb.InvalidID(), err
	} else {
		bucketID = *id
	}

	return
}

func (o *orgBucket) Name() ([influxdb.IDLength]byte, error) {
	org, bucket, err := o.OrgBucketID()
	return tsdb.EncodeName(org, bucket), err
}
