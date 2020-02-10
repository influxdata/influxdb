package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"
	"github.com/spf13/cobra"
)

type bucketSVCsFn func() (influxdb.BucketService, influxdb.OrganizationService, error)

func cmdBucket(opts ...genericCLIOptFn) *cobra.Command {
	return newCmdBucketBuilder(newBucketSVCs, opts...).cmd()
}

type cmdBucketBuilder struct {
	genericCLIOpts

	svcFn bucketSVCsFn

	id          string
	headers     bool
	name        string
	description string
	org         organization
	retention   time.Duration
}

func newCmdBucketBuilder(svcsFn bucketSVCsFn, opts ...genericCLIOptFn) *cmdBucketBuilder {
	opt := genericCLIOpts{
		in: os.Stdin,
		w:  os.Stdout,
	}
	for _, o := range opts {
		o(&opt)
	}

	return &cmdBucketBuilder{
		genericCLIOpts: opt,
		svcFn:          svcsFn,
	}
}

func (b *cmdBucketBuilder) cmd() *cobra.Command {
	cmd := b.newCmd("bucket", nil)
	cmd.Short = "Bucket management commands"
	cmd.TraverseChildren = true
	cmd.Run = seeHelp
	cmd.AddCommand(
		b.cmdCreate(),
		b.cmdDelete(),
		b.cmdFind(),
		b.cmdUpdate(),
	)

	return cmd
}

func (b *cmdBucketBuilder) cmdCreate() *cobra.Command {
	cmd := b.newCmd("create", b.cmdCreateRunEFn)
	cmd.Short = "Create bucket"

	opts := flagOpts{
		{
			DestP:    &b.name,
			Flag:     "name",
			Short:    'n',
			EnvVar:   "BUCKET_NAME",
			Desc:     "New bucket name",
			Required: true,
		},
	}
	opts.mustRegister(cmd)

	cmd.Flags().StringVarP(&b.description, "description", "d", "", "Description of bucket that will be created")
	cmd.Flags().DurationVarP(&b.retention, "retention", "r", 0, "Duration in nanoseconds data will live in bucket")
	b.org.register(cmd, false)

	return cmd
}

func (b *cmdBucketBuilder) cmdCreateRunEFn(*cobra.Command, []string) error {
	if err := b.org.validOrgFlags(); err != nil {
		return err
	}

	bktSVC, orgSVC, err := b.svcFn()
	if err != nil {
		return err
	}

	bkt := &influxdb.Bucket{
		Name:            b.name,
		Description:     b.description,
		RetentionPeriod: b.retention,
	}
	bkt.OrgID, err = b.org.getID(orgSVC)
	if err != nil {
		return err
	}

	if err := bktSVC.CreateBucket(context.Background(), bkt); err != nil {
		return fmt.Errorf("failed to create bucket: %v", err)
	}

	w := b.newTabWriter()
	w.WriteHeaders("ID", "Name", "Retention", "OrganizationID")
	w.Write(map[string]interface{}{
		"ID":             bkt.ID.String(),
		"Name":           bkt.Name,
		"Retention":      bkt.RetentionPeriod,
		"OrganizationID": bkt.OrgID.String(),
	})
	w.Flush()

	return nil
}

func (b *cmdBucketBuilder) cmdDelete() *cobra.Command {
	cmd := b.newCmd("delete", b.cmdDeleteRunEFn)
	cmd.Short = "Delete bucket"

	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The bucket ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func (b *cmdBucketBuilder) cmdDeleteRunEFn(cmd *cobra.Command, args []string) error {
	bktSVC, _, err := b.svcFn()
	if err != nil {
		return err
	}

	var id influxdb.ID
	if err := id.DecodeFromString(b.id); err != nil {
		return fmt.Errorf("failed to decode bucket id %q: %v", b.id, err)
	}

	ctx := context.Background()
	bkt, err := bktSVC.FindBucketByID(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to find bucket with id %q: %v", id, err)
	}

	if err := bktSVC.DeleteBucket(ctx, id); err != nil {
		return fmt.Errorf("failed to delete bucket with id %q: %v", id, err)
	}

	w := b.newTabWriter()
	w.WriteHeaders("ID", "Name", "Retention", "OrganizationID", "Deleted")
	w.Write(map[string]interface{}{
		"ID":             bkt.ID.String(),
		"Name":           bkt.Name,
		"Retention":      bkt.RetentionPeriod,
		"OrganizationID": bkt.OrgID.String(),
		"Deleted":        true,
	})
	w.Flush()

	return nil
}

func (b *cmdBucketBuilder) cmdFind() *cobra.Command {
	cmd := b.newCmd("find", b.cmdFindRunEFn)
	cmd.Short = "Find buckets"

	opts := flagOpts{
		{
			DestP:  &b.name,
			Flag:   "name",
			Short:  'n',
			EnvVar: "BUCKET_NAME",
			Desc:   "The bucket name",
		},
	}
	opts.mustRegister(cmd)

	b.org.register(cmd, false)
	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The bucket ID")
	cmd.Flags().BoolVar(&b.headers, "headers", true, "To print the table headers; defaults true")

	return cmd
}

func (b *cmdBucketBuilder) cmdFindRunEFn(cmd *cobra.Command, args []string) error {
	if err := b.org.validOrgFlags(); err != nil {
		return err
	}

	bktSVC, _, err := b.svcFn()
	if err != nil {
		return err
	}

	var filter influxdb.BucketFilter
	if b.name != "" {
		filter.Name = &b.name
	}
	if b.id != "" {
		id, err := influxdb.IDFromString(b.id)
		if err != nil {
			return fmt.Errorf("failed to decode bucket id %q: %v", b.id, err)
		}
		filter.ID = id
	}
	if b.org.id != "" {
		orgID, err := influxdb.IDFromString(b.org.id)
		if err != nil {
			return fmt.Errorf("failed to decode org id %q: %v", b.org.id, err)
		}
		filter.OrganizationID = orgID
	}
	if b.org.name != "" {
		filter.Org = &b.org.name
	}

	buckets, _, err := bktSVC.FindBuckets(context.Background(), filter)
	if err != nil {
		return fmt.Errorf("failed to retrieve buckets: %s", err)
	}

	w := b.newTabWriter()
	w.HideHeaders(!b.headers)
	w.WriteHeaders("ID", "Name", "Retention", "OrganizationID")
	for _, b := range buckets {
		w.Write(map[string]interface{}{
			"ID":             b.ID.String(),
			"Name":           b.Name,
			"Retention":      b.RetentionPeriod,
			"OrganizationID": b.OrgID.String(),
		})
	}
	w.Flush()

	return nil
}

func (b *cmdBucketBuilder) cmdUpdate() *cobra.Command {
	cmd := b.newCmd("update", b.cmdUpdateRunEFn)
	cmd.Short = "Update bucket"

	opts := flagOpts{
		{
			DestP:  &b.name,
			Flag:   "name",
			Short:  'n',
			EnvVar: "BUCKET_NAME",
			Desc:   "New bucket name",
		},
	}
	opts.mustRegister(cmd)

	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The bucket ID (required)")
	cmd.Flags().StringVarP(&b.description, "description", "d", "", "Description of bucket that will be created")
	cmd.MarkFlagRequired("id")
	cmd.Flags().DurationVarP(&b.retention, "retention", "r", 0, "New duration data will live in bucket")

	return cmd
}

func (b *cmdBucketBuilder) cmdUpdateRunEFn(cmd *cobra.Command, args []string) error {
	bktSVC, _, err := b.svcFn()
	if err != nil {
		return err
	}

	var id influxdb.ID
	if err := id.DecodeFromString(b.id); err != nil {
		return fmt.Errorf("failed to decode bucket id %q: %v", b.id, err)
	}

	var update influxdb.BucketUpdate
	if b.name != "" {
		update.Name = &b.name
	}
	if b.description != "" {
		update.Description = &b.description
	}
	if b.retention != 0 {
		update.RetentionPeriod = &b.retention
	}

	bkt, err := bktSVC.UpdateBucket(context.Background(), id, update)
	if err != nil {
		return fmt.Errorf("failed to update bucket: %v", err)
	}

	w := b.newTabWriter()
	w.WriteHeaders("ID", "Name", "Retention", "OrganizationID")
	w.Write(map[string]interface{}{
		"ID":             bkt.ID.String(),
		"Name":           bkt.Name,
		"Retention":      bkt.RetentionPeriod,
		"OrganizationID": bkt.OrgID.String(),
	})
	w.Flush()

	return nil
}

func newBucketSVCs() (influxdb.BucketService, influxdb.OrganizationService, error) {
	httpClient, err := newHTTPClient()
	if err != nil {
		return nil, nil, err
	}

	orgSvc := &http.OrganizationService{Client: httpClient}

	return &http.BucketService{Client: httpClient}, orgSvc, nil
}
