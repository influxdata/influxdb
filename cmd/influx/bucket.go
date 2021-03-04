package main

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/internal"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/spf13/cobra"
)

type bucketSVCsFn func() (influxdb.BucketService, influxdb.OrganizationService, error)

func cmdBucket(f *globalFlags, opt genericCLIOpts) (*cobra.Command, error) {
	builder := newCmdBucketBuilder(newBucketSVCs, f, opt)
	return builder.cmd()
}

type cmdBucketBuilder struct {
	genericCLIOpts
	*globalFlags

	svcFn bucketSVCsFn

	id          string
	hideHeaders bool
	json        bool
	name        string
	description string
	org         organization
	retention   string
}

func newCmdBucketBuilder(svcsFn bucketSVCsFn, f *globalFlags, opts genericCLIOpts) *cmdBucketBuilder {
	return &cmdBucketBuilder{
		globalFlags:    f,
		genericCLIOpts: opts,
		svcFn:          svcsFn,
	}
}

func (b *cmdBucketBuilder) cmd() (*cobra.Command, error) {
	cmd, err := b.newCmd("bucket", nil)
	if err != nil {
		return nil, err
	}
	cmd.Short = "Bucket management commands"
	cmd.TraverseChildren = true
	cmd.Run = seeHelp

	builders := []func() (*cobra.Command, error){b.cmdCreate, b.cmdDelete, b.cmdList, b.cmdUpdate}
	for _, buildCmd := range builders {
		subcommand, err := buildCmd()
		if err != nil {
			return nil, err
		}
		cmd.AddCommand(subcommand)
	}

	return cmd, nil
}

func (b *cmdBucketBuilder) cmdCreate() (*cobra.Command, error) {
	cmd, err := b.newCmd("create", b.cmdCreateRunEFn)
	if err != nil {
		return nil, err
	}
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
	if err := opts.register(b.viper, cmd); err != nil {
		return nil, err
	}

	cmd.Flags().StringVarP(&b.description, "description", "d", "", "Description of bucket that will be created")
	cmd.Flags().StringVarP(&b.retention, "retention", "r", "", "Duration bucket will retain data. 0 is infinite. Default is 0.")
	if err := b.org.register(b.viper, cmd, false); err != nil {
		return nil, err
	}
	if err := b.registerPrintFlags(cmd); err != nil {
		return nil, err
	}

	return cmd, nil
}

func (b *cmdBucketBuilder) cmdCreateRunEFn(*cobra.Command, []string) error {
	if err := b.org.validOrgFlags(b.globalFlags); err != nil {
		return err
	}

	bktSVC, orgSVC, err := b.svcFn()
	if err != nil {
		return err
	}

	dur, err := internal.RawDurationToTimeDuration(b.retention)
	if err != nil {
		return err
	}

	bkt := &influxdb.Bucket{
		Name:            b.name,
		Description:     b.description,
		RetentionPeriod: dur,
	}
	bkt.OrgID, err = b.org.getID(orgSVC)
	if err != nil {
		return err
	}

	if err := bktSVC.CreateBucket(context.Background(), bkt); err != nil {
		return fmt.Errorf("failed to create bucket: %v", err)
	}

	return b.printBuckets(bucketPrintOpt{bucket: bkt})
}

func (b *cmdBucketBuilder) cmdDelete() (*cobra.Command, error) {
	cmd, err := b.newCmd("delete", b.cmdDeleteRunEFn)
	if err != nil {
		return nil, err
	}
	cmd.Short = "Delete bucket"

	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The bucket ID, required if name isn't provided")
	cmd.Flags().StringVarP(&b.name, "name", "n", "", "The bucket name, org or org-id will be required by choosing this")
	if err := b.org.register(b.viper, cmd, false); err != nil {
		return nil, err
	}
	if err := b.registerPrintFlags(cmd); err != nil {
		return nil, err
	}

	return cmd, nil
}

func (b *cmdBucketBuilder) cmdDeleteRunEFn(cmd *cobra.Command, args []string) error {
	bktSVC, _, err := b.svcFn()
	if err != nil {
		return err
	}

	var id influxdb.ID
	var filter influxdb.BucketFilter
	if b.id == "" && b.name != "" {
		if err = b.org.validOrgFlags(&flags); err != nil {
			return err
		}
		filter.Name = &b.name
		if b.org.id != "" {
			if filter.OrganizationID, err = influxdb.IDFromString(b.org.id); err != nil {
				return err
			}
		} else if b.org.name != "" {
			filter.Org = &b.org.name
		}

	} else if err := id.DecodeFromString(b.id); err != nil {
		return fmt.Errorf("failed to decode bucket id %q: %v", b.id, err)
	}

	if id.Valid() {
		filter.ID = &id
	}

	ctx := context.Background()
	bkt, err := bktSVC.FindBucket(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to find bucket with id %q: %v", id, err)
	}
	if err := bktSVC.DeleteBucket(ctx, bkt.ID); err != nil {
		return fmt.Errorf("failed to delete bucket with id %q: %v", id, err)
	}
	return b.printBuckets(bucketPrintOpt{
		deleted: true,
		bucket:  bkt,
	})
}

func (b *cmdBucketBuilder) cmdList() (*cobra.Command, error) {
	cmd, err := b.newCmd("list", b.cmdListRunEFn)
	if err != nil {
		return nil, err
	}
	cmd.Short = "List buckets"
	cmd.Aliases = []string{"find", "ls"}

	opts := flagOpts{
		{
			DestP:  &b.name,
			Flag:   "name",
			Short:  'n',
			EnvVar: "BUCKET_NAME",
			Desc:   "The bucket name",
		},
	}
	if err := opts.register(b.viper, cmd); err != nil {
		return nil, err
	}
	if err := b.org.register(b.viper, cmd, false); err != nil {
		return nil, err
	}
	if err := b.registerPrintFlags(cmd); err != nil {
		return nil, err
	}
	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The bucket ID")

	return cmd, nil
}

func (b *cmdBucketBuilder) cmdListRunEFn(cmd *cobra.Command, args []string) error {
	if err := b.org.validOrgFlags(b.globalFlags); err != nil {
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

	return b.printBuckets(bucketPrintOpt{
		buckets: buckets,
	})
}

func (b *cmdBucketBuilder) cmdUpdate() (*cobra.Command, error) {
	cmd, err := b.newCmd("update", b.cmdUpdateRunEFn)
	if err != nil {
		return nil, err
	}
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
	if err := opts.register(b.viper, cmd); err != nil {
		return nil, err
	}

	if err := b.registerPrintFlags(cmd); err != nil {
		return nil, err
	}
	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The bucket ID (required)")
	cmd.Flags().StringVarP(&b.description, "description", "d", "", "Description of bucket that will be created")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		return nil, fmt.Errorf("failed to mark 'id' as required: %w", err)
	}
	cmd.Flags().StringVarP(&b.retention, "retention", "r", "", "Duration bucket will retain data. 0 is infinite. Default is 0.")

	return cmd, nil
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

	dur, err := internal.RawDurationToTimeDuration(b.retention)
	if err != nil {
		return err
	}
	if dur != 0 {
		update.RetentionPeriod = &dur
	}

	bkt, err := bktSVC.UpdateBucket(context.Background(), id, update)
	if err != nil {
		return fmt.Errorf("failed to update bucket: %v", err)
	}

	return b.printBuckets(bucketPrintOpt{bucket: bkt})
}

func (b *cmdBucketBuilder) newCmd(use string, runE func(*cobra.Command, []string) error) (*cobra.Command, error) {
	cmd := b.genericCLIOpts.newCmd(use, runE, true)
	if err := b.globalFlags.registerFlags(b.viper, cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

func (b *cmdBucketBuilder) registerPrintFlags(cmd *cobra.Command) error {
	return registerPrintOptions(b.viper, cmd, &b.hideHeaders, &b.json)
}

type bucketPrintOpt struct {
	deleted bool
	bucket  *influxdb.Bucket
	buckets []*influxdb.Bucket
}

func (b *cmdBucketBuilder) printBuckets(printOpt bucketPrintOpt) error {
	if b.json {
		var v interface{} = printOpt.buckets
		if printOpt.buckets == nil {
			v = printOpt.bucket
		}
		return b.writeJSON(v)
	}

	w := b.newTabWriter()
	defer w.Flush()

	w.HideHeaders(b.hideHeaders)

	headers := []string{"ID", "Name", "Retention", "Organization ID"}
	if printOpt.deleted {
		headers = append(headers, "Deleted")
	}
	w.WriteHeaders(headers...)

	if printOpt.bucket != nil {
		printOpt.buckets = append(printOpt.buckets, printOpt.bucket)
	}

	for _, bkt := range printOpt.buckets {
		m := map[string]interface{}{
			"ID":              bkt.ID.String(),
			"Name":            bkt.Name,
			"Retention":       bkt.RetentionPeriod,
			"Organization ID": bkt.OrgID.String(),
		}
		if printOpt.deleted {
			m["Deleted"] = true
		}
		w.Write(m)
	}

	return nil
}

func newBucketSVCs() (influxdb.BucketService, influxdb.OrganizationService, error) {
	httpClient, err := newHTTPClient()
	if err != nil {
		return nil, nil, err
	}

	orgSvc := &tenant.OrgClientService{Client: httpClient}

	return &tenant.BucketClientService{Client: httpClient}, orgSvc, nil
}
