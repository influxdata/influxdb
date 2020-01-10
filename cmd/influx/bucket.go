package main

import (
	"context"
	"fmt"
	"os"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/spf13/cobra"
)

func cmdBucket() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "bucket",
		Short:            "Bucket management commands",
		TraverseChildren: true,
		Run:              seeHelp,
	}
	cmd.AddCommand(
		bucketCreateCmd(),
		bucketDeleteCmd(),
		bucketFindCmd(),
		bucketUpdateCmd(),
	)

	return cmd
}

var bucketCreateFlags struct {
	name string
	organization
	retention time.Duration
}

func bucketCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create bucket",
		RunE:  wrapCheckSetup(bucketCreateF),
	}

	cmd.Flags().StringVar(&bucketCreateFlags.name, "name", "n", "Name of bucket that will be created")
	cmd.MarkFlagRequired("name")
	cmd.Flags().DurationVarP(&bucketCreateFlags.retention, "retention", "r", 0, "Duration in nanoseconds data will live in bucket")
	bucketCreateFlags.organization.register(cmd, false)

	return cmd
}

func newBucketService() (platform.BucketService, error) {
	if flags.local {
		return newLocalKVService()
	}

	client, err := newHTTPClient()
	if err != nil {
		return nil, err
	}

	return &http.BucketService{
		Client: client,
	}, nil
}

func bucketCreateF(cmd *cobra.Command, args []string) error {
	if err := bucketCreateFlags.organization.validOrgFlags(); err != nil {
		return err
	}

	s, err := newBucketService()
	if err != nil {
		return fmt.Errorf("failed to initialize bucket service client: %v", err)
	}

	b := &platform.Bucket{
		Name:            bucketCreateFlags.name,
		RetentionPeriod: bucketCreateFlags.retention,
	}

	orgSvc, err := newOrganizationService()
	if err != nil {
		return err
	}

	b.OrgID, err = bucketCreateFlags.organization.getID(orgSvc)
	if err != nil {
		return err
	}

	if err := s.CreateBucket(context.Background(), b); err != nil {
		return fmt.Errorf("failed to create bucket: %v", err)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"Retention",
		"OrganizationID",
	)
	w.Write(map[string]interface{}{
		"ID":             b.ID.String(),
		"Name":           b.Name,
		"Retention":      b.RetentionPeriod,
		"OrganizationID": b.OrgID.String(),
	})
	w.Flush()

	return nil
}

var bucketFindFlags struct {
	name    string
	id      string
	headers bool
	organization
}

func bucketFindCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "find",
		Short: "Find buckets",
		RunE:  wrapCheckSetup(bucketFindF),
	}

	opts := flagOpts{
		{
			DestP:  &bucketFindFlags.name,
			Flag:   "name",
			Short:  'n',
			EnvVar: "BUCKET_NAME",
			Desc:   "The bucket name",
		},
	}
	opts.mustRegister(cmd)

	cmd.Flags().StringVarP(&bucketFindFlags.id, "id", "i", "", "The bucket ID")
	cmd.Flags().BoolVar(&bucketFindFlags.headers, "headers", true, "To print the table headers; defaults true")
	bucketFindFlags.organization.register(cmd, false)

	return cmd
}

func bucketFindF(cmd *cobra.Command, args []string) error {
	s, err := newBucketService()
	if err != nil {
		return fmt.Errorf("failed to initialize bucket service client: %v", err)
	}

	filter := platform.BucketFilter{}
	if bucketFindFlags.name != "" {
		filter.Name = &bucketFindFlags.name
	}

	if bucketFindFlags.id != "" {
		id, err := platform.IDFromString(bucketFindFlags.id)
		if err != nil {
			return fmt.Errorf("failed to decode bucket id %q: %v", bucketFindFlags.id, err)
		}
		filter.ID = id
	}

	if err := bucketFindFlags.organization.validOrgFlags(); err != nil {
		return err
	}

	if bucketFindFlags.organization.id != "" {
		orgID, err := platform.IDFromString(bucketFindFlags.organization.id)
		if err != nil {
			return fmt.Errorf("failed to decode org id %q: %v", bucketFindFlags.organization.id, err)
		}
		filter.OrganizationID = orgID
	}

	if bucketFindFlags.organization.name != "" {
		filter.Org = &bucketFindFlags.organization.name
	}

	buckets, _, err := s.FindBuckets(context.Background(), filter)
	if err != nil {
		return fmt.Errorf("failed to retrieve buckets: %s", err)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.HideHeaders(!bucketFindFlags.headers)
	w.WriteHeaders(
		"ID",
		"Name",
		"Retention",
		"OrganizationID",
	)
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

var bucketUpdateFlags struct {
	id        string
	name      string
	retention time.Duration
}

func bucketUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update bucket",
		RunE:  wrapCheckSetup(bucketUpdateF),
	}

	opts := flagOpts{
		{
			DestP:  &bucketUpdateFlags.name,
			Flag:   "name",
			Short:  'n',
			EnvVar: "BUCKET_NAME",
			Desc:   "New bucket name",
		},
	}
	opts.mustRegister(cmd)

	cmd.Flags().StringVarP(&bucketUpdateFlags.id, "id", "i", "", "The bucket ID (required)")
	cmd.MarkFlagRequired("id")
	cmd.Flags().DurationVarP(&bucketUpdateFlags.retention, "retention", "r", 0, "New duration data will live in bucket")

	return cmd
}

func bucketUpdateF(cmd *cobra.Command, args []string) error {
	s, err := newBucketService()
	if err != nil {
		return fmt.Errorf("failed to initialize bucket service client: %v", err)
	}

	var id platform.ID
	if err := id.DecodeFromString(bucketUpdateFlags.id); err != nil {
		return fmt.Errorf("failed to decode bucket id %q: %v", bucketUpdateFlags.id, err)
	}

	update := platform.BucketUpdate{}
	if bucketUpdateFlags.name != "" {
		update.Name = &bucketUpdateFlags.name
	}
	if bucketUpdateFlags.retention != 0 {
		update.RetentionPeriod = &bucketUpdateFlags.retention
	}

	b, err := s.UpdateBucket(context.Background(), id, update)
	if err != nil {
		return fmt.Errorf("failed to update bucket: %v", err)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"Retention",
		"OrganizationID",
	)
	w.Write(map[string]interface{}{
		"ID":             b.ID.String(),
		"Name":           b.Name,
		"Retention":      b.RetentionPeriod,
		"OrganizationID": b.OrgID.String(),
	})
	w.Flush()

	return nil
}

var bucketDeleteFlags struct {
	id string
}

func bucketDeleteF(cmd *cobra.Command, args []string) error {
	s, err := newBucketService()
	if err != nil {
		return fmt.Errorf("failed to initialize bucket service client: %v", err)
	}

	var id platform.ID
	if err := id.DecodeFromString(bucketDeleteFlags.id); err != nil {
		return fmt.Errorf("failed to decode bucket id %q: %v", bucketDeleteFlags.id, err)
	}

	ctx := context.Background()
	b, err := s.FindBucketByID(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to find bucket with id %q: %v", id, err)
	}

	if err = s.DeleteBucket(ctx, id); err != nil {
		return fmt.Errorf("failed to delete bucket with id %q: %v", id, err)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"Retention",
		"OrganizationID",
		"Deleted",
	)
	w.Write(map[string]interface{}{
		"ID":             b.ID.String(),
		"Name":           b.Name,
		"Retention":      b.RetentionPeriod,
		"OrganizationID": b.OrgID.String(),
		"Deleted":        true,
	})
	w.Flush()

	return nil
}

func bucketDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete bucket",
		RunE:  wrapCheckSetup(bucketDeleteF),
	}

	cmd.Flags().StringVar(&bucketDeleteFlags.id, "id", "i", "The bucket ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}
