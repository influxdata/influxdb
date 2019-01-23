package main

import (
	"context"
	"fmt"
	"os"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/internal/fs"
	"github.com/spf13/cobra"
)

// Bucket Command
var bucketCmd = &cobra.Command{
	Use:   "bucket",
	Short: "Bucket management commands",
	Run:   bucketF,
}

func bucketF(cmd *cobra.Command, args []string) {
	cmd.Usage()
}

// BucketCreateFlags define the Create Command
type BucketCreateFlags struct {
	name      string
	org       string
	orgID     string
	retention time.Duration
}

var bucketCreateFlags BucketCreateFlags

func init() {
	bucketCreateCmd := &cobra.Command{
		Use:   "create",
		Short: "Create bucket",
		RunE:  wrapCheckSetup(bucketCreateF),
	}

	bucketCreateCmd.Flags().StringVarP(&bucketCreateFlags.name, "name", "n", "", "Name of bucket that will be created")
	bucketCreateCmd.Flags().DurationVarP(&bucketCreateFlags.retention, "retention", "r", 0, "Duration in nanoseconds data will live in bucket")
	bucketCreateCmd.Flags().StringVarP(&bucketCreateFlags.org, "org", "o", "", "Name of the organization that owns the bucket")
	bucketCreateCmd.Flags().StringVarP(&bucketCreateFlags.orgID, "org-id", "", "", "The ID of the organization that owns the bucket")
	bucketCreateCmd.MarkFlagRequired("name")

	bucketCmd.AddCommand(bucketCreateCmd)
}

func newBucketService(f Flags) (platform.BucketService, error) {
	if flags.local {
		boltFile, err := fs.BoltFile()
		if err != nil {
			return nil, err
		}
		c := bolt.NewClient()
		c.Path = boltFile
		if err := c.Open(context.Background()); err != nil {
			return nil, err
		}

		return c, nil
	}
	return &http.BucketService{
		Addr:  flags.host,
		Token: flags.token,
	}, nil
}

func bucketCreateF(cmd *cobra.Command, args []string) error {
	if (bucketCreateFlags.org == "" && bucketCreateFlags.orgID == "") ||
		(bucketCreateFlags.org != "" && bucketCreateFlags.orgID != "") {
		return fmt.Errorf("must specify exactly one of org or org-id")
	}

	s, err := newBucketService(flags)
	if err != nil {
		return fmt.Errorf("failed to initialize bucket service client: %v", err)
	}

	b := &platform.Bucket{
		Name:            bucketCreateFlags.name,
		RetentionPeriod: bucketCreateFlags.retention,
	}

	if bucketCreateFlags.org != "" {
		b.Organization = bucketCreateFlags.org
	}

	if bucketCreateFlags.orgID != "" {
		id, err := platform.IDFromString(bucketCreateFlags.orgID)
		if err != nil {
			return fmt.Errorf("failed to decode org id %q: %v", bucketCreateFlags.orgID, err)
		}
		b.OrganizationID = *id
	}

	if err := s.CreateBucket(context.Background(), b); err != nil {
		return fmt.Errorf("failed to create bucket: %v", err)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"Retention",
		"Organization",
		"OrganizationID",
	)
	w.Write(map[string]interface{}{
		"ID":             b.ID.String(),
		"Name":           b.Name,
		"Retention":      b.RetentionPeriod,
		"Organization":   b.Organization,
		"OrganizationID": b.OrganizationID.String(),
	})
	w.Flush()

	return nil
}

// BucketFindFlags define the Find Command
type BucketFindFlags struct {
	name  string
	id    string
	org   string
	orgID string
}

var bucketFindFlags BucketFindFlags

func init() {
	bucketFindCmd := &cobra.Command{
		Use:   "find",
		Short: "Find buckets",
		RunE:  wrapCheckSetup(bucketFindF),
	}

	bucketFindCmd.Flags().StringVarP(&bucketFindFlags.name, "name", "n", "", "The bucket name")
	bucketFindCmd.Flags().StringVarP(&bucketFindFlags.id, "id", "i", "", "The bucket ID")
	bucketFindCmd.Flags().StringVarP(&bucketFindFlags.orgID, "org-id", "", "", "The bucket organization ID")
	bucketFindCmd.Flags().StringVarP(&bucketFindFlags.org, "org", "o", "", "The bucket organization name")

	bucketCmd.AddCommand(bucketFindCmd)
}

func bucketFindF(cmd *cobra.Command, args []string) error {
	s, err := newBucketService(flags)
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

	if bucketFindFlags.orgID != "" && bucketFindFlags.org != "" {
		return fmt.Errorf("must specify at exactly one of org and org-id")
	}

	if bucketFindFlags.orgID != "" {
		orgID, err := platform.IDFromString(bucketFindFlags.orgID)
		if err != nil {
			return fmt.Errorf("failed to decode org id %q: %v", bucketFindFlags.orgID, err)
		}
		filter.OrganizationID = orgID
	}

	if bucketFindFlags.org != "" {
		filter.Organization = &bucketFindFlags.org
	}

	buckets, _, err := s.FindBuckets(context.Background(), filter)
	if err != nil {
		return fmt.Errorf("failed to retrieve buckets: %s", err)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"Retention",
		"Organization",
		"OrganizationID",
	)
	for _, b := range buckets {
		w.Write(map[string]interface{}{
			"ID":             b.ID.String(),
			"Name":           b.Name,
			"Retention":      b.RetentionPeriod,
			"Organization":   b.Organization,
			"OrganizationID": b.OrganizationID.String(),
		})
	}
	w.Flush()

	return nil
}

// BucketUpdateFlags define the Update Command
type BucketUpdateFlags struct {
	id        string
	name      string
	retention time.Duration
}

var bucketUpdateFlags BucketUpdateFlags

func init() {
	bucketUpdateCmd := &cobra.Command{
		Use:   "update",
		Short: "Update bucket",
		RunE:  wrapCheckSetup(bucketUpdateF),
	}

	bucketUpdateCmd.Flags().StringVarP(&bucketUpdateFlags.id, "id", "i", "", "The bucket ID (required)")
	bucketUpdateCmd.Flags().StringVarP(&bucketUpdateFlags.name, "name", "n", "", "New bucket name")
	bucketUpdateCmd.Flags().DurationVarP(&bucketUpdateFlags.retention, "retention", "r", 0, "New duration data will live in bucket")
	bucketUpdateCmd.MarkFlagRequired("id")

	bucketCmd.AddCommand(bucketUpdateCmd)
}

func bucketUpdateF(cmd *cobra.Command, args []string) error {
	s, err := newBucketService(flags)
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
		"Organization",
		"OrganizationID",
	)
	w.Write(map[string]interface{}{
		"ID":             b.ID.String(),
		"Name":           b.Name,
		"Retention":      b.RetentionPeriod,
		"Organization":   b.Organization,
		"OrganizationID": b.OrganizationID.String(),
	})
	w.Flush()

	return nil
}

// BucketDeleteFlags define the Delete command
type BucketDeleteFlags struct {
	id string
}

var bucketDeleteFlags BucketDeleteFlags

func bucketDeleteF(cmd *cobra.Command, args []string) error {
	s, err := newBucketService(flags)
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
		"Organization",
		"OrganizationID",
		"Deleted",
	)
	w.Write(map[string]interface{}{
		"ID":             b.ID.String(),
		"Name":           b.Name,
		"Retention":      b.RetentionPeriod,
		"Organization":   b.Organization,
		"OrganizationID": b.OrganizationID.String(),
		"Deleted":        true,
	})
	w.Flush()

	return nil
}

func init() {
	bucketDeleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete bucket",
		RunE:  wrapCheckSetup(bucketDeleteF),
	}

	bucketDeleteCmd.Flags().StringVarP(&bucketDeleteFlags.id, "id", "i", "", "The bucket ID (required)")
	bucketDeleteCmd.MarkFlagRequired("id")

	bucketCmd.AddCommand(bucketDeleteCmd)
}
