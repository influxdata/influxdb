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
	"github.com/spf13/viper"
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
	name string
	organization
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
	bucketCreateCmd.MarkFlagRequired("name")
	bucketCreateFlags.organization.register(bucketCreateCmd)

	bucketCmd.AddCommand(bucketCreateCmd)
}

func newBucketService(f Flags) (platform.BucketService, error) {
	if f.local {
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

	s, err := newBucketService(flags)
	if err != nil {
		return fmt.Errorf("failed to initialize bucket service client: %v", err)
	}

	b := &platform.Bucket{
		Name:            bucketCreateFlags.name,
		RetentionPeriod: bucketCreateFlags.retention,
	}

	orgSvc, err := newOrganizationService()
	if err != nil {
		return nil
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

// BucketFindFlags define the Find Command
type BucketFindFlags struct {
	name    string
	id      string
	headers bool
	organization
}

var bucketFindFlags BucketFindFlags

func init() {
	bucketFindCmd := &cobra.Command{
		Use:   "find",
		Short: "Find buckets",
		RunE:  wrapCheckSetup(bucketFindF),
	}

	bucketFindCmd.Flags().StringVarP(&bucketFindFlags.name, "name", "n", "", "The bucket name")
	viper.BindEnv("BUCKET_NAME")
	if h := viper.GetString("BUCKET_NAME"); h != "" {
		bucketFindFlags.name = h
	}
	bucketFindCmd.Flags().StringVarP(&bucketFindFlags.id, "id", "i", "", "The bucket ID")
	bucketFindCmd.Flags().BoolVar(&bucketFindFlags.headers, "headers", true, "To print the table headers; defaults true")
	bucketFindFlags.organization.register(bucketFindCmd)

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
	viper.BindEnv("BUCKET_NAME")
	if h := viper.GetString("BUCKET_NAME"); h != "" {
		bucketFindFlags.name = h
	}

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
