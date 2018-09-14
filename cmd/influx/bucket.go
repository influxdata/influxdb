package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/cmd/influx/internal"
	"github.com/influxdata/platform/http"
	"github.com/spf13/cobra"
)

// Bucket Command
var bucketCmd = &cobra.Command{
	Use:   "bucket",
	Short: "bucket related commands",
	Run:   bucketF,
}

func bucketF(cmd *cobra.Command, args []string) {
	cmd.Usage()
}

// Create Command
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
		Run:   bucketCreateF,
	}

	bucketCreateCmd.Flags().StringVarP(&bucketCreateFlags.name, "name", "n", "", "name of bucket that will be created")
	bucketCreateCmd.Flags().DurationVarP(&bucketCreateFlags.retention, "retention", "r", 0, "duration data will live in bucket")
	bucketCreateCmd.Flags().StringVarP(&bucketCreateFlags.org, "org", "o", "", "name of the organization that owns the bucket")
	bucketCreateCmd.Flags().StringVarP(&bucketCreateFlags.orgID, "org-id", "", "", "id of the organization that owns the bucket")
	bucketCreateCmd.MarkFlagRequired("name")

	bucketCmd.AddCommand(bucketCreateCmd)
}

func bucketCreateF(cmd *cobra.Command, args []string) {
	if bucketCreateFlags.org != "" && bucketCreateFlags.orgID != "" {
		fmt.Println("must specify exactly one of org or org-id")
		cmd.Usage()
		os.Exit(1)
	}

	s := &http.BucketService{
		Addr:  flags.host,
		Token: flags.token,
	}

	b := &platform.Bucket{
		Name:            bucketCreateFlags.name,
		RetentionPeriod: bucketCreateFlags.retention,
	}

	if bucketCreateFlags.org != "" {
		b.Organization = bucketCreateFlags.org
	}

	if bucketCreateFlags.orgID != "" {
		var id platform.ID
		if err := id.DecodeFromString(bucketCreateFlags.orgID); err != nil {
			fmt.Printf("error parsing organization id: %v\n", err)
			os.Exit(1)
		}
		b.OrganizationID = id
	}

	if err := s.CreateBucket(context.Background(), b); err != nil {
		fmt.Println(err)
		os.Exit(1)
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
}

// Find Command
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
		Run:   bucketFindF,
	}

	bucketFindCmd.Flags().StringVarP(&bucketFindFlags.name, "name", "n", "", "bucket name")
	bucketFindCmd.Flags().StringVarP(&bucketFindFlags.id, "id", "i", "", "bucket ID")
	bucketFindCmd.Flags().StringVarP(&bucketFindFlags.orgID, "org-id", "", "", "bucket organization ID")
	bucketFindCmd.Flags().StringVarP(&bucketFindFlags.org, "org", "o", "", "bucket organization name")

	bucketCmd.AddCommand(bucketFindCmd)
}

func bucketFindF(cmd *cobra.Command, args []string) {
	s := &http.BucketService{
		Addr:  flags.host,
		Token: flags.token,
	}

	filter := platform.BucketFilter{}
	if bucketFindFlags.name != "" {
		filter.Name = &bucketFindFlags.name
	}

	if bucketFindFlags.id != "" {
		filter.ID = &platform.ID{}
		err := filter.ID.DecodeFromString(bucketFindFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	if bucketFindFlags.orgID != "" && bucketFindFlags.org != "" {
		fmt.Println("must specify at exactly one of org and org-id")
		cmd.Usage()
		os.Exit(1)
	}

	if bucketFindFlags.orgID != "" {
		filter.OrganizationID = &platform.ID{}
		err := filter.OrganizationID.DecodeFromString(bucketFindFlags.orgID)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	if bucketFindFlags.org != "" {
		filter.Organization = &bucketFindFlags.org
	}

	buckets, _, err := s.FindBuckets(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
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
}

// Update Command
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
		Run:   bucketUpdateF,
	}

	bucketUpdateCmd.Flags().StringVarP(&bucketUpdateFlags.id, "id", "i", "", "bucket ID (required)")
	bucketUpdateCmd.Flags().StringVarP(&bucketUpdateFlags.name, "name", "n", "", "new bucket name")
	bucketUpdateCmd.Flags().DurationVarP(&bucketUpdateFlags.retention, "retention", "r", 0, "new duration data will live in bucket")
	bucketUpdateCmd.MarkFlagRequired("id")

	bucketCmd.AddCommand(bucketUpdateCmd)
}

func bucketUpdateF(cmd *cobra.Command, args []string) {
	s := &http.BucketService{
		Addr:  flags.host,
		Token: flags.token,
	}

	var id platform.ID
	if err := id.DecodeFromString(bucketUpdateFlags.id); err != nil {
		fmt.Println(err)
		os.Exit(1)
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
		fmt.Println(err)
		os.Exit(1)
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
}

// Delete command
type BucketDeleteFlags struct {
	id string
}

var bucketDeleteFlags BucketDeleteFlags

func bucketDeleteF(cmd *cobra.Command, args []string) {
	s := &http.BucketService{
		Addr:  flags.host,
		Token: flags.token,
	}

	var id platform.ID
	err := id.DecodeFromString(bucketDeleteFlags.id)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx := context.TODO()
	b, err := s.FindBucketByID(ctx, id)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err = s.DeleteBucket(ctx, id); err != nil {
		fmt.Println(err)
		os.Exit(1)
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
}

func init() {
	bucketDeleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete bucket",
		Run:   bucketDeleteF,
	}

	bucketDeleteCmd.Flags().StringVarP(&bucketDeleteFlags.id, "id", "i", "", "bucket id (required)")
	bucketDeleteCmd.MarkFlagRequired("id")

	bucketCmd.AddCommand(bucketDeleteCmd)
}
