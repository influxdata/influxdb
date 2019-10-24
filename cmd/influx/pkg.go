package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/pkger"
	"github.com/spf13/cobra"
	input "github.com/tcnksm/go-input"
	"go.uber.org/zap"
)

func pkgCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pkg",
		Short: "Create a reusable pkg to create resources in a declarative manner",
	}

	path := cmd.Flags().String("path", "", "path to manifest file")
	cmd.MarkFlagFilename("path", "yaml", "yml", "json")
	cmd.MarkFlagRequired("path")

	orgID := cmd.Flags().String("org-id", "", "The ID of the organization that owns the bucket")
	cmd.MarkFlagRequired("org-id")

	cmd.RunE = manifestApply(orgID, path)

	return cmd
}

func manifestApply(orgID, path *string) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) (e error) {
		svc, err := newPkgerSVC(flags)
		if err != nil {
			return err
		}

		pkg, err := pkgFromFile(*path)
		if err != nil {
			return err
		}

		printManifestSummary(pkg.Summary())

		ui := &input.UI{
			Writer: os.Stdout,
			Reader: os.Stdin,
		}

		confirm := getInput(ui, "Confirm application of the above resources (y/n)", "n")
		if strings.ToLower(confirm) != "y" {
			fmt.Fprintln(os.Stdout, "aborted application of manifest")
			return nil
		}

		influxOrgID, err := influxdb.IDFromString(*orgID)
		if err != nil {
			return err
		}

		summary, err := svc.Apply(context.Background(), *influxOrgID, pkg)
		if err != nil {
			return err
		}

		w := internal.NewTabWriter(os.Stdout)
		if newBuckets := summary.Buckets; len(newBuckets) > 0 {
			w.WriteHeaders(strings.ToUpper("Buckets"))
			w.WriteHeaders("ID", "Name", "Description", "Retention", "Created At")
			for _, bucket := range newBuckets {
				w.Write(map[string]interface{}{
					"ID":          bucket.ID.String(),
					"Name":        bucket.Name,
					"Description": bucket.Description,
					"Retention":   formatDuration(bucket.RetentionPeriod),
				})
			}
			w.WriteHeaders()
		}
		w.Flush()

		return nil
	}
}

func newPkgerSVC(f Flags) (*pkger.Service, error) {
	bucketSVC, err := newBucketService(f)
	if err != nil {
		return nil, err
	}

	return pkger.NewService(zap.NewNop(), bucketSVC), nil
}

func pkgFromFile(path string) (*pkger.Pkg, error) {
	var enc pkger.Encoding
	switch ext := filepath.Ext(path); ext {
	case ".yaml", ".yml":
		enc = pkger.EncodingYAML
	case ".json":
		enc = pkger.EncodingJSON
	default:
		return nil, errors.New("file provided must be one of yaml/yml/json extension but got: " + ext)
	}

	return pkger.Parse(enc, pkger.FromFile(path))
}

func printManifestSummary(m pkger.Summary) {
	w := internal.NewTabWriter(os.Stdout)
	if buckets := m.Buckets; len(buckets) > 0 {
		w.WriteHeaders(strings.ToUpper("Buckets"))
		w.WriteHeaders("Name", "Retention", "Description")
		for _, bucket := range buckets {
			w.Write(map[string]interface{}{
				"Name":        bucket.Name,
				"Retention":   formatDuration(bucket.RetentionPeriod),
				"Description": bucket.Description,
			})
		}
		w.WriteHeaders()
	}
	w.Flush()
}

func formatDuration(d time.Duration) string {
	if d == 0 {
		return "inf"
	}
	return d.String()
}
