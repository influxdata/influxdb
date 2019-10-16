package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/influxdata/influxdb/kit/errors"

	"github.com/influxdata/influxdb"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"

	"github.com/tcnksm/go-input"

	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/internal/manifest"

	"github.com/spf13/cobra"
)

func manifestCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "manifest",
		Short: "Create resources from declarative config files",
	}

	path := cmd.Flags().String("path", "", "path to manifest file")
	cmd.MarkFlagFilename("path", "yaml", "yml")
	cmd.MarkFlagRequired("path")

	orgID := cmd.Flags().String("org-id", "", "The ID of the organization that owns the bucket")
	cmd.MarkFlagRequired("org-id")

	cmd.RunE = manifestApply(orgID, path)

	return cmd
}

func manifestApply(orgID, path *string) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) (e error) {
		f, err := os.Open(*path)
		if err != nil {
			return err
		}
		defer f.Close()

		fest, err := manifest.ParseYAML(f)
		if err != nil {
			return err
		}

		w := internal.NewTabWriter(os.Stdout)
		if len(fest.Labels()) > 0 {
			w.WriteHeaders(strings.ToUpper("Labels"))
			w.WriteHeaders("Name")
			for _, label := range fest.Labels() {
				w.Write(map[string]interface{}{
					"Name": label.Name,
				})
			}
			w.WriteHeaders()
		}

		if len(fest.Buckets()) > 0 {
			w.WriteHeaders(strings.ToUpper("Buckets"))
			w.WriteHeaders("Name", "Retention")
			for _, bucket := range fest.Buckets() {
				w.Write(map[string]interface{}{
					"Name":      bucket.Name,
					"Retention": bucket.RetentionPeriod,
				})
			}
			w.WriteHeaders()
		}
		w.Flush()

		ui := &input.UI{
			Writer: os.Stdout,
			Reader: os.Stdin,
		}

		confirm := getInput(ui, "Confirm application of the above resources (y/n)", "n")
		if strings.ToLower(confirm) != "y" {
			fmt.Fprintln(os.Stdout, "aborted application of manifest")
			return nil
		}

		organizationID, err := platform.IDFromString(*orgID)
		if err != nil {
			return err
		}

		bucketSVC, err := newBucketService(flags)
		if err != nil {
			return err
		}

		labelSVC, err := newLabelService(flags)
		if err != nil {
			return err
		}

		svc := &manifestSVC{
			orgID:     *organizationID,
			bucketSVC: bucketSVC,
			labelSVC:  labelSVC,
		}

		var newBuckets []influxdb.Bucket
		defer func() {
			if e != nil {
				svc.deleteBuckets(newBuckets)
			}
		}()

		newBuckets, err = svc.applyBuckets(fest.Buckets())
		if err != nil {
			if deleteErr := svc.deleteBuckets(newBuckets); deleteErr != nil {
				return errors.Wrap(deleteErr, err.Error())
			}
			return err
		}

		var newLabels []influxdb.Label
		defer func() {
			if e != nil {
				svc.deleteLabels(newLabels)
			}
		}()

		newLabels, err = svc.applyLabels(fest.Labels())
		if err != nil {
			return err
		}

		var newLabelMappings []influxdb.LabelMapping
		defer func() {
			if e != nil {
				svc.deleteLabelMappings(newLabelMappings)
			}
		}()

		newLabelMappings, err = svc.applyLabelMappings(fest.Buckets())
		if err != nil {
			return err
		}

		if len(newLabels) > 0 {
			w.WriteHeaders(strings.ToUpper("Labels"))
			w.WriteHeaders("ID", "Name")
			for _, label := range newLabels {
				w.Write(map[string]interface{}{
					"ID":   label.ID.String(),
					"Name": label.Name,
				})
			}
			w.WriteHeaders()
		}

		if len(newBuckets) > 0 {
			w.WriteHeaders(strings.ToUpper("Buckets"))
			w.WriteHeaders("ID", "Name", "Description", "Retention", "Created At")
			for _, bucket := range newBuckets {
				w.Write(map[string]interface{}{
					"ID":          bucket.ID.String(),
					"Name":        bucket.Name,
					"Description": bucket.Description,
					"Retention":   bucket.RetentionPeriod,
					"Created At":  bucket.CreatedAt.Format(time.RFC3339),
				})
			}
			w.WriteHeaders()
		}

		if len(newLabelMappings) > 0 {
			w.WriteHeaders(strings.ToUpper("Label Mappings"))
			w.WriteHeaders("Resource Type", "Label ID", "Resource ID")
			for _, mapping := range newLabelMappings {
				w.Write(map[string]interface{}{
					"Resource Type": mapping.ResourceType,
					"Label ID":      mapping.LabelID.String(),
					"Resource ID":   mapping.ResourceID.String(),
				})
			}
			w.WriteHeaders()
		}
		w.Flush()

		return nil
	}
}

type manifestSVC struct {
	bucketSVC influxdb.BucketService
	labelSVC  influxdb.LabelService
	orgID     platform.ID
}

func (m *manifestSVC) applyBuckets(buckets []*manifest.Bucket) ([]influxdb.Bucket, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	influxBuckets := make([]influxdb.Bucket, 0, len(buckets))
	for i, b := range buckets {
		influxBucket := influxdb.Bucket{
			OrgID:           m.orgID,
			Description:     b.Description,
			Name:            b.Name,
			RetentionPeriod: b.RetentionPeriod,
		}
		err := m.bucketSVC.CreateBucket(ctx, &influxBucket)
		if err != nil {
			return influxBuckets, err
		}
		buckets[i].ID = influxBucket.ID
		influxBuckets = append(influxBuckets, influxBucket)
	}

	return influxBuckets, nil
}

func (m *manifestSVC) applyLabels(labels []*manifest.Label) ([]influxdb.Label, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	influxLabels := make([]influxdb.Label, 0, len(labels))
	for i, l := range labels {
		influxLabel := influxdb.Label{
			OrgID:      m.orgID,
			Name:       l.Name,
			Properties: nil, // what are properties?
		}
		err := m.labelSVC.CreateLabel(ctx, &influxLabel)
		if err != nil {
			return influxLabels, err
		}
		labels[i].ID = influxLabel.ID
		influxLabels = append(influxLabels, influxLabel)
	}

	return influxLabels, nil
}

func (m *manifestSVC) applyLabelMappings(buckets []*manifest.Bucket) ([]influxdb.LabelMapping, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var mappings []influxdb.LabelMapping
	for _, b := range buckets {
		for _, l := range b.Labels {
			mapping := &influxdb.LabelMapping{
				LabelID:      l.ID,
				ResourceID:   b.ID,
				ResourceType: influxdb.BucketsResourceType,
			}
			err := m.labelSVC.CreateLabelMapping(ctx, mapping)
			if err != nil {
				return mappings, err
			}
			mappings = append(mappings, *mapping)
		}
	}

	return mappings, nil
}

func (m *manifestSVC) deleteBuckets(buckets []influxdb.Bucket) error {
	var errs []string
	for _, b := range buckets {
		err := m.bucketSVC.DeleteBucket(context.Background(), b.ID)
		if err != nil {
			errs = append(errs, b.ID.String())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(`bucket_ids=[%s] err="unable to delete bucket"`, strings.Join(errs, ", "))
	}

	return nil
}

func (m *manifestSVC) deleteLabels(labels []influxdb.Label) error {
	var errs []string
	for _, l := range labels {
		err := m.labelSVC.DeleteLabel(context.Background(), l.ID)
		if err != nil {
			errs = append(errs, l.ID.String())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(`label_ids=[%s] err="unable to delete label"`, strings.Join(errs, ", "))
	}

	return nil
}

func (m *manifestSVC) deleteLabelMappings(mappings []influxdb.LabelMapping) error {
	var errs []string
	for i := range mappings {
		l := mappings[i]
		err := m.labelSVC.DeleteLabelMapping(context.Background(), &l)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s:%s", l.LabelID.String(), l.ResourceID.String()))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(`label_resource_id_pairs=[%s] err="unable to delete label"`, strings.Join(errs, ", "))
	}

	return nil
}

func newLabelService(f Flags) (platform.LabelService, error) {
	if flags.local {
		return newLocalKVService()
	}
	return &http.LabelService{
		Addr:  flags.host,
		Token: flags.token,
	}, nil
}
