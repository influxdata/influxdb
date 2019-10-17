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
		svc, err := newManifestSVC(*orgID, flags)
		if err != nil {
			return err
		}

		f, err := os.Open(*path)
		if err != nil {
			return err
		}
		defer f.Close()

		fest, err := manifest.ParseYAML(f)
		if err != nil {
			return err
		}

		printManifestSummary(fest)

		ui := &input.UI{
			Writer: os.Stdout,
			Reader: os.Stdin,
		}

		confirm := getInput(ui, "Confirm application of the above resources (y/n)", "n")
		if strings.ToLower(confirm) != "y" {
			fmt.Fprintln(os.Stdout, "aborted application of manifest")
			return nil
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

		var newDashboards []influxdb.Dashboard
		defer func() {
			if e != nil {
				svc.deleteDashboards(newDashboards)
			}
		}()
		newDashboards, err = svc.applyDashboards(fest.Dashboards())
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

		dashMappings, err := svc.applyDashboardLabelMappings(fest.Dashboards())
		// weird append first, is for cleaning up the badies in case of error
		newLabelMappings = append(newLabelMappings, dashMappings...)
		if err != nil {
			return err
		}

		w := internal.NewTabWriter(os.Stdout)
		if len(newLabels) > 0 {
			w.WriteHeaders(strings.ToUpper("Labels"))
			w.WriteHeaders("ID", "Name", "Properties")
			for _, label := range newLabels {
				w.Write(map[string]interface{}{
					"ID":         label.ID.String(),
					"Name":       label.Name,
					"Properties": label.Properties,
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

		if len(newDashboards) > 0 {
			w.WriteHeaders(strings.ToUpper("Dashboards"))
			w.WriteHeaders("ID", "Name", "Description", "Cells")

			for _, d := range newDashboards {
				w.Write(map[string]interface{}{
					"ID":          d.ID.String(),
					"Name":        d.Name,
					"Description": d.Description,
					"Cells":       len(d.Cells),
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
		}
		w.Flush()

		return nil
	}
}

type manifestSVC struct {
	orgID platform.ID

	bucketSVC    influxdb.BucketService
	dashboardSVC influxdb.DashboardService
	labelSVC     influxdb.LabelService
}

func newManifestSVC(orgID string, f Flags) (*manifestSVC, error) {
	organizationID, err := platform.IDFromString(orgID)
	if err != nil {
		return nil, err
	}

	bucketSVC, err := newBucketService(f)
	if err != nil {
		return nil, err
	}

	labelSVC, err := newLabelService(f)
	if err != nil {
		return nil, err
	}

	dashSVC, err := newDashboardService(f)
	if err != nil {
		return nil, err
	}

	return &manifestSVC{
		orgID:        *organizationID,
		bucketSVC:    bucketSVC,
		labelSVC:     labelSVC,
		dashboardSVC: dashSVC,
	}, nil
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
			Properties: l.Properties,
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

func (m *manifestSVC) applyDashboards(dashboards []*manifest.Dashboard) ([]influxdb.Dashboard, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	cellMap := make(map[*influxdb.Cell]*manifest.Cell)

	getInfluxCells := func(cells []*manifest.Cell) []*influxdb.Cell {
		icells := make([]*influxdb.Cell, 0, len(cells))
		for _, cell := range cells {
			icell := &influxdb.Cell{
				CellProperty: influxdb.CellProperty{
					X: int32(cell.X),
					Y: int32(cell.Y),
					H: int32(cell.Height),
					W: int32(cell.Width),
				},
			}
			cellMap[icell] = cell
			icells = append(icells, icell)
		}
		return icells
	}

	influxDashs := make([]influxdb.Dashboard, 0, len(dashboards))
	for i, l := range dashboards {
		influxDash := influxdb.Dashboard{
			OrganizationID: m.orgID,
			Name:           l.Name,
			Description:    l.Description,
			Cells:          getInfluxCells(l.Cells),
		}
		err := m.dashboardSVC.CreateDashboard(ctx, &influxDash)
		if err != nil {
			return influxDashs, err
		}
		dashboards[i].ID = influxDash.ID

		for i := range influxDash.Cells {
			cell := influxDash.Cells[i]
			manCell := cellMap[cell]
			manCell.ID = cell.ID

			upd := influxdb.ViewUpdate{
				ViewContentsUpdate: influxdb.ViewContentsUpdate{
					Name: &manCell.View.Name,
				},
				Properties: manCell.View.Properties(),
			}
			view, err := m.dashboardSVC.UpdateDashboardCellView(ctx, influxDash.ID, cell.ID, upd)
			if err != nil {
				return influxDashs, err
			}
			manCell.View.ID = view.ID
		}

		influxDashs = append(influxDashs, influxDash)
	}

	return influxDashs, nil
}

func (m *manifestSVC) applyDashboardLabelMappings(dashboards []*manifest.Dashboard) ([]influxdb.LabelMapping, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var mappings []influxdb.LabelMapping
	for _, d := range dashboards {
		for _, l := range d.Labels {
			mapping := &influxdb.LabelMapping{
				LabelID:      l.ID,
				ResourceID:   d.ID,
				ResourceType: influxdb.DashboardsResourceType,
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

func (m *manifestSVC) deleteDashboards(dashs []influxdb.Dashboard) error {
	var errs []string
	for _, d := range dashs {
		err := m.dashboardSVC.DeleteDashboard(context.Background(), d.ID)
		if err != nil {
			errs = append(errs, d.ID.String())
		}

		for _, cell := range d.Cells {
			err := m.dashboardSVC.RemoveDashboardCell(context.Background(), d.ID, cell.ID)
			if err != nil {
				//TODO: handle failed delete gracefully
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(`dashboard_ids=[%s] err="unable to delete dashboard"`, strings.Join(errs, ", "))
	}

	return nil
}

func newLabelService(f Flags) (platform.LabelService, error) {
	if f.local {
		return newLocalKVService()
	}
	return &http.LabelService{
		Addr:  f.host,
		Token: f.token,
	}, nil
}

func newDashboardService(f Flags) (platform.DashboardService, error) {
	if f.local {
		return newLocalKVService()
	}
	return &http.DashboardService{
		Addr:  f.host,
		Token: f.token,
	}, nil
}

func printManifestSummary(m *manifest.Manifest) {
	w := internal.NewTabWriter(os.Stdout)
	if labels := m.Labels(); len(labels) > 0 {
		w.WriteHeaders(strings.ToUpper("Labels"))
		w.WriteHeaders("Name", "Properties")
		for _, label := range labels {
			w.Write(map[string]interface{}{
				"Name":       label.Name,
				"Properties": label.Properties,
			})
		}
		w.WriteHeaders()
	}

	if buckets := m.Buckets(); len(buckets) > 0 {
		w.WriteHeaders(strings.ToUpper("Buckets"))
		w.WriteHeaders("Name", "Retention", "Description")
		for _, bucket := range buckets {
			w.Write(map[string]interface{}{
				"Name":        bucket.Name,
				"Retention":   bucket.RetentionPeriod,
				"Description": bucket.Description,
			})
		}
		w.WriteHeaders()
	}

	printManCells := func(cc []*manifest.Cell) string {
		var sb strings.Builder
		for _, c := range cc {
			const cellFmt = "{X: %d, Y: %d, Height: %d, Width: %d, ViewName: %s, ViewType: %s} "
			sb.WriteString(fmt.Sprintf(cellFmt, c.X, c.Y, c.Height, c.Width, c.View.Name, c.View.Properties().GetType()))
		}
		return sb.String()
	}

	if dashboards := m.Dashboards(); len(dashboards) > 0 {
		w.WriteHeaders(strings.ToUpper("Dashboards"))
		w.WriteHeaders("Name", "Description", "Cells")
		for _, d := range dashboards {
			w.Write(map[string]interface{}{
				"Name":        d.Name,
				"Description": d.Description,
				"Cells":       printManCells(d.Cells),
			})
		}
		w.WriteHeaders()
	}

	w.Flush()
}
