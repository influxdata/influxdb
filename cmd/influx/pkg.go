package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/pkger"
	"github.com/olekukonko/tablewriter"
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

	cmd.RunE = pkgApply(orgID, path)

	return cmd
}

func pkgApply(orgID, path *string) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) (e error) {
		influxOrgID, err := influxdb.IDFromString(*orgID)
		if err != nil {
			return err
		}

		svc, err := newPkgerSVC(flags)
		if err != nil {
			return err
		}

		pkg, err := pkgFromFile(*path)
		if err != nil {
			return err
		}

		_, diff, err := svc.DryRun(context.Background(), *influxOrgID, pkg)
		if err != nil {
			return err
		}

		printPkgDiff(diff)

		ui := &input.UI{
			Writer: os.Stdout,
			Reader: os.Stdin,
		}

		confirm := getInput(ui, "Confirm application of the above resources (y/n)", "n")
		if strings.ToLower(confirm) != "y" {
			fmt.Fprintln(os.Stdout, "aborted application of package")
			return nil
		}

		summary, err := svc.Apply(context.Background(), *influxOrgID, pkg)
		if err != nil {
			return err
		}

		printPkgSummary(summary)

		return nil
	}
}

func newPkgerSVC(f Flags) (*pkger.Service, error) {
	bucketSVC, err := newBucketService(f)
	if err != nil {
		return nil, err
	}

	labelSVC, err := newLabelService(f)
	if err != nil {
		return nil, err
	}

	return pkger.NewService(zap.NewNop(), bucketSVC, labelSVC), nil
}

func newLabelService(f Flags) (influxdb.LabelService, error) {
	if f.local {
		return newLocalKVService()
	}
	return &http.LabelService{
		Addr:  f.host,
		Token: f.token,
	}, nil
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

func printPkgDiff(diff pkger.Diff) {
	red := color.New(color.FgRed).SprintfFunc()
	green := color.New(color.FgHiGreen, color.Bold).SprintfFunc()

	strDiff := func(isNew bool, old, new string) string {
		if isNew {
			return green(new)
		}
		if old == new {
			return new
		}
		return fmt.Sprintf("%s\n%s", red("%q", old), green("%q", new))
	}

	boolDiff := func(b bool) string {
		bb := strconv.FormatBool(b)
		if b {
			return green(bb)
		}
		return bb
	}

	durDiff := func(isNew bool, oldDur, newDur time.Duration) string {
		o := oldDur.String()
		if oldDur == 0 {
			o = "inf"
		}
		n := newDur.String()
		if newDur == 0 {
			n = "inf"
		}
		if isNew {
			return green(n)
		}
		if oldDur == newDur {
			return n
		}
		return fmt.Sprintf("%s\n%s", red(o), green(n))
	}

	if len(diff.Labels) > 0 {
		headers := []string{"New", "ID", "Name", "Color", "Description"}
		tablePrinter("LABELS", headers, len(diff.Labels), func(w *tablewriter.Table) {
			for _, l := range diff.Labels {
				w.Append([]string{
					boolDiff(l.IsNew()),
					l.ID.String(),
					l.Name,
					strDiff(l.IsNew(), l.OldColor, l.NewColor),
					strDiff(l.IsNew(), l.OldDesc, l.NewDesc),
				})
			}
		})
	}

	if len(diff.Buckets) > 0 {
		headers := []string{"New", "ID", "Name", "Retention Period", "Description"}
		tablePrinter("BUCKETS", headers, len(diff.Buckets), func(w *tablewriter.Table) {
			for _, b := range diff.Buckets {
				w.Append([]string{
					boolDiff(b.IsNew()),
					b.ID.String(),
					b.Name,
					durDiff(b.IsNew(), b.OldRetention, b.NewRetention),
					strDiff(b.IsNew(), b.OldDesc, b.NewDesc),
				})
			}
		})
	}

	if len(diff.LabelMappings) > 0 {
		headers := []string{"New", "Resource Type", "Resource Name", "Resource ID", "Label Name", "Label ID"}
		tablePrinter("LABEL MAPPINGS", headers, len(diff.LabelMappings), func(w *tablewriter.Table) {
			for _, m := range diff.LabelMappings {
				w.Append([]string{
					boolDiff(m.IsNew),
					string(m.ResType),
					m.ResName,
					m.ResID.String(),
					m.LabelName,
					m.LabelID.String(),
				})
			}
		})
	}
}

func printPkgSummary(sum pkger.Summary) {
	if labels := sum.Labels; len(labels) > 0 {
		headers := []string{"ID", "Name", "Description", "Color"}
		tablePrinter("LABELS", headers, len(labels), func(w *tablewriter.Table) {
			for _, l := range labels {
				w.Append([]string{
					l.ID.String(),
					l.Name,
					l.Properties["description"],
					l.Properties["color"],
				})
			}
		})
	}

	if buckets := sum.Buckets; len(buckets) > 0 {
		headers := []string{"ID", "Name", "Retention", "Description"}
		tablePrinter("BUCKETS", headers, len(buckets), func(w *tablewriter.Table) {
			for _, bucket := range buckets {
				w.Append([]string{
					bucket.ID.String(),
					bucket.Name,
					formatDuration(bucket.RetentionPeriod),
					bucket.Description,
				})
			}
		})
	}

	if mappings := sum.LabelMappings; len(mappings) > 0 {
		headers := []string{"Resource Type", "Resource Name", "Resource ID", "Label Name", "Label ID"}
		tablePrinter("LABEL MAPPINGS", headers, len(mappings), func(w *tablewriter.Table) {
			for _, m := range mappings {
				w.Append([]string{
					string(m.ResourceType),
					m.ResourceName,
					m.ResourceID.String(),
					m.LabelName,
					m.LabelID.String(),
				})
			}
		})
	}
}

func tablePrinter(table string, headers []string, count int, appendFn func(w *tablewriter.Table)) {
	descrCol := -1
	for i, h := range headers {
		if strings.ToLower(h) == "description" {
			descrCol = i
			break
		}
	}

	w := tablewriter.NewWriter(os.Stdout)
	w.SetBorder(false)
	if descrCol != -1 {
		w.SetAutoWrapText(false)
		w.SetColMinWidth(descrCol, 30)
	}

	color.New(color.FgYellow, color.Bold).Fprintln(os.Stdout, strings.ToUpper(table))
	w.SetHeader(headers)
	var colors []tablewriter.Colors
	for i := 0; i < len(headers); i++ {
		colors = append(colors, tablewriter.Color(tablewriter.FgHiCyanColor))
	}
	w.SetHeaderColor(colors...)

	appendFn(w)

	footers := make([]string, len(headers))
	footers[len(footers)-2] = "TOTAL"
	footers[len(footers)-1] = strconv.Itoa(count)
	w.SetFooter(footers)
	w.SetFooterColor(colors...)

	w.Render()
	fmt.Fprintln(os.Stdout)
}

func formatDuration(d time.Duration) string {
	if d == 0 {
		return "inf"
	}
	return d.String()
}
