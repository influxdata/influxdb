package upgrade

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/spf13/cobra"
)

var v2DumpMetaCommand = &cobra.Command{
	Use:   "v2-dump-meta",
	Short: "Dump InfluxDB 2.x influxd.bolt",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		svc, err := newInfluxDBv2(ctx, &v2DumpMetaOptions)
		if err != nil {
			return fmt.Errorf("error opening InfluxDB 2.0: %w", err)
		}

		tw := tabwriter.NewWriter(os.Stdout, 15, 4, 1, ' ', 0)

		fmt.Fprintln(os.Stdout, "Orgs")
		fmt.Fprintln(os.Stdout, "----")
		fmt.Fprintf(tw, "%s\t%s\n", "ID", "Name")
		orgs, _, err := svc.ts.FindOrganizations(ctx, influxdb.OrganizationFilter{})
		if err != nil {
			return err
		}
		for _, row := range orgs {
			fmt.Fprintf(tw, "%s\t%s\n", row.ID.String(), row.Name)
		}
		_ = tw.Flush()
		fmt.Fprintln(os.Stdout)

		fmt.Fprintln(os.Stdout, "Users")
		fmt.Fprintln(os.Stdout, "-----")
		fmt.Fprintf(tw, "%s\t%s\n", "ID", "Name")
		users, _, err := svc.ts.FindUsers(ctx, influxdb.UserFilter{})
		if err != nil {
			return err
		}
		for _, row := range users {
			fmt.Fprintf(tw, "%s\t%s\n", row.ID.String(), row.Name)
		}
		_ = tw.Flush()
		fmt.Fprintln(os.Stdout)

		fmt.Fprintln(os.Stdout, "Buckets")
		fmt.Fprintln(os.Stdout, "-------")
		fmt.Fprintf(tw, "%s\t%s\n", "ID", "Name")
		buckets, _, err := svc.ts.FindBuckets(ctx, influxdb.BucketFilter{})
		if err != nil {
			return err
		}
		for _, row := range buckets {
			fmt.Fprintf(tw, "%s\t%s\n", row.ID.String(), row.Name)
		}
		_ = tw.Flush()
		fmt.Fprintln(os.Stdout)

		return nil
	},
}

var v2DumpMetaOptions = optionsV2{}

func init() {
	flags := v2DumpMetaCommand.Flags()

	v2dir, err := fs.InfluxDir()
	if err != nil {
		panic("error fetching default InfluxDB 2.0 dir: " + err.Error())
	}

	flags.StringVar(&v2DumpMetaOptions.boltPath, "v2-bolt-path", filepath.Join(v2dir, "influxd.bolt"), "Path to 2.0 metadata")
}
