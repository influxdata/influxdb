package upgrade

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/fluxinit"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var v2DumpMetaCommand = &cobra.Command{
	Use:    "v2-dump-meta",
	Short:  "Dump InfluxDB 2.x influxd.bolt",
	Args:   cobra.NoArgs,
	Hidden: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		fluxinit.FluxInit()
		ctx := context.Background()
		svc, err := newInfluxDBv2(ctx, &v2DumpMetaOptions, zap.NewNop())
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

		fmt.Fprintln(os.Stdout, "Databases")
		fmt.Fprintln(os.Stdout, "---------")
		fmt.Fprintf(tw, "%s\t%s\t%s\n", "Name", "Default RP", "Shards")
		for _, row := range svc.meta.Databases() {
			fmt.Fprintf(tw, "%s\t%s\t", row.Name, row.DefaultRetentionPolicy)
			for i, si := range row.ShardInfos() {
				if i > 0 {
					fmt.Fprint(tw, ",")
				}
				fmt.Fprintf(tw, "%d", si.ID)
			}
			fmt.Fprintln(tw)
		}
		_ = tw.Flush()
		fmt.Fprintln(os.Stdout)

		fmt.Fprintln(os.Stdout, "Retention policies")
		fmt.Fprintln(os.Stdout, "---------")
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", "Database", "Name", "Duration", "Shard Group duration")
		for _, db := range svc.meta.Databases() {
			for _, rp := range db.RetentionPolicies {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", db.Name, rp.Name, rp.Duration.String(), rp.ShardGroupDuration.String())
			}
		}
		_ = tw.Flush()
		fmt.Fprintln(os.Stdout)

		fmt.Fprintln(os.Stdout, "Shard groups")
		fmt.Fprintln(os.Stdout, "---------")
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", "Database/RP", "Start Time", "End Time", "Shards")
		for _, db := range svc.meta.Databases() {
			for _, rp := range db.RetentionPolicies {
				for _, sg := range rp.ShardGroups {
					fmt.Fprintf(tw, "%s/%s\t%s\t%s\t", db.Name, rp.Name, sg.StartTime.String(), sg.EndTime.String())
					for i, si := range sg.Shards {
						if i > 0 {
							fmt.Fprint(tw, ",")
						}
						fmt.Fprintf(tw, "%d", si.ID)
					}
					fmt.Fprintln(tw)
				}
			}
		}
		_ = tw.Flush()
		fmt.Fprintln(os.Stdout)

		fmt.Fprintln(os.Stdout, "Mappings")
		fmt.Fprintln(os.Stdout, "---------")
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", "Database", "RP", "Org", "Bucket", "Default")
		mappings, _, err := svc.dbrpSvc.FindMany(ctx, influxdb.DBRPMappingFilter{})
		if err != nil {
			return err
		}
		showBool := func(b bool) string {
			if b {
				return "yes"
			}
			return "no"
		}
		for _, row := range mappings {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", row.Database, row.RetentionPolicy, row.OrganizationID.String(), row.BucketID.String(), showBool(row.Default))
		}
		_ = tw.Flush()
		fmt.Fprintln(os.Stdout)

		showCheck := func(b bool) string {
			if b {
				return "✓"
			}
			return ""
		}

		fmt.Fprintln(os.Stdout, "Users")
		fmt.Fprintln(os.Stdout, "-----")
		fmt.Fprintf(tw, "%s\t%s\n", "Name", "Admin")
		for _, row := range svc.meta.Users() {
			fmt.Fprintf(tw, "%s\t%s\n", row.Name, showCheck(row.Admin))
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
