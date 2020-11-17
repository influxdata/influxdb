package upgrade

import (
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/influxdata/influxdb/v2/fluxinit"
	"github.com/spf13/cobra"
)

var v1DumpMetaCommand = &cobra.Command{
	Use:    "v1-dump-meta",
	Short:  "Dump InfluxDB 1.x meta.db",
	Args:   cobra.NoArgs,
	Hidden: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		fluxinit.FluxInit()
		svc, err := newInfluxDBv1(&v1DumpMetaOptions)
		if err != nil {
			return fmt.Errorf("error opening 1.x meta.db: %w", err)
		}
		meta := svc.meta

		tw := tabwriter.NewWriter(os.Stdout, 15, 4, 1, ' ', 0)

		showBool := func(b bool) string {
			if b {
				return "✓"
			}
			return ""
		}

		fmt.Fprintln(os.Stdout, "Databases")
		fmt.Fprintln(os.Stdout, "---------")
		fmt.Fprintf(tw, "%s\t%s\t%s\n", "Name", "Default RP", "Shards")
		for _, row := range meta.Databases() {
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
		for _, db := range meta.Databases() {
			for _, rp := range db.RetentionPolicies {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", db.Name, rp.Name, rp.Duration.String(), rp.ShardGroupDuration.String())
			}
		}
		_ = tw.Flush()
		fmt.Fprintln(os.Stdout)

		fmt.Fprintln(os.Stdout, "Shard groups")
		fmt.Fprintln(os.Stdout, "---------")
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", "Database/RP", "Start Time", "End Time", "Shards")
		for _, db := range meta.Databases() {
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

		fmt.Fprintln(os.Stdout, "Users")
		fmt.Fprintln(os.Stdout, "-----")
		fmt.Fprintf(tw, "%s\t%s\n", "Name", "Admin")
		for _, row := range meta.Users() {
			fmt.Fprintf(tw, "%s\t%s\n", row.Name, showBool(row.Admin))
		}
		_ = tw.Flush()
		fmt.Fprintln(os.Stdout)
		return nil
	},
}

var v1DumpMetaOptions = optionsV1{}

func init() {
	flags := v1DumpMetaCommand.Flags()

	v1dir, err := influxDirV1()
	if err != nil {
		panic("error fetching default InfluxDB 1.x dir: " + err.Error())
	}

	flags.StringVar(&v1DumpMetaOptions.metaDir, "v1-meta-dir", filepath.Join(v1dir, "meta"), "Path to meta.db directory")
}
