package generate

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/cmd/influxd/internal/profile"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/pkg/data/gen"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var Command = &cobra.Command{
	Use:   "generate <schema.toml>",
	Short: "Generate time series data sets using TOML schema",
	Long: `
This command will generate time series data direct to disk using schema 
defined in a TOML file. Use the help-schema subcommand to produce a TOML 
file to STDOUT, which includes documentation describing the available options.



NOTES:

* The influxd server should not be running when using the generate tool
  as it modifies the index and TSM data.
* This tool is intended for development and testing purposes only and 
  SHOULD NOT be run on a production server.
`,
	Args: cobra.ExactArgs(1),
	RunE: generateFE,
}

var flags struct {
	printOnly   bool
	storageSpec StorageSpec
	profile     profile.Config
}

func init() {
	Command.Flags().SortFlags = false

	pfs := Command.PersistentFlags()
	pfs.SortFlags = false
	pfs.BoolVar(&flags.printOnly, "print", false, "Print data spec and exit")

	flags.storageSpec.AddFlags(Command, pfs)

	pfs.StringVar(&flags.profile.CPU, "cpuprofile", "", "Collect a CPU profile")
	pfs.StringVar(&flags.profile.Memory, "memprofile", "", "Collect a memory profile")
}

func generateFE(_ *cobra.Command, args []string) error {
	storagePlan, err := flags.storageSpec.Plan()
	if err != nil {
		return err
	}

	storagePlan.PrintPlan(os.Stdout)

	spec, err := gen.NewSpecFromPath(args[0])
	if err != nil {
		return err
	}

	if err = assignOrgBucket(spec); err != nil {
		return err
	}

	if flags.printOnly {
		return nil
	}

	return exec(storagePlan, spec)
}

func assignOrgBucket(spec *gen.Spec) error {
	boltFile, err := fs.BoltFile()
	if err != nil {
		return err
	}

	store := bolt.NewKVStore(zap.NewNop(), boltFile)
	if err = store.Open(context.Background()); err != nil {
		return err
	}

	s := kv.NewService(zap.NewNop(), store)
	if err = s.Initialize(context.Background()); err != nil {
		return err
	}

	org, err := s.FindOrganizationByName(context.Background(), flags.storageSpec.Organization)
	if err != nil {
		return err
	}

	bucket, err := s.FindBucketByName(context.Background(), org.ID, flags.storageSpec.Bucket)
	if err != nil {
		return err
	}

	store.Close()

	spec.OrgID = org.ID
	spec.BucketID = bucket.ID

	return nil
}

func exec(storagePlan *StoragePlan, spec *gen.Spec) error {
	tr := gen.TimeRange{
		Start: storagePlan.StartTime,
		End:   storagePlan.EndTime,
	}
	sg := gen.NewSeriesGeneratorFromSpec(spec, tr)

	stop := flags.profile.Start()
	defer stop()

	var files []string
	start := time.Now().UTC()
	defer func() {
		elapsed := time.Since(start)
		fmt.Println()
		fmt.Println("Generated:")
		for _, f := range files {
			fmt.Println(f)
		}
		fmt.Println()
		fmt.Printf("Total time: %0.1f seconds\n", elapsed.Seconds())
	}()

	path, err := fs.InfluxDir()
	if err != nil {
		return err
	}
	g := &Generator{Clean: storagePlan.Clean}
	files, err = g.Run(context.Background(), path, sg)
	return err
}
