package exec

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/influxdata/influxdb/cmd/influx_tools/generate"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/profile"
	"github.com/influxdata/influxdb/cmd/influx_tools/server"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/platform/pkg/data/gen"
)

// Command represents the program execution for "store query".
type Command struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	deps   Dependencies
	server server.Interface
	filter SeriesGeneratorFilter

	configPath  string
	printOnly   bool
	noTSI       bool
	concurrency int
	spec        generate.Spec

	profile profile.Config
}

type SeriesGeneratorFilter func(sgi meta.ShardGroupInfo, g SeriesGenerator) SeriesGenerator

type Dependencies struct {
	Server server.Interface

	// SeriesGeneratorFilter wraps g with a SeriesGenerator that
	// returns a subset of keys from g
	SeriesGeneratorFilter SeriesGeneratorFilter
}

// NewCommand returns a new instance of Command.
func NewCommand(deps Dependencies) *Command {
	return &Command{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		server: deps.Server,
		filter: deps.SeriesGeneratorFilter,
	}
}

func (cmd *Command) Run(args []string) (err error) {
	err = cmd.parseFlags(args)
	if err != nil {
		return err
	}

	err = cmd.server.Open(cmd.configPath)
	if err != nil {
		return err
	}

	plan, err := cmd.spec.Plan(cmd.server)
	if err != nil {
		return err
	}

	plan.PrintPlan(cmd.Stdout)

	if cmd.printOnly {
		return nil
	}

	if err = plan.InitFileSystem(cmd.server.MetaClient()); err != nil {
		return err
	}

	return cmd.exec(plan)
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("gen-init", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.BoolVar(&cmd.printOnly, "print", false, "Print data spec only")
	fs.BoolVar(&cmd.noTSI, "no-tsi", false, "Skip building TSI index")
	fs.IntVar(&cmd.concurrency, "c", 1, "Number of shards to generate concurrently")
	fs.StringVar(&cmd.profile.CPU, "cpuprofile", "", "Collect a CPU profile")
	fs.StringVar(&cmd.profile.Memory, "memprofile", "", "Collect a memory profile")
	cmd.spec.AddFlags(fs)

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.spec.Database == "" {
		return errors.New("database is required")
	}

	if cmd.spec.Retention == "" {
		return errors.New("retention policy is required")
	}

	return nil
}

func (cmd *Command) exec(p *generate.Plan) error {
	groups := p.ShardGroups()
	gens := make([]SeriesGenerator, len(groups))
	for i := range gens {
		var (
			name []byte
			keys []string
			tv   []gen.CountableSequence
		)

		name = []byte("m0")
		tv = make([]gen.CountableSequence, len(p.Tags))
		setTagVals(p.Tags, tv)
		keys = make([]string, len(p.Tags))
		setTagKeys("tag", keys)

		sgi := groups[i]
		vg := gen.NewIntegerConstantValuesSequence(p.PointsPerSeriesPerShard, sgi.StartTime, p.ShardDuration/time.Duration(p.PointsPerSeriesPerShard), 1)
		gens[i] = NewSeriesGenerator(name, []byte("v0"), vg, gen.NewTagsValuesSequenceKeysValues(keys, tv))
		if cmd.filter != nil {
			gens[i] = cmd.filter(sgi, gens[i])
		}
	}

	stop := cmd.profile.Start()
	defer stop()

	start := time.Now().UTC()
	defer func() {
		elapsed := time.Since(start)
		fmt.Println()
		fmt.Printf("Total time: %0.1f seconds\n", elapsed.Seconds())
	}()

	g := Generator{Concurrency: cmd.concurrency, BuildTSI: !cmd.noTSI}
	return g.Run(context.Background(), p.Database, p.ShardPath(), p.NodeShardGroups(), gens)
}

func setTagVals(tags []int, tv []gen.CountableSequence) {
	for j := range tags {
		tv[j] = gen.NewCounterByteSequenceCount(tags[j])
	}
}

func setTagKeys(prefix string, keys []string) {
	tw := int(math.Ceil(math.Log10(float64(len(keys)))))
	tf := fmt.Sprintf("%s%%0%dd", prefix, tw)
	for i := range keys {
		keys[i] = fmt.Sprintf(tf, i)
	}
}
