package store

import (
	"context"
	"fmt"
	"math"

	"github.com/influxdata/influxdb/models"
	"github.com/spf13/cobra"
)

var tagKeysCommand = &cobra.Command{
	Use:  "tag-keys",
	RunE: tagKeysFE,
}

var tagKeysFlags struct {
	orgBucket
	min, max int64
	expr     exprValue
	count    int
	noPrint  bool
	warm     bool
}

func init() {
	tagKeysFlags.orgBucket.AddFlags(tagKeysCommand)
	flagSet := tagKeysCommand.Flags()
	flagSet.Int64Var(&tagKeysFlags.min, "min", math.MinInt64, "minimum timestamp")
	flagSet.Int64Var(&tagKeysFlags.max, "max", math.MaxInt64, "maximum timestamp")
	flagSet.Var(&tagKeysFlags.expr, "expr", "optional InfluxQL predicate expression")
	flagSet.IntVar(&tagKeysFlags.count, "count", 1, "Number of times to run benchmark")
	flagSet.BoolVar(&tagKeysFlags.noPrint, "no-print", false, "Suppress output")
	flagSet.BoolVar(&tagKeysFlags.warm, "warm", false, "Warm the series file before benchmarking")
	RootCommand.AddCommand(tagKeysCommand)
}

func tagKeysFE(_ *cobra.Command, _ []string) error {
	ctx := context.Background()

	engine, err := newEngine(ctx)
	if err != nil {
		return err
	}
	defer engine.Close()

	orgID, bucketID, err := tagKeysFlags.OrgBucketID()
	if err != nil {
		return err
	}

	benchFn := func() {
		itr, err := engine.TagKeys(ctx, orgID, bucketID, tagKeysFlags.min, tagKeysFlags.max, tagKeysFlags.expr.e)
		if err != nil {
			panic(err)
		}
		for itr.Next() {
			key := itr.Value()
			if !tagKeysFlags.noPrint {
				switch key {
				case models.MeasurementTagKey:
					key = "_measurement"
				case models.FieldKeyTagKey:
					key = "_field"
				}
				fmt.Println(key)
			}
		}
	}

	if tagKeysFlags.warm {
		benchFn()
	}

	stop := storeFlags.profile.Start()
	defer stop()

	for i := tagKeysFlags.count; i > 0; i-- {
		benchFn()
	}

	return nil
}
