package store

import (
	"context"
	"fmt"
	"math"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
	"github.com/spf13/cobra"
)

var tagValuesCommand = &cobra.Command{
	Use:  "tag-values",
	RunE: tagValuesFE,
}

var tagValuesFlags struct {
	orgBucket
	key      string
	min, max int64
	expr     exprValue
	count    int
	noPrint  bool
	warm     bool
}

func init() {
	tagValuesFlags.orgBucket.AddFlags(tagValuesCommand)
	flagSet := tagValuesCommand.Flags()
	flagSet.StringVar(&tagValuesFlags.key, "key", "_measurement", "Which tag key to list values")
	flagSet.Int64Var(&tagValuesFlags.min, "min", math.MinInt64, "minimum timestamp")
	flagSet.Int64Var(&tagValuesFlags.max, "max", math.MaxInt64, "maximum timestamp")
	flagSet.Var(&tagValuesFlags.expr, "expr", "optional InfluxQL predicate expression")
	flagSet.IntVar(&tagValuesFlags.count, "count", 1, "Number of times to run benchmark")
	flagSet.BoolVar(&tagValuesFlags.noPrint, "no-print", false, "Suppress output")
	flagSet.BoolVar(&tagValuesFlags.warm, "warm", false, "Warm the series file before benchmarking")
	RootCommand.AddCommand(tagValuesCommand)
}

func tagValuesFE(_ *cobra.Command, _ []string) error {
	ctx := context.Background()

	engine, err := newEngine(ctx)
	if err != nil {
		return err
	}
	defer engine.Close()

	orgID, bucketID, err := tagValuesFlags.OrgBucketID()
	if err != nil {
		return err
	}

	var key string
	switch tagValuesFlags.key {
	case "_measurement":
		key = models.MeasurementTagKey
	case "_field":
		key = models.FieldKeyTagKey
	default:
		key = tagValuesFlags.key
	}

	benchFn := func() {
		itr, err := engine.TagValues(ctx, orgID, bucketID, key, tagValuesFlags.min, tagValuesFlags.max, tagValuesFlags.expr.e)
		if err != nil {
			panic(err)
		}
		for itr.Next() {
			key := itr.Value()
			if !tagValuesFlags.noPrint {
				fmt.Println(key)
			}
		}
	}

	if tagValuesFlags.warm {
		benchFn()
	}

	stop := storeFlags.profile.Start()
	defer stop()

	for i := tagValuesFlags.count; i > 0; i-- {
		benchFn()
	}

	return nil
}

type exprValue struct {
	s string
	e influxql.Expr
}

func (e *exprValue) String() string {
	if e.e == nil {
		return "<none>"
	}
	return e.s
}

func (e *exprValue) Set(v string) (err error) {
	e.s = v
	e.e, err = influxql.ParseExpr(e.s)
	if err != nil {
		return err
	}

	e.e = influxql.RewriteExpr(e.e, func(expr influxql.Expr) influxql.Expr {
		switch n := expr.(type) {
		case *influxql.BinaryExpr:
			if r, ok := n.LHS.(*influxql.VarRef); ok {
				switch r.Val {
				case "_measurement":
					r.Val = models.MeasurementTagKey
				case "_field":
					r.Val = models.FieldKeyTagKey
				}
			}
		}
		return expr
	})
	return err
}

func (e *exprValue) Type() string {
	return "expression"
}
