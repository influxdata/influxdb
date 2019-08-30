package store

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/storage/readservice"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxql"
	"github.com/spf13/cobra"
)

var readCommand = &cobra.Command{
	Use:  "read",
	RunE: readFE,
}

var readFlags struct {
	orgBucket
	start string
	end   string
	expr  string
}

func init() {
	readFlags.orgBucket.AddFlags(readCommand)
	readCommand.Flags().StringVar(&readFlags.expr, "expr", "", "InfluxQL conditional expression such as \"_m='foo' and tag='value'\"")
	readCommand.Flags().StringVar(&readFlags.start, "start", "", "Optional: the start time to query (RFC3339 format)")
	readCommand.Flags().StringVar(&readFlags.end, "end", "", "Optional: the end time to query (RFC3339 format)")

	RootCommand.AddCommand(readCommand)
}

func readFE(_ *cobra.Command, _ []string) error {
	ctx := context.Background()

	engine, err := newEngine(ctx)
	if err != nil {
		return err
	}
	defer engine.Close()

	store := readservice.NewStore(engine)
	var req datatypes.ReadFilterRequest

	if readFlags.start != "" {
		t, err := parseTime(readFlags.start)
		if err != nil {
			return err
		}
		req.Range.Start = t

	} else {
		req.Range.Start = models.MinNanoTime
	}
	if readFlags.end != "" {
		t, err := parseTime(readFlags.end)
		if err != nil {
			return err
		}
		req.Range.End = t

	} else {
		// set end time to max if it is not set.
		req.Range.End = models.MaxNanoTime
	}

	orgID, bucketID, err := readFlags.OrgBucketID()
	if err != nil {
		return err
	}

	source := store.GetSource(uint64(orgID), uint64(bucketID))
	if any, err := types.MarshalAny(source); err != nil {
		return err
	} else {
		req.ReadSource = any
	}

	if readFlags.expr != "" {
		expr, err := influxql.ParseExpr(readFlags.expr)
		if err != nil {
			return nil
		}
		var v exprToNodeVisitor
		influxql.Walk(&v, expr)
		if v.Err() != nil {
			return v.Err()
		}

		req.Predicate = &datatypes.Predicate{Root: v.nodes[0]}
	}

	stop := storeFlags.profile.Start()
	defer stop()

	rs, err := store.ReadFilter(ctx, &req)
	if err != nil {
		return err
	}
	defer rs.Close()

	points := 0
	series := 0

	fmt.Println("Consuming data...")

	start := time.Now()
	defer func() {
		dur := time.Since(start)
		tw := tabwriter.NewWriter(os.Stdout, 10, 4, 0, ' ', 0)
		fmt.Fprintf(tw, "Series:\t%d\n", series)
		fmt.Fprintf(tw, "Points:\t%d\n", points)
		fmt.Fprintf(tw, "Time:\t%0.0fms\n", dur.Seconds()*1000)
		fmt.Fprintf(tw, "Series/s:\t%0.3f\n", float64(series)/dur.Seconds())
		fmt.Fprintf(tw, "Points/s:\t%0.3f\n", float64(points)/dur.Seconds())
		tw.Flush()
	}()

	wr := bufio.NewWriter(os.Stdout)
	defer wr.Flush()

	resultSetWriter(wr, rs)

	//for rs.Next() {
	//	series += 1
	//	cur := rs.Cursor()
	//	switch tcur := cur.(type) {
	//	case cursors.FloatArrayCursor:
	//		ts := tcur.Next()
	//		for ts.Len() > 0 {
	//			points += ts.Len()
	//			ts = tcur.Next()
	//		}
	//
	//	case cursors.IntegerArrayCursor:
	//		ts := tcur.Next()
	//		for ts.Len() > 0 {
	//			points += ts.Len()
	//			ts = tcur.Next()
	//		}
	//
	//	case cursors.UnsignedArrayCursor:
	//		ts := tcur.Next()
	//		for ts.Len() > 0 {
	//			points += ts.Len()
	//			ts = tcur.Next()
	//		}
	//
	//	case cursors.StringArrayCursor:
	//		ts := tcur.Next()
	//		for ts.Len() > 0 {
	//			points += ts.Len()
	//			ts = tcur.Next()
	//		}
	//
	//	case cursors.BooleanArrayCursor:
	//		ts := tcur.Next()
	//		for ts.Len() > 0 {
	//			points += ts.Len()
	//			ts = tcur.Next()
	//		}
	//
	//	default:
	//		panic(fmt.Sprintf("unexpected type: %T", cur))
	//	}
	//	cur.Close()
	//}

	return nil
}

func parseTime(v string) (int64, error) {
	if s, err := time.Parse(time.RFC3339, v); err == nil {
		return s.UnixNano(), nil
	}

	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i, nil
	}

	return 0, errors.New("invalid time")
}

func resultSetWriter(wr *bufio.Writer, rs reads.ResultSet) {
	for rs.Next() {
		tags := rs.Tags()
		if k := tags.HashKey(); len(k) > 0 {
			fmt.Fprintf(wr, "%s\n", string(k[1:]))
		}

		cur := rs.Cursor()
		if cur == nil {
			continue
		}

		cursorWriter(wr, cur)
	}
}

func cursorWriter(wr *bufio.Writer, cur cursors.Cursor) {
	switch ccur := cur.(type) {
	case cursors.IntegerArrayCursor:
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					fmt.Fprintf(wr, "%20d | %20d\n", a.Timestamps[i], a.Values[i])
				}
			} else {
				break
			}
		}
	case cursors.FloatArrayCursor:
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					fmt.Fprintf(wr, "%20d | %18.2f\n", a.Timestamps[i], a.Values[i])
				}
			} else {
				break
			}
		}
	case cursors.UnsignedArrayCursor:
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					fmt.Fprintf(wr, "%20d | %20d\n", a.Timestamps[i], a.Values[i])
				}
			} else {
				break
			}
		}
	case cursors.BooleanArrayCursor:
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					fmt.Fprintf(wr, "%20d | %t\n", a.Timestamps[i], a.Values[i])
				}
			} else {
				break
			}
		}
	case cursors.StringArrayCursor:
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					fmt.Fprintf(wr, "%20d | %20s\n", a.Timestamps[i], a.Values[i])
				}
			} else {
				break
			}
		}
	default:
		fmt.Fprintf(wr, "unreachable: %T\n", cur)
	}

	if err := cur.Err(); err != nil && err != io.EOF {
		fmt.Fprintf(wr, "cursor err: %s\n", cur.Err().Error())
	}

	cur.Close()
}

type exprToNodeVisitor struct {
	nodes []*datatypes.Node
	err   error
}

func (v *exprToNodeVisitor) Err() error {
	return v.err
}

func (v *exprToNodeVisitor) pop() (top *datatypes.Node) {
	if len(v.nodes) < 1 {
		panic("exprToNodeVisitor: stack empty")
	}

	top, v.nodes = v.nodes[len(v.nodes)-1], v.nodes[:len(v.nodes)-1]
	return
}

func (v *exprToNodeVisitor) pop2() (lhs, rhs *datatypes.Node) {
	if len(v.nodes) < 2 {
		panic("exprToNodeVisitor: stack empty")
	}

	rhs = v.nodes[len(v.nodes)-1]
	lhs = v.nodes[len(v.nodes)-2]
	v.nodes = v.nodes[:len(v.nodes)-2]
	return
}

func mapOpToComparison(op influxql.Token) datatypes.Node_Comparison {
	switch op {
	case influxql.EQ:
		return datatypes.ComparisonEqual
	case influxql.NEQ:
		return datatypes.ComparisonNotEqual
	case influxql.LT:
		return datatypes.ComparisonLess
	case influxql.LTE:
		return datatypes.ComparisonLessEqual
	case influxql.GT:
		return datatypes.ComparisonGreater
	case influxql.GTE:
		return datatypes.ComparisonGreaterEqual

	default:
		return -1
	}
}

func (v *exprToNodeVisitor) Visit(node influxql.Node) influxql.Visitor {
	switch n := node.(type) {
	case *influxql.BinaryExpr:
		if v.err != nil {
			return nil
		}

		influxql.Walk(v, n.LHS)
		if v.err != nil {
			return nil
		}

		influxql.Walk(v, n.RHS)
		if v.err != nil {
			return nil
		}

		if comp := mapOpToComparison(n.Op); comp != -1 {
			lhs, rhs := v.pop2()
			v.nodes = append(v.nodes, &datatypes.Node{
				NodeType: datatypes.NodeTypeComparisonExpression,
				Value:    &datatypes.Node_Comparison_{Comparison: comp},
				Children: []*datatypes.Node{lhs, rhs},
			})
		} else if n.Op == influxql.AND || n.Op == influxql.OR {
			var op datatypes.Node_Logical
			if n.Op == influxql.AND {
				op = datatypes.LogicalAnd
			} else {
				op = datatypes.LogicalOr
			}

			lhs, rhs := v.pop2()
			v.nodes = append(v.nodes, &datatypes.Node{
				NodeType: datatypes.NodeTypeLogicalExpression,
				Value:    &datatypes.Node_Logical_{Logical: op},
				Children: []*datatypes.Node{lhs, rhs},
			})
		} else {
			v.err = fmt.Errorf("unsupported operator, %s", n.Op)
		}

		return nil

	case *influxql.ParenExpr:
		influxql.Walk(v, n.Expr)
		if v.err != nil {
			return nil
		}

		v.nodes = append(v.nodes, &datatypes.Node{
			NodeType: datatypes.NodeTypeParenExpression,
			Children: []*datatypes.Node{v.pop()},
		})
		return nil

	case *influxql.StringLiteral:
		v.nodes = append(v.nodes, &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value:    &datatypes.Node_StringValue{StringValue: n.Val},
		})
		return nil

	case *influxql.NumberLiteral:
		v.nodes = append(v.nodes, &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value:    &datatypes.Node_FloatValue{FloatValue: n.Val},
		})
		return nil

	case *influxql.IntegerLiteral:
		v.nodes = append(v.nodes, &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value:    &datatypes.Node_IntegerValue{IntegerValue: n.Val},
		})
		return nil

	case *influxql.UnsignedLiteral:
		v.nodes = append(v.nodes, &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value:    &datatypes.Node_UnsignedValue{UnsignedValue: n.Val},
		})
		return nil

	case *influxql.VarRef:
		var val string
		switch n.Val {
		case "_measurement", "_m":
			val = models.MeasurementTagKey
		case "_field", "_f":
			val = models.FieldKeyTagKey
		default:
			val = n.Val
		}
		v.nodes = append(v.nodes, &datatypes.Node{
			NodeType: datatypes.NodeTypeTagRef,
			Value:    &datatypes.Node_TagRefValue{TagRefValue: val},
		})
		return nil

	default:
		v.err = errors.New("unsupported expression")
		return nil
	}
}
