package query

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxql"
	"github.com/influxdata/yarpc"
	"go.uber.org/zap"
)

// Command represents the program execution for "influx_inspect export".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger *zap.Logger

	addr            string
	cpuProfile      string
	memProfile      string
	database        string
	retentionPolicy string
	startTime       int64
	endTime         int64
	limit           uint64
	slimit          uint64
	soffset         uint64
	desc            bool
	silent          bool
	expr            string
	agg             string
	grouping        string
	keys            []string

	aggType storage.Aggregate_AggregateType

	// response
	integerSum  int64
	unsignedSum uint64
	floatSum    float64
	pointCount  uint64
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
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

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	var start, end string
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	fs.StringVar(&cmd.cpuProfile, "cpuprofile", "", "CPU profile name")
	fs.StringVar(&cmd.memProfile, "memprofile", "", "memory profile name")
	fs.StringVar(&cmd.addr, "addr", ":8082", "the RPC address")
	fs.StringVar(&cmd.database, "database", "", "Optional: the database to export")
	fs.StringVar(&cmd.retentionPolicy, "retention", "", "Optional: the retention policy to export (requires -database)")
	fs.StringVar(&start, "start", "", "Optional: the start time to query (RFC3339 format)")
	fs.StringVar(&end, "end", "", "Optional: the end time to query (RFC3339 format)")
	fs.Uint64Var(&cmd.slimit, "slimit", 0, "Optional: limit number of series")
	fs.Uint64Var(&cmd.soffset, "soffset", 0, "Optional: start offset for series")
	fs.Uint64Var(&cmd.limit, "limit", 0, "Optional: limit number of values per series")
	fs.BoolVar(&cmd.desc, "desc", false, "Optional: return results in descending order")
	fs.BoolVar(&cmd.silent, "silent", false, "silence output")
	fs.StringVar(&cmd.expr, "expr", "", "InfluxQL conditional expression")
	fs.StringVar(&cmd.agg, "agg", "", "aggregate functions (sum, count)")
	fs.StringVar(&cmd.grouping, "grouping", "", "comma-separated list of tags to specify series order")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = func() {
		fmt.Fprintln(cmd.Stdout, "Query via RPC")
		fmt.Fprintf(cmd.Stdout, "Usage: %s query [flags]\n\n", filepath.Base(os.Args[0]))
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}

	// set defaults
	if start != "" {
		if t, err := parseTime(start); err != nil {
			return err
		} else {
			cmd.startTime = t
		}
	} else {
		cmd.startTime = models.MinNanoTime
	}
	if end != "" {
		if t, err := parseTime(end); err != nil {
			return err
		} else {
			cmd.endTime = t
		}
	} else {
		// set end time to max if it is not set.
		cmd.endTime = models.MaxNanoTime
	}

	if cmd.agg != "" {
		tm := proto.EnumValueMap("storage.Aggregate_AggregateType")
		if agg, ok := tm[strings.ToUpper(cmd.agg)]; !ok {
			return errors.New("invalid aggregate function: " + cmd.agg)
		} else {
			cmd.aggType = storage.Aggregate_AggregateType(agg)
		}
	}

	if cmd.grouping != "" {
		cmd.keys = strings.Split(cmd.grouping, ",")
	}

	if err := cmd.validate(); err != nil {
		return err
	}

	conn, err := yarpc.Dial(cmd.addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	c := storage.NewStorageClient(conn)
	return cmd.query(c)
}

func (cmd *Command) validate() error {
	if cmd.retentionPolicy != "" && cmd.database == "" {
		return fmt.Errorf("must specify a db")
	}
	if cmd.startTime != 0 && cmd.endTime != 0 && cmd.endTime < cmd.startTime {
		return fmt.Errorf("end time before start time")
	}
	return nil
}

func (cmd *Command) query(c storage.StorageClient) error {
	var req storage.ReadRequest
	var db = cmd.database
	if cmd.retentionPolicy != "" {
		db += "/" + cmd.retentionPolicy
	}

	req.Database = db
	req.TimestampRange.Start = cmd.startTime
	req.TimestampRange.End = cmd.endTime
	req.SeriesLimit = cmd.slimit
	req.SeriesOffset = cmd.soffset
	req.PointsLimit = cmd.limit
	req.Descending = cmd.desc
	req.Grouping = cmd.keys

	if cmd.aggType != storage.AggregateTypeNone {
		req.Aggregate = &storage.Aggregate{Type: cmd.aggType}
	}

	if cmd.expr != "" {
		expr, err := influxql.ParseExpr(cmd.expr)
		if err != nil {
			return nil
		}
		fmt.Fprintln(cmd.Stdout, expr)
		var v exprToNodeVisitor
		influxql.Walk(&v, expr)
		if v.Err() != nil {
			return v.Err()
		}

		req.Predicate = &storage.Predicate{Root: v.nodes[0]}
	}

	stream, err := c.Read(context.Background(), &req)
	if err != nil {
		fmt.Fprintln(cmd.Stdout, err)
		return err
	}

	wr := bufio.NewWriter(os.Stdout)

	now := time.Now()
	defer func() {
		dur := time.Since(now)
		fmt.Fprintf(cmd.Stdout, "time: %v\n", dur)
	}()

	for {
		var rep storage.ReadResponse

		if err = stream.RecvMsg(&rep); err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		if cmd.silent {
			cmd.processFramesSilent(rep.Frames)
		} else {
			cmd.processFrames(wr, rep.Frames)
		}
	}

	fmt.Fprintln(cmd.Stdout)
	fmt.Fprint(cmd.Stdout, "points(count): ", cmd.pointCount, ", sum(int64): ", cmd.integerSum, ", sum(uint64): ", cmd.unsignedSum, ", sum(float64): ", cmd.floatSum, "\n")

	return nil
}

func (cmd *Command) processFramesSilent(frames []storage.ReadResponse_Frame) {
	for _, frame := range frames {
		switch f := frame.Data.(type) {
		case *storage.ReadResponse_Frame_IntegerPoints:
			for _, v := range f.IntegerPoints.Values {
				cmd.integerSum += v
			}
			cmd.pointCount += uint64(len(f.IntegerPoints.Values))

		case *storage.ReadResponse_Frame_UnsignedPoints:
			for _, v := range f.UnsignedPoints.Values {
				cmd.unsignedSum += v
			}
			cmd.pointCount += uint64(len(f.UnsignedPoints.Values))

		case *storage.ReadResponse_Frame_FloatPoints:
			for _, v := range f.FloatPoints.Values {
				cmd.floatSum += v
			}
			cmd.pointCount += uint64(len(f.FloatPoints.Values))

		case *storage.ReadResponse_Frame_StringPoints:
			cmd.pointCount += uint64(len(f.StringPoints.Values))

		case *storage.ReadResponse_Frame_BooleanPoints:
			cmd.pointCount += uint64(len(f.BooleanPoints.Values))
		}
	}
}

func (cmd *Command) processFrames(wr *bufio.Writer, frames []storage.ReadResponse_Frame) {
	var buf [1024]byte
	var line []byte

	for _, frame := range frames {
		switch f := frame.Data.(type) {
		case *storage.ReadResponse_Frame_Series:
			s := f.Series
			wr.WriteString("\033[36m")
			first := true
			for _, t := range s.Tags {
				if !first {
					wr.WriteByte(',')
				} else {
					first = false
				}
				wr.Write(t.Key)
				wr.WriteByte(':')
				wr.Write(t.Value)
			}
			wr.WriteString("\033[0m\n")
			wr.Flush()

		case *storage.ReadResponse_Frame_IntegerPoints:
			p := f.IntegerPoints
			for i := 0; i < len(p.Timestamps); i++ {
				line = buf[:0]
				wr.Write(strconv.AppendInt(line, p.Timestamps[i], 10))
				wr.WriteByte(' ')

				line = buf[:0]
				wr.Write(strconv.AppendInt(line, p.Values[i], 10))
				wr.WriteString("\n")
				wr.Flush()

				cmd.integerSum += p.Values[i]
			}
			cmd.pointCount += uint64(len(f.IntegerPoints.Values))

		case *storage.ReadResponse_Frame_UnsignedPoints:
			p := f.UnsignedPoints
			for i := 0; i < len(p.Timestamps); i++ {
				line = buf[:0]
				wr.Write(strconv.AppendInt(line, p.Timestamps[i], 10))
				wr.WriteByte(' ')

				line = buf[:0]
				wr.Write(strconv.AppendUint(line, p.Values[i], 10))
				wr.WriteString("\n")
				wr.Flush()

				cmd.unsignedSum += p.Values[i]
			}
			cmd.pointCount += uint64(len(f.UnsignedPoints.Values))

		case *storage.ReadResponse_Frame_FloatPoints:
			p := f.FloatPoints
			for i := 0; i < len(p.Timestamps); i++ {
				line = buf[:0]
				wr.Write(strconv.AppendInt(line, p.Timestamps[i], 10))
				wr.WriteByte(' ')

				line = buf[:0]
				wr.Write(strconv.AppendFloat(line, p.Values[i], 'f', 10, 64))
				wr.WriteString("\n")
				wr.Flush()

				cmd.floatSum += p.Values[i]
			}
			cmd.pointCount += uint64(len(f.FloatPoints.Values))

		case *storage.ReadResponse_Frame_StringPoints:
			p := f.StringPoints
			for i := 0; i < len(p.Timestamps); i++ {
				line = buf[:0]
				wr.Write(strconv.AppendInt(line, p.Timestamps[i], 10))
				wr.WriteByte(' ')

				line = buf[:0]
				wr.WriteString(p.Values[i])
				wr.WriteString("\n")
				wr.Flush()
			}
			cmd.pointCount += uint64(len(f.StringPoints.Values))

		case *storage.ReadResponse_Frame_BooleanPoints:
			p := f.BooleanPoints
			for i := 0; i < len(p.Timestamps); i++ {
				line = buf[:0]
				wr.Write(strconv.AppendInt(line, p.Timestamps[i], 10))
				wr.WriteByte(' ')

				line = buf[:0]
				if p.Values[i] {
					wr.WriteString("true")
				} else {
					wr.WriteString("false")
				}
				wr.WriteString("\n")
				wr.Flush()
			}
			cmd.pointCount += uint64(len(f.BooleanPoints.Values))
		}
	}
}

type exprToNodeVisitor struct {
	nodes []*storage.Node
	err   error
}

func (v *exprToNodeVisitor) Err() error {
	return v.err
}

func (v *exprToNodeVisitor) pop() (top *storage.Node) {
	if len(v.nodes) < 1 {
		panic("exprToNodeVisitor: stack empty")
	}

	top, v.nodes = v.nodes[len(v.nodes)-1], v.nodes[:len(v.nodes)-1]
	return
}

func (v *exprToNodeVisitor) pop2() (lhs, rhs *storage.Node) {
	if len(v.nodes) < 2 {
		panic("exprToNodeVisitor: stack empty")
	}

	rhs = v.nodes[len(v.nodes)-1]
	lhs = v.nodes[len(v.nodes)-2]
	v.nodes = v.nodes[:len(v.nodes)-2]
	return
}

func mapOpToComparison(op influxql.Token) storage.Node_Comparison {
	switch op {
	case influxql.EQ:
		return storage.ComparisonEqual
	case influxql.NEQ:
		return storage.ComparisonNotEqual
	case influxql.LT:
		return storage.ComparisonLess
	case influxql.LTE:
		return storage.ComparisonLessEqual
	case influxql.GT:
		return storage.ComparisonGreater
	case influxql.GTE:
		return storage.ComparisonGreaterEqual

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
			v.nodes = append(v.nodes, &storage.Node{
				NodeType: storage.NodeTypeComparisonExpression,
				Value:    &storage.Node_Comparison_{Comparison: comp},
				Children: []*storage.Node{lhs, rhs},
			})
		} else if n.Op == influxql.AND || n.Op == influxql.OR {
			var op storage.Node_Logical
			if n.Op == influxql.AND {
				op = storage.LogicalAnd
			} else {
				op = storage.LogicalOr
			}

			lhs, rhs := v.pop2()
			v.nodes = append(v.nodes, &storage.Node{
				NodeType: storage.NodeTypeLogicalExpression,
				Value:    &storage.Node_Logical_{Logical: op},
				Children: []*storage.Node{lhs, rhs},
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

		v.nodes = append(v.nodes, &storage.Node{
			NodeType: storage.NodeTypeParenExpression,
			Children: []*storage.Node{v.pop()},
		})
		return nil

	case *influxql.StringLiteral:
		v.nodes = append(v.nodes, &storage.Node{
			NodeType: storage.NodeTypeLiteral,
			Value:    &storage.Node_StringValue{StringValue: n.Val},
		})
		return nil

	case *influxql.NumberLiteral:
		v.nodes = append(v.nodes, &storage.Node{
			NodeType: storage.NodeTypeLiteral,
			Value:    &storage.Node_FloatValue{FloatValue: n.Val},
		})
		return nil

	case *influxql.IntegerLiteral:
		v.nodes = append(v.nodes, &storage.Node{
			NodeType: storage.NodeTypeLiteral,
			Value:    &storage.Node_IntegerValue{IntegerValue: n.Val},
		})
		return nil

	case *influxql.UnsignedLiteral:
		v.nodes = append(v.nodes, &storage.Node{
			NodeType: storage.NodeTypeLiteral,
			Value:    &storage.Node_UnsignedValue{UnsignedValue: n.Val},
		})
		return nil

	case *influxql.VarRef:
		v.nodes = append(v.nodes, &storage.Node{
			NodeType: storage.NodeTypeTagRef,
			Value:    &storage.Node_TagRefValue{TagRefValue: n.Val},
		})
		return nil

	default:
		v.err = errors.New("unsupported expression")
		return nil
	}
}
