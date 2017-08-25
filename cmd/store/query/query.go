package query

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"errors"

	"github.com/gogo/protobuf/codec"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/yarpc"
	"github.com/uber-go/zap"
)

// Command represents the program execution for "influx_inspect export".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger zap.Logger

	addr            string
	cpuProfile      string
	memProfile      string
	database        string
	retentionPolicy string
	startTime       int64
	endTime         int64
	limit           int
	offset          int
	desc            bool
	silent          bool
	expr            string
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
	fs.StringVar(&start, "start", "", "Optional: the start time to export (RFC3339 format)")
	fs.StringVar(&end, "end", "", "Optional: the end time to export (RFC3339 format)")
	fs.IntVar(&cmd.limit, "limit", 10, "Optional: limit number of rows")
	fs.IntVar(&cmd.offset, "offset", 0, "Optional: start offset for rows")
	fs.BoolVar(&cmd.desc, "desc", false, "Optional: return results in descending order")
	fs.BoolVar(&cmd.silent, "silent", false, "silence output")
	fs.StringVar(&cmd.expr, "expr", "", "InfluxQL expression")

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

	if err := cmd.validate(); err != nil {
		return err
	}

	opts := []yarpc.DialOption{yarpc.WithCodec(codec.New(1000))}
	conn, err := yarpc.Dial(cmd.addr, opts...)
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
	req.Database = cmd.database
	req.TimestampRange.Start = cmd.startTime
	req.TimestampRange.End = cmd.endTime
	req.Limit = int64(cmd.limit)

	if cmd.expr != "" {
		expr, err := influxql.ParseExpr(cmd.expr)
		if err != nil {
			return nil
		}
		fmt.Println(expr)
		var v exprToNodeVisitor
		influxql.Walk(&v, expr)
		if v.Err() != nil {
			return v.Err()
		}

		req.Predicate = &storage.Predicate{Root: v.nodes[0]}
	}

	stream, err := c.Read(context.Background(), &req)
	if err != nil {
		return err
	}

	integerSum := int64(0)
	floatSum := float64(0)
	var buf [1024]byte
	var line []byte
	wr := bufio.NewWriter(os.Stdout)

	now := time.Now()
	defer func() {
		dur := time.Since(now)
		fmt.Printf("time: %v\n", dur)
	}()

	for {
		var rep storage.ReadResponse

		if err := stream.RecvMsg(&rep); err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		for _, frame := range rep.Frames {
			if s := frame.GetSeries(); s != nil {
				if !cmd.silent {
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
					wr.WriteString("\n\033[0m")
					wr.Flush()
				}
			} else if p := frame.GetIntegerPoints(); p != nil {
				if cmd.silent {
					for _, v := range p.Values {
						integerSum += v
					}
				} else {
					for i := 0; i < len(p.Timestamps); i++ {
						line = buf[:0]
						wr.Write(strconv.AppendInt(line, p.Timestamps[i], 10))
						wr.WriteByte(' ')

						line = buf[:0]
						wr.Write(strconv.AppendInt(line, p.Values[i], 10))
						wr.WriteString("\n")
						wr.Flush()

						integerSum += p.Values[i]
					}
				}
			} else if p := frame.GetFloatPoints(); p != nil {
				if cmd.silent {
					for _, v := range p.Values {
						floatSum += v
					}
				} else {
					for i := 0; i < len(p.Timestamps); i++ {
						line = buf[:0]
						wr.Write(strconv.AppendInt(line, p.Timestamps[i], 10))
						wr.WriteByte(' ')

						line = buf[:0]
						wr.Write(strconv.AppendFloat(line, p.Values[i], 'f', 10, 64))
						wr.WriteString("\n")
						wr.Flush()

						floatSum += p.Values[i]
					}
				}
			}
		}
	}

	fmt.Println()
	fmt.Println("integerSum", integerSum, "floatSum", floatSum)

	return nil
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

		if n.Op == influxql.EQ {
			lhs, rhs := v.pop2()
			node := &storage.Node{
				NodeType: storage.NodeTypeComparisonExpression,
				Value:    &storage.Node_Comparison_{Comparison: storage.ComparisonEqual},
				Children: []*storage.Node{lhs, rhs},
			}
			v.nodes = append(v.nodes, node)
		} else if n.Op == influxql.AND || n.Op == influxql.OR {
			var op storage.Node_Logical
			if n.Op == influxql.AND {
				op = storage.LogicalAnd
			} else {
				op = storage.LogicalOr
			}

			lhs, rhs := v.pop2()
			node := &storage.Node{
				NodeType: storage.NodeTypeLogicalExpression,
				Value:    &storage.Node_Logical_{Logical: op},
				Children: []*storage.Node{lhs, rhs},
			}
			v.nodes = append(v.nodes, node)
		} else {
			v.err = fmt.Errorf("unsupported operator, %s", n.Op)
		}

		return nil

	case *influxql.ParenExpr:
		influxql.Walk(v, n.Expr)
		if v.err != nil {
			return nil
		}

		node := &storage.Node{
			NodeType: storage.NodeTypeParenExpression,
			Children: []*storage.Node{v.pop()},
		}
		v.nodes = append(v.nodes, node)
		return nil

	case *influxql.StringLiteral:
		node := &storage.Node{NodeType: storage.NodeTypeLiteral, Value: &storage.Node_StringValue{StringValue: n.Val}}
		v.nodes = append(v.nodes, node)
		return nil

	case *influxql.VarRef:
		node := &storage.Node{NodeType: storage.NodeTypeTagRef, Value: &storage.Node_TagRefValue{TagRefValue: n.Val}}
		v.nodes = append(v.nodes, node)
		return nil

	default:
		v.err = errors.New("unsupported expression")
		return nil
	}
}
