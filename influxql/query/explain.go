package query

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/influxdata/influxql"
)

func (p *preparedStatement) Explain() (string, error) {
	// Determine the cost of all iterators created as part of this plan.
	ic := &explainIteratorCreator{ic: p.ic}
	p.ic = ic
	cur, err := p.Select(context.Background())
	p.ic = ic.ic

	if err != nil {
		return "", err
	}
	cur.Close()

	var buf bytes.Buffer
	for i, node := range ic.nodes {
		if i > 0 {
			buf.WriteString("\n")
		}

		expr := "<nil>"
		if node.Expr != nil {
			expr = node.Expr.String()
		}
		fmt.Fprintf(&buf, "EXPRESSION: %s\n", expr)
		if len(node.Aux) != 0 {
			refs := make([]string, len(node.Aux))
			for i, ref := range node.Aux {
				refs[i] = ref.String()
			}
			fmt.Fprintf(&buf, "AUXILIARY FIELDS: %s\n", strings.Join(refs, ", "))
		}
		fmt.Fprintf(&buf, "NUMBER OF SHARDS: %d\n", node.Cost.NumShards)
		fmt.Fprintf(&buf, "NUMBER OF SERIES: %d\n", node.Cost.NumSeries)
		fmt.Fprintf(&buf, "CACHED VALUES: %d\n", node.Cost.CachedValues)
		fmt.Fprintf(&buf, "NUMBER OF FILES: %d\n", node.Cost.NumFiles)
		fmt.Fprintf(&buf, "NUMBER OF BLOCKS: %d\n", node.Cost.BlocksRead)
		fmt.Fprintf(&buf, "SIZE OF BLOCKS: %d\n", node.Cost.BlockSize)
	}
	return buf.String(), nil
}

type planNode struct {
	Expr influxql.Expr
	Aux  []influxql.VarRef
	Cost IteratorCost
}

type explainIteratorCreator struct {
	ic interface {
		IteratorCreator
		io.Closer
	}
	nodes []planNode
}

func (e *explainIteratorCreator) CreateIterator(ctx context.Context, m *influxql.Measurement, opt IteratorOptions) (Iterator, error) {
	cost, err := e.ic.IteratorCost(m, opt)
	if err != nil {
		return nil, err
	}
	e.nodes = append(e.nodes, planNode{
		Expr: opt.Expr,
		Aux:  opt.Aux,
		Cost: cost,
	})
	return &nilFloatIterator{}, nil
}

func (e *explainIteratorCreator) IteratorCost(m *influxql.Measurement, opt IteratorOptions) (IteratorCost, error) {
	return e.ic.IteratorCost(m, opt)
}

func (e *explainIteratorCreator) Close() error {
	return e.ic.Close()
}
