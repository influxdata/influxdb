package functions

import (
	"fmt"

	"context"
	"strings"

	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/ifql/semantic"
	"github.com/influxdata/platform/query/csv"
	"github.com/pkg/errors"
)

const FromCSVKind = "fromCSV"

type FromCSVOpSpec struct {
	CSV string `json:"csv"`
}

var fromCSVSignature = semantic.FunctionSignature{
	Params: map[string]semantic.Type{
		"csv": semantic.String,
	},
	ReturnType: query.TableObjectType,
}

func init() {
	query.RegisterFunction(FromCSVKind, createFromCSVOpSpec, fromCSVSignature)
	query.RegisterOpSpec(FromCSVKind, newFromCSVOp)
	plan.RegisterProcedureSpec(FromCSVKind, newFromCSVProcedure, FromCSVKind)
	execute.RegisterSource(FromCSVKind, createFromCSVSource)
}

func createFromCSVOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	spec := new(FromCSVOpSpec)

	if csv, ok, err := args.GetString("db"); err != nil {
		return nil, err
	} else if ok {
		spec.CSV = csv
	}

	if spec.CSV == "" {
		return nil, errors.New("must provide csv text")
	}

	// TODO(adam): validate the CSV before we go much further?
	return spec, nil
}

func newFromCSVOp() query.OperationSpec {
	return new(FromCSVOpSpec)
}

func (s *FromCSVOpSpec) Kind() query.OperationKind {
	return FromCSVKind
}

type FromCSVProcedureSpec struct {
	CSV string
}

func newFromCSVProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*FromCSVOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &FromCSVProcedureSpec{
		CSV: spec.CSV,
	}, nil
}

func (s *FromCSVProcedureSpec) Kind() plan.ProcedureKind {
	return FromCSVKind
}

func (s *FromCSVProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(FromCSVProcedureSpec)
	ns.CSV = s.CSV
	return ns
}

func createFromCSVSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	spec, ok := prSpec.(*FromCSVProcedureSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", prSpec)
	}

	decoder := csv.NewResultDecoder(csv.ResultDecoderConfig{})
	result, err := decoder.Decode(strings.NewReader(spec.CSV))
	if err != nil {
		return nil, err
	}
	csvSource := CSVSource{id: dsid, data: result}

	return &csvSource, nil
}

type CSVSource struct {
	id   execute.DatasetID
	data execute.Result
	ts   []execute.Transformation
}

func (c *CSVSource) AddTransformation(t execute.Transformation) {
	c.ts = append(c.ts, t)
}

func (c *CSVSource) Run(ctx context.Context) {
	var err error
	for _, t := range c.ts {
		var max execute.Time
		err = c.data.Blocks().Do(func(b execute.Block) error {
			err := t.Process(c.id, b)
			if err != nil {
				return err
			}
			if s := b.Bounds().Stop; s > max {
				max = s
			}
			return nil
		})
		if err != nil {
			break
		}

		t.UpdateWatermark(c.id, max)
	}

	for _, t := range c.ts {
		t.Finish(c.id, err)
	}

}
