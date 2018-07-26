package functions

import (
	"fmt"
	"io/ioutil"
	"os"

	"context"
	"strings"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/csv"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
	"github.com/pkg/errors"
)

const FromCSVKind = "fromCSV"

type FromCSVOpSpec struct {
	CSV  string `json:"csv"`
	File string `json:"file"`
}

var fromCSVSignature = semantic.FunctionSignature{
	Params: map[string]semantic.Type{
		"csv":  semantic.String,
		"file": semantic.String,
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

	if csv, ok, err := args.GetString("csv"); err != nil {
		return nil, err
	} else if ok {
		spec.CSV = csv
	}

	if file, ok, err := args.GetString("file"); err != nil {
		return nil, err
	} else if ok {
		spec.File = file
	}

	if spec.CSV == "" && spec.File == "" {
		return nil, errors.New("must provide csv raw text or filename")
	}

	if spec.CSV != "" && spec.File != "" {
		return nil, errors.New("must provide exactly one of the parameters csv or file")
	}

	if spec.File != "" {
		if _, err := os.Stat(spec.File); err != nil {
			return nil, errors.Wrap(err, "failed to stat csv file: ")
		}
	}

	return spec, nil
}

func newFromCSVOp() query.OperationSpec {
	return new(FromCSVOpSpec)
}

func (s *FromCSVOpSpec) Kind() query.OperationKind {
	return FromCSVKind
}

type FromCSVProcedureSpec struct {
	CSV  string
	File string
}

func newFromCSVProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*FromCSVOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &FromCSVProcedureSpec{
		CSV:  spec.CSV,
		File: spec.File,
	}, nil
}

func (s *FromCSVProcedureSpec) Kind() plan.ProcedureKind {
	return FromCSVKind
}

func (s *FromCSVProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(FromCSVProcedureSpec)
	ns.CSV = s.CSV
	ns.File = s.File
	return ns
}

func createFromCSVSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	spec, ok := prSpec.(*FromCSVProcedureSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", prSpec)
	}

	csvText := spec.CSV
	// if spec.File non-empty then spec.CSV is empty
	if spec.File != "" {
		csvBytes, err := ioutil.ReadFile(spec.File)
		if err != nil {
			return nil, err
		}
		csvText = string(csvBytes)
	}

	decoder := csv.NewResultDecoder(csv.ResultDecoderConfig{})
	result, err := decoder.Decode(strings.NewReader(csvText))
	if err != nil {
		return nil, err
	}
	csvSource := CSVSource{id: dsid, data: result}

	return &csvSource, nil
}

type CSVSource struct {
	id   execute.DatasetID
	data query.Result
	ts   []execute.Transformation
}

func (c *CSVSource) AddTransformation(t execute.Transformation) {
	c.ts = append(c.ts, t)
}

func (c *CSVSource) Run(ctx context.Context) {
	var err error
	var max execute.Time
	maxSet := false
	err = c.data.Tables().Do(func(tbl query.Table) error {
		for _, t := range c.ts {
			err := t.Process(c.id, tbl)
			if err != nil {
				return err
			}
			if idx := execute.ColIdx(execute.DefaultStopColLabel, tbl.Key().Cols()); idx >= 0 {
				if stop := tbl.Key().ValueTime(idx); !maxSet || stop > max {
					max = stop
					maxSet = true
				}
			}
		}
		return nil
	})
	if err != nil {
		goto FINISH
	}

	if maxSet {
		for _, t := range c.ts {
			t.UpdateWatermark(c.id, max)
		}
	}

FINISH:
	for _, t := range c.ts {
		t.Finish(c.id, err)
	}
}
