package main

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/query/influxql"
	"github.com/spf13/cobra"
)

var transpileFlags struct {
	Now string
}

func cmdTranspile(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("transpile [InfluxQL query]", transpileF, false)
	cmd.Args = cobra.ExactArgs(1)
	cmd.Short = "Transpile an InfluxQL query to Flux source code"
	cmd.Long = `Transpile an InfluxQL query to Flux source code.


The transpiled query assumes that the bucket name is the of the form '<database>/<retention policy>'.

The transpiled query will be written for absolute time ranges using the provided now() time.`

	opts := flagOpts{
		{
			DestP: &transpileFlags.Now,
			Flag:  "now",
			Desc:  "An RFC3339Nano formatted time to use as the now() time. Defaults to the current time",
		},
	}
	opts.mustRegister(cmd)

	return cmd
}

func transpileF(cmd *cobra.Command, args []string) error {
	now := time.Now()
	if transpileFlags.Now != "" {
		var err error
		now, err = time.Parse(time.RFC3339Nano, transpileFlags.Now)
		if err != nil {
			return errors.Wrap(err, "invalid now time")
		}
	}
	t := influxql.NewTranspilerWithConfig(dbrpMapper{}, influxql.Config{
		Now:            now,
		FallbackToDBRP: true,
	})
	pkg, err := t.Transpile(context.Background(), args[0])
	if err != nil {
		return err
	}
	fmt.Println(ast.Format(pkg))
	return nil
}

type dbrpMapper struct{}

// FindBy returns the dbrp mapping for the specified ID.
func (d dbrpMapper) FindByID(ctx context.Context, orgID influxdb.ID, id influxdb.ID) (*influxdb.DBRPMappingV2, error) {
	return nil, errors.New("mapping not found")
}

// FindMany returns a list of dbrp mappings that match filter and the total count of matching dbrp mappings.
func (d dbrpMapper) FindMany(ctx context.Context, dbrp influxdb.DBRPMappingFilterV2, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMappingV2, int, error) {
	return nil, 0, errors.New("mapping not found")
}

// Create creates a new dbrp mapping, if a different mapping exists an error is returned.
func (d dbrpMapper) Create(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error {
	return errors.New("dbrpMapper does not support creating new mappings")
}

// Update a new dbrp mapping
func (d dbrpMapper) Update(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error {
	return errors.New("dbrpMapper does not support updating mappings")
}

// Delete removes a dbrp mapping.
func (d dbrpMapper) Delete(ctx context.Context, orgID influxdb.ID, id influxdb.ID) error {
	return errors.New("dbrpMapper does not support deleting mappings")
}
