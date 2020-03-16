package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/errors"
	"github.com/influxdata/influxdb/query/influxql"
	"github.com/spf13/cobra"
)

var transpileFlags struct {
	Now      string
	UseStdin bool
}

func cmdTranspile(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("transpile [InfluxQL query]", transpileF)
	cmd.Args = cobra.MaximumNArgs(1)
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
		{
			DestP: &transpileFlags.UseStdin,
			Flag:  "use-stdin",
			Desc:  "Read InfluxQL query from STDIN instead of a command line argument",
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
	var inputQuery string
	if transpileFlags.UseStdin {
		buf := new(bytes.Buffer)
		_, err := io.Copy(buf, os.Stdin)
		if err != nil {
			return errors.Wrap(err, "failed to read query from STDIN")
		}
		inputQuery = buf.String()
	} else {
		inputQuery = args[0]
	}
	pkg, err := t.Transpile(context.Background(), inputQuery)
	if err != nil {
		return err
	}
	fmt.Println(ast.Format(pkg))
	return nil
}

type dbrpMapper struct{}

func (m dbrpMapper) FindBy(ctx context.Context, cluster string, db string, rp string) (*influxdb.DBRPMapping, error) {
	return nil, errors.New("mapping not found")
}
func (m dbrpMapper) Find(ctx context.Context, filter influxdb.DBRPMappingFilter) (*influxdb.DBRPMapping, error) {
	return nil, errors.New("mapping not found")
}
func (m dbrpMapper) FindMany(ctx context.Context, filter influxdb.DBRPMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.DBRPMapping, int, error) {
	return nil, 0, errors.New("mapping not found")

}
func (m dbrpMapper) Create(ctx context.Context, dbrpMap *influxdb.DBRPMapping) error {
	return errors.New("dbrpMapper does not support creating new mappings")
}
func (m dbrpMapper) Delete(ctx context.Context, cluster string, db string, rp string) error {
	return errors.New("dbrpMapper does not support deleteing mappings")
}
