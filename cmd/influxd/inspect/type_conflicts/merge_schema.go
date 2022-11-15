package typecheck

import (
	"errors"

	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type MergeFilesCommand struct {
	OutputFile    string
	ConflictsFile string
}

func NewMergeSchemaCommand(v *viper.Viper) (*cobra.Command, error) {
	flags := MergeFilesCommand{}

	cmd := &cobra.Command{
		Use:   "merge-schema",
		Short: "Merge a set of schema files from the check-schema command",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return mergeSchemaRunE(cmd, args, flags)
		},
	}

	opts := []cli.Opt{
		{
			DestP:   &flags.OutputFile,
			Flag:    "schema-file",
			Desc:    "Filename for the output file",
			Default: "schema.json",
		},
		{
			DestP:   &flags.ConflictsFile,
			Flag:    "conflicts-file",
			Desc:    "Filename conflicts data should be written to",
			Default: "conflicts.json",
		},
	}

	if err := cli.BindOptions(v, cmd, opts); err != nil {
		return nil, err
	}
	return cmd, nil
}

func mergeSchemaRunE(_ *cobra.Command, args []string, mf MergeFilesCommand) error {
	return mf.mergeFiles(args)
}

func (rc *MergeFilesCommand) mergeFiles(filenames []string) error {
	if len(filenames) < 1 {
		return errors.New("at least 1 file must be specified")
	}

	schema, err := SchemaFromFile(filenames[0])
	if err != nil {
		return err
	}

	for _, filename := range filenames[1:] {
		other, err := SchemaFromFile(filename)
		if err != nil {
			return err
		}
		schema.Merge(other)
	}

	if err := schema.WriteConflictsFile(rc.ConflictsFile); err != nil {
		return err
	}

	return schema.WriteSchemaFile(rc.OutputFile)
}
