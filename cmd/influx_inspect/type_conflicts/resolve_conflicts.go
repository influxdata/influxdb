package typecheck

import (
	"errors"
	"flag"
)

type MergeFilesCommand struct {
	OutputFile    string
	ConflictsFile string
}

func NewMergeFilesCommand() *MergeFilesCommand {
	return &MergeFilesCommand{}
}

func (rc *MergeFilesCommand) Run(args ...string) error {
	flags := flag.NewFlagSet("merge-schema", flag.ExitOnError)
	flags.StringVar(&rc.OutputFile, "schema-file", "schema.json", "Filename for the output file")
	flags.StringVar(&rc.ConflictsFile, "conflicts-file", "conflicts.json", "Filename conflicts data should be written to")

	if err := flags.Parse(args); err != nil {
		return err
	}

	return rc.mergeFiles(flags.Args())
}

func (rc *MergeFilesCommand) mergeFiles(filenames []string) error {
	if len(filenames) < 1 {
		return errors.New("At least 1 file must be specified")
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
