package typecheck

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type TypeConflictChecker struct {
	Path          string
	SchemaFile    string
	ConflictsFile string
	Logger        *zap.Logger

	logLevel zapcore.Level
}

func NewCheckSchemaCommand(v *viper.Viper) (*cobra.Command, error) {
	flags := TypeConflictChecker{}

	cmd := &cobra.Command{
		Use:   "check-schema",
		Short: "Check for conflicts in the types between shards",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return checkSchemaRunE(cmd, flags)
		},
	}
	opts := []cli.Opt{
		{
			DestP:   &flags.Path,
			Flag:    "path",
			Desc:    "Path under which fields.idx files are located",
			Default: ".",
		},
		{
			DestP:   &flags.SchemaFile,
			Flag:    "schema-file",
			Desc:    "Filename schema data should be written to",
			Default: "schema.json",
		},
		{
			DestP:   &flags.ConflictsFile,
			Flag:    "conflicts-file",
			Desc:    "Filename conflicts data should be written to",
			Default: "conflicts.json",
		},
		{
			DestP:   &flags.logLevel,
			Flag:    "log-level",
			Desc:    "The level of logging used througout the command",
			Default: zap.InfoLevel,
		},
	}

	if err := cli.BindOptions(v, cmd, opts); err != nil {
		return nil, err
	}
	return cmd, nil
}

func checkSchemaRunE(_ *cobra.Command, tc TypeConflictChecker) error {
	logconf := zap.NewProductionConfig()
	logconf.Level = zap.NewAtomicLevelAt(tc.logLevel)
	logger, err := logconf.Build()
	if err != nil {
		return err
	}
	tc.Logger = logger

	// Get a set of every measurement/field/type tuple present.
	var schema Schema
	schema, err = tc.readFields()
	if err != nil {
		return err
	}

	if err := schema.WriteSchemaFile(tc.SchemaFile); err != nil {
		return err
	}
	if err := schema.WriteConflictsFile(tc.ConflictsFile); err != nil {
		return err
	}

	return nil
}

func (tc *TypeConflictChecker) readFields() (Schema, error) {
	schema := NewSchema()
	var root string
	fi, err := os.Stat(tc.Path)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		root = tc.Path
	} else {
		root = path.Dir(tc.Path)
	}
	fileSystem := os.DirFS(".")
	err = fs.WalkDir(fileSystem, root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error walking file: %w", err)
		}

		if filepath.Base(path) == tsdb.FieldsChangeFile {
			fmt.Printf("WARN: A %s file was encountered at %s. The database was not shutdown properly, results of this command may be incomplete\n",
				tsdb.FieldsChangeFile,
				path,
			)
			return nil
		}

		if filepath.Base(path) != "fields.idx" {
			return nil
		}

		dirs := strings.Split(path, string(os.PathSeparator))
		bucket := dirs[len(dirs)-4]
		rp := dirs[len(dirs)-3]
		fmt.Printf("Processing %s\n", path)

		mfs, err := tsdb.NewMeasurementFieldSet(path, tc.Logger)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("unable to open file %q: %w", path, err)
		}
		defer mfs.Close()

		measurements := mfs.MeasurementNames()
		for _, m := range measurements {
			for f, typ := range mfs.FieldsByString(m).FieldSet() {
				schema.AddField(bucket, rp, m, f, typ.String())
			}
		}

		return nil
	})

	return schema, err
}
