package typecheck

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	errors2 "github.com/influxdata/influxdb/pkg/errors"
	"github.com/influxdata/influxdb/tsdb"
)

type TypeConflictChecker struct {
	Path          string
	SchemaFile    string
	ConflictsFile string
}

func NewTypeConflictCheckerCommand() *TypeConflictChecker {
	return &TypeConflictChecker{}
}

func (tc *TypeConflictChecker) Run(args ...string) error {
	flags := flag.NewFlagSet("check-schema", flag.ExitOnError)
	flags.StringVar(&tc.Path, "path", ".", "Path under which fields.idx files are located")
	flags.StringVar(&tc.SchemaFile, "schema-file", "schema.json", "Filename schema data should be written to")
	flags.StringVar(&tc.ConflictsFile, "conflicts-file", "conflicts.json", "Filename conflicts data should be written to")

	if err := flags.Parse(args); err != nil {
		return err
	}

	// Get a set of every measurement/field/type tuple present.
	var schema Schema
	var err error

	tc.Path, err = filepath.Abs(tc.Path)
	if err != nil {
		return err
	}
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
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) (rErr error) {
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
		if len(dirs) < 4 {
			return fmt.Errorf("wrong directory structure for InfluxDB: %q", path)
		}
		db := dirs[len(dirs)-4]
		rp := dirs[len(dirs)-3]
		fmt.Printf("Processing %s\n", path)

		mfs, err := tsdb.NewMeasurementFieldSet(path, nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("unable to open file %q: %w", path, err)
		}
		defer errors2.Capture(&rErr, mfs.Close)()

		measurements := mfs.MeasurementNames()
		for _, m := range measurements {
			for f, typ := range mfs.FieldsByString(m).FieldSet() {
				schema.AddField(db, rp, m, f, typ.String())
			}
		}

		return nil
	})

	return schema, err
}
