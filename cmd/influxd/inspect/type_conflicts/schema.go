package typecheck

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	errors2 "github.com/influxdata/influxdb/v2/pkg/errors"
)

type UniqueField struct {
	Database    string `json:"database"`
	Retention   string `json:"retention"`
	Measurement string `json:"measurement"`
	Field       string `json:"field"`
}

type FieldTypes map[string]struct{}
type Schema map[string]FieldTypes

func (ft FieldTypes) MarshalText() (text []byte, err error) {
	s := make([]string, 0, len(ft))
	for f := range ft {
		s = append(s, f)
	}
	return []byte(strings.Join(s, ",")), nil
}

func (ft *FieldTypes) UnmarshalText(text []byte) error {
	if *ft == nil {
		*ft = make(FieldTypes)
	}
	for _, ty := range strings.Split(string(text), ",") {
		(*ft)[ty] = struct{}{}
	}
	return nil
}

func NewSchema() Schema {
	return make(Schema)
}

func SchemaFromFile(filename string) (Schema, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("unable to open schema file %q: %w", filename, err)
	}

	s := NewSchema()
	if err := s.Decode(f); err != nil {
		return nil, fmt.Errorf("unable to decode schema file %q: %w", filename, err)
	}
	return s, nil
}

func (uf *UniqueField) String() string {
	return fmt.Sprintf("%q.%q.%q.%q", uf.Database, uf.Retention, uf.Measurement, uf.Field)
}

func (s Schema) AddField(database, retention, measurement, field, dataType string) {
	uf := UniqueField{
		Database:    database,
		Retention:   retention,
		Measurement: measurement,
		Field:       field,
	}
	s.AddFormattedField(uf.String(), dataType)
}

func (s Schema) AddFormattedField(field string, dataType string) {
	if _, ok := s[field]; !ok {
		s[field] = make(map[string]struct{})
	}
	s[field][dataType] = struct{}{}
}

func (s Schema) Merge(schema Schema) {
	for field, types := range schema {
		for t := range types {
			s.AddFormattedField(field, t)
		}
	}
}

func (s Schema) Conflicts() Schema {
	cs := NewSchema()
	for field, t := range s {
		if len(t) > 1 {
			for ty := range t {
				cs.AddFormattedField(field, ty)
			}
		}
	}
	return cs
}

func (s Schema) WriteSchemaFile(filename string) error {
	if len(s) == 0 {
		fmt.Println("No schema file generated: no valid measurements/fields found")
		return nil
	}

	if err := s.encodeSchema(filename); err != nil {
		return fmt.Errorf("unable to write schema file to %q: %w", filename, err)
	}
	fmt.Printf("Schema file written successfully to: %q\n", filename)
	return nil
}

func (s Schema) WriteConflictsFile(filename string) error {
	conflicts := s.Conflicts()
	if len(conflicts) == 0 {
		fmt.Println("No conflicts file generated: no conflicts found")
		return nil
	}

	if err := conflicts.encodeSchema(filename); err != nil {
		return fmt.Errorf("unable to write conflicts file to %q: %w", filename, err)
	}
	fmt.Printf("Conflicts file written successfully to: %q\n", filename)
	return nil
}

func (s Schema) encodeSchema(filename string) (rErr error) {
	schemaFile, err := os.Create(filename)
	defer errors2.Capture(&rErr, schemaFile.Close)
	if err != nil {
		return fmt.Errorf("unable to create schema file: %w", err)
	}
	return s.Encode(schemaFile)
}

func (s Schema) Encode(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(s); err != nil {
		return fmt.Errorf("unable to encode schema: %w", err)
	}
	return nil
}

func (s Schema) Decode(r io.Reader) error {
	if err := json.NewDecoder(r).Decode(&s); err != nil {
		return fmt.Errorf("unable to decode schema: %w", err)
	}
	return nil
}
