package typecheck_test

import (
	"bytes"
	"testing"

	typecheck "github.com/influxdata/influxdb/v2/cmd/influxd/inspect/type_conflicts"
	"github.com/stretchr/testify/assert"
)

func TestSchema_Encoding(t *testing.T) {
	s := typecheck.NewSchema()

	b := bytes.Buffer{}

	s.AddField("db1", "rp1", "foo", "v2", "float")
	s.AddField("db1", "rp1", "foo", "v2", "bool")
	s.AddField("db1", "rp1", "bZ", "v1", "int")

	err := s.Encode(&b)
	assert.NoError(t, err, "encode failed unexpectedly")
	s2 := typecheck.NewSchema()
	err = s2.Decode(&b)
	assert.NoError(t, err, "decode failed unexpectedly")
	assert.Len(t, s2, 2, "wrong number of fields - expected %d, got %d", 2, len(s))
	for f1, fields1 := range s {
		assert.Len(t,
			s2[f1],
			len(fields1),
			"differing number of types for a conflicted field %s: expected %d, got %d",
			f1,
			len(fields1),
			len(s2[f1]))
	}
}

type filler struct {
	typecheck.UniqueField
	typ string
}

func TestSchema_Merge(t *testing.T) {
	const expectedConflicts = 2
	s1Fill := []filler{
		{typecheck.UniqueField{"db1", "rp1", "m1", "f1"}, "integer"},
		{typecheck.UniqueField{"db2", "rp1", "m1", "f1"}, "float"},
		{typecheck.UniqueField{"db1", "rp2", "m1", "f1"}, "string"},
		{typecheck.UniqueField{"db1", "rp1", "m2", "f1"}, "string"},
		{typecheck.UniqueField{"db1", "rp1", "m1", "f2"}, "float"},
		{typecheck.UniqueField{"db2", "rp2", "m2", "f2"}, "integer"},
	}

	s2Fill := []filler{
		{typecheck.UniqueField{"db1", "rp1", "m1", "f1"}, "integer"},
		{typecheck.UniqueField{"db2", "rp1", "m1", "f1"}, "string"},
		{typecheck.UniqueField{"db2", "rp2", "m2", "f2"}, "float"},
		{typecheck.UniqueField{"db1", "rp2", "m1", "f1"}, "string"},
		{typecheck.UniqueField{"db1", "rp1", "m2", "f1"}, "string"},
		{typecheck.UniqueField{"db1", "rp1", "m1", "f2"}, "float"},
		{typecheck.UniqueField{"db2", "rp2", "m2", "f2"}, "integer"},
	}

	s1 := typecheck.NewSchema()
	s2 := typecheck.NewSchema()
	fillSchema(s1, s1Fill)
	fillSchema(s2, s2Fill)

	s1.Merge(s2)
	conflicts := s1.Conflicts()

	assert.Len(t, conflicts, expectedConflicts, "wrong number of type conflicts detected: expected %d, got %d", expectedConflicts, len(conflicts))
}

func fillSchema(s typecheck.Schema, fill []filler) {
	for _, f := range fill {
		s.AddFormattedField(f.String(), f.typ)
	}
}
