package testing

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
)

// IDPtr returns a pointer to an influxdb.ID.
func IDPtr(id influxdb.ID) *influxdb.ID { return &id }

// IDFields include the IDGenerator
type IDFields struct {
	IDGenerator influxdb.IDGenerator
}

// ID testing
func ID(
	init func(IDFields, *testing.T) (influxdb.IDGenerator, string, func()),
	t *testing.T,
) {
	type wants struct {
		id influxdb.ID
	}

	tests := []struct {
		name   string
		fields IDFields
		wants  wants
	}{
		{
			name: "generate id",
			fields: IDFields{
				IDGenerator: mock.NewIDGenerator(oneID, t),
			},
			wants: wants{
				id: MustIDBase16(oneID),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()

			id := s.ID()
			if diff := cmp.Diff(id, tt.wants.id); diff != "" {
				t.Errorf("ids are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
