package influxdb_test

import (
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"testing"

	"github.com/influxdata/influxdb/v2"
	influxTesting "github.com/influxdata/influxdb/v2/testing"
)

func TestTagValid(t *testing.T) {
	cases := []struct {
		name string
		src  influxdb.TagRule
		err  error
	}{
		{
			name: "regular status rule",
			src: influxdb.TagRule{
				Tag:      influxdb.Tag{Key: "k1", Value: "v1"},
				Operator: influxdb.Equal,
			},
		},
		{
			name: "empty",
			src:  influxdb.TagRule{},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "tag must contain a key and a value",
			},
		},
		{
			name: "empty key",
			src: influxdb.TagRule{
				Tag: influxdb.Tag{Value: "v1"},
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "tag must contain a key and a value",
			},
		},
		{
			name: "empty value",
			src: influxdb.TagRule{
				Tag: influxdb.Tag{Key: "k1"},
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "tag must contain a key and a value",
			},
		},
		{
			name: "invalid operator",
			src: influxdb.TagRule{
				Tag:      influxdb.Tag{Key: "k1", Value: "v1"},
				Operator: influxdb.Operator(-1),
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "Operator is invalid",
			},
		},
	}
	for _, c := range cases {
		err := c.src.Valid()
		influxTesting.ErrorsEqual(t, err, c.err)
	}
}
