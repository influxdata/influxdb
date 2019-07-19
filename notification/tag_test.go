package notification_test

import (
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
	influxTesting "github.com/influxdata/influxdb/testing"
)

func TestTagValid(t *testing.T) {
	cases := []struct {
		name string
		src  notification.TagRule
		err  error
	}{
		{
			name: "regular status rule",
			src: notification.TagRule{
				Tag:      notification.Tag{Key: "k1", Value: "v1"},
				Operator: notification.Equal,
			},
		},
		{
			name: "empty",
			src:  notification.TagRule{},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "tag must contain a key and a value",
			},
		},
		{
			name: "empty key",
			src: notification.TagRule{
				Tag: notification.Tag{Value: "v1"},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "tag must contain a key and a value",
			},
		},
		{
			name: "empty value",
			src: notification.TagRule{
				Tag: notification.Tag{Key: "k1"},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "tag must contain a key and a value",
			},
		},
		{
			name: "invalid operator",
			src: notification.TagRule{
				Tag: notification.Tag{Key: "k1", Value: "v1"},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Operator \"\" is invalid",
			},
		},
	}
	for _, c := range cases {
		err := c.src.Valid()
		influxTesting.ErrorsEqual(t, err, c.err)
	}
}
