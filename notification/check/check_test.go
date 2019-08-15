package check_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/notification"

	"github.com/influxdata/influxdb/mock"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/check"
	influxTesting "github.com/influxdata/influxdb/testing"
)

const (
	id1 = "020f755c3c082000"
	id2 = "020f755c3c082001"
	id3 = "020f755c3c082002"
)

var goodBase = check.Base{
	ID:                    influxTesting.MustIDBase16(id1),
	Name:                  "name1",
	AuthorizationID:       influxTesting.MustIDBase16(id2),
	OrgID:                 influxTesting.MustIDBase16(id3),
	Status:                influxdb.Active,
	StatusMessageTemplate: "temp1",
	Tags: []notification.Tag{
		{Key: "k1", Value: "v1"},
		{Key: "k2", Value: "v2"},
	},
}

func TestValidCheck(t *testing.T) {
	cases := []struct {
		name string
		src  influxdb.Check
		err  error
	}{
		{
			name: "invalid check id",
			src:  &check.Deadman{},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Check ID is invalid",
			},
		},
		{
			name: "empty name",
			src: &check.Threshold{
				Base: check.Base{
					ID: influxTesting.MustIDBase16(id1),
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Check Name can't be empty",
			},
		},
		{
			name: "invalid auth id",
			src: &check.Threshold{
				Base: check.Base{
					ID:   influxTesting.MustIDBase16(id1),
					Name: "name1",
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Check AuthorizationID is invalid",
			},
		},
		{
			name: "invalid org id",
			src: &check.Threshold{
				Base: check.Base{
					ID:              influxTesting.MustIDBase16(id1),
					Name:            "name1",
					AuthorizationID: influxTesting.MustIDBase16(id2),
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Check OrgID is invalid",
			},
		},
		{
			name: "invalid status",
			src: &check.Deadman{
				Base: check.Base{
					ID:              influxTesting.MustIDBase16(id1),
					Name:            "name1",
					AuthorizationID: influxTesting.MustIDBase16(id2),
					OrgID:           influxTesting.MustIDBase16(id3),
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "invalid status",
			},
		},
		{
			name: "invalid tag",
			src: &check.Deadman{
				Base: check.Base{
					ID:                    influxTesting.MustIDBase16(id1),
					Name:                  "name1",
					AuthorizationID:       influxTesting.MustIDBase16(id2),
					OrgID:                 influxTesting.MustIDBase16(id3),
					Status:                influxdb.Active,
					StatusMessageTemplate: "temp1",
					Tags:                  []notification.Tag{{Key: "key1"}},
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "tag must contain a key and a value",
			},
		},
		{
			name: "bad thredshold",
			src: &check.Threshold{
				Base: goodBase,
				Thresholds: []check.ThresholdConfig{
					&check.Range{Min: 200, Max: 100},
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "range threshold min can't be larger than max",
			},
		},
	}
	for _, c := range cases {
		got := c.src.Valid()
		influxTesting.ErrorsEqual(t, got, c.err)
	}
}

var timeGen1 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 13, 4, 19, 10, 0, time.UTC)}
var timeGen2 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 14, 5, 23, 53, 10, time.UTC)}

func TestJSON(t *testing.T) {
	cases := []struct {
		name string
		src  influxdb.Check
	}{
		{
			name: "simple Deadman",
			src: &check.Deadman{
				Base: check.Base{
					ID:              influxTesting.MustIDBase16(id1),
					AuthorizationID: influxTesting.MustIDBase16(id2),
					Name:            "name1",
					OrgID:           influxTesting.MustIDBase16(id3),
					Status:          influxdb.Active,
					Every:           influxdb.Duration{Duration: time.Hour},
					Tags: []notification.Tag{
						{
							Key:   "k1",
							Value: "v1",
						},
						{
							Key:   "k2",
							Value: "v2",
						},
					},
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: timeGen1.Now(),
						UpdatedAt: timeGen2.Now(),
					},
				},
				TimeSince:  33,
				ReportZero: true,
				Level:      notification.Warn,
			},
		},
		{
			name: "simple threshold",
			src: &check.Threshold{
				Base: check.Base{
					ID:              influxTesting.MustIDBase16(id1),
					Name:            "name1",
					AuthorizationID: influxTesting.MustIDBase16(id2),
					OrgID:           influxTesting.MustIDBase16(id3),
					Status:          influxdb.Active,
					Every:           influxdb.Duration{Duration: time.Hour},
					Tags: []notification.Tag{
						{
							Key:   "k1",
							Value: "v1",
						},
						{
							Key:   "k2",
							Value: "v2",
						},
					},
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: timeGen1.Now(),
						UpdatedAt: timeGen2.Now(),
					},
				},
				Thresholds: []check.ThresholdConfig{
					&check.Greater{ThresholdConfigBase: check.ThresholdConfigBase{AllValues: true}, Value: -1.36},
					&check.Range{Min: -10000, Max: 500},
					&check.Lesser{ThresholdConfigBase: check.ThresholdConfigBase{Level: notification.Critical}},
				},
			},
		},
	}
	for _, c := range cases {
		b, err := json.Marshal(c.src)
		if err != nil {
			t.Fatalf("%s marshal failed, err: %s", c.name, err.Error())
		}
		got, err := check.UnmarshalJSON(b)
		if err != nil {
			t.Fatalf("%s unmarshal failed, err: %s", c.name, err.Error())
		}
		if diff := cmp.Diff(got, c.src); diff != "" {
			t.Errorf("failed %s, Check are different -got/+want\ndiff %s", c.name, diff)
		}
	}
}
