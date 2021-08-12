package check_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/ast/astutil"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/stretchr/testify/require"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	influxTesting "github.com/influxdata/influxdb/v2/testing"
)

const (
	id1 = "020f755c3c082000"
	id2 = "020f755c3c082001"
	id3 = "020f755c3c082002"
)

var goodBase = check.Base{
	ID:                    influxTesting.MustIDBase16(id1),
	Name:                  "name1",
	OwnerID:               influxTesting.MustIDBase16(id2),
	OrgID:                 influxTesting.MustIDBase16(id3),
	StatusMessageTemplate: "temp1",
	Every:                 mustDuration("1m"),
	Tags: []influxdb.Tag{
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
			err: &errors.Error{
				Code: errors.EInvalid,
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
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "Check Name can't be empty",
			},
		},
		{
			name: "invalid owner id",
			src: &check.Threshold{
				Base: check.Base{
					ID:   influxTesting.MustIDBase16(id1),
					Name: "name1",
				},
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "Check OwnerID is invalid",
			},
		},
		{
			name: "invalid org id",
			src: &check.Threshold{
				Base: check.Base{
					ID:      influxTesting.MustIDBase16(id1),
					Name:    "name1",
					OwnerID: influxTesting.MustIDBase16(id2),
				},
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "Check OrgID is invalid",
			},
		},
		{
			name: "nil every",
			src: &check.Deadman{
				Base: check.Base{
					ID:                    influxTesting.MustIDBase16(id1),
					Name:                  "name1",
					OwnerID:               influxTesting.MustIDBase16(id2),
					OrgID:                 influxTesting.MustIDBase16(id3),
					StatusMessageTemplate: "temp1",
					Tags:                  []influxdb.Tag{{Key: "key1"}},
				},
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "Check Every must exist",
			},
		},
		{
			name: "offset greater then interval",
			src: &check.Deadman{
				Base: check.Base{
					ID:      influxTesting.MustIDBase16(id1),
					Name:    "name1",
					OwnerID: influxTesting.MustIDBase16(id2),
					OrgID:   influxTesting.MustIDBase16(id3),
					Every:   mustDuration("1m"),
					Offset:  mustDuration("2m"),
				},
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "Offset should not be equal or greater than the interval",
			},
		},
		{
			name: "invalid tag",
			src: &check.Deadman{
				Base: check.Base{
					ID:                    influxTesting.MustIDBase16(id1),
					Name:                  "name1",
					OwnerID:               influxTesting.MustIDBase16(id2),
					OrgID:                 influxTesting.MustIDBase16(id3),
					StatusMessageTemplate: "temp1",
					Every:                 mustDuration("1m"),
					Tags:                  []influxdb.Tag{{Key: "key1"}},
				},
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "tag must contain a key and a value",
			},
		},
		{
			name: "bad threshold",
			src: &check.Threshold{
				Base: goodBase,
				Thresholds: []check.ThresholdConfig{
					&check.Range{Min: 200, Max: 100},
				},
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "range threshold min can't be larger than max",
			},
		},
	}
	for _, c := range cases {
		got := c.src.Valid(fluxlang.DefaultService)
		influxTesting.ErrorsEqual(t, got, c.err)
	}
}

var timeGen1 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 13, 4, 19, 10, 0, time.UTC)}
var timeGen2 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 14, 5, 23, 53, 10, time.UTC)}

func mustDuration(d string) *notification.Duration {
	dur, err := parser.ParseDuration(d)
	if err != nil {
		panic(err)
	}

	return (*notification.Duration)(dur)
}

func TestJSON(t *testing.T) {
	cases := []struct {
		name string
		src  influxdb.Check
	}{
		{
			name: "simple Deadman",
			src: &check.Deadman{
				Base: check.Base{
					ID:      influxTesting.MustIDBase16(id1),
					OwnerID: influxTesting.MustIDBase16(id2),
					Name:    "name1",
					OrgID:   influxTesting.MustIDBase16(id3),
					Every:   mustDuration("1h"),
					Query: influxdb.DashboardQuery{
						BuilderConfig: influxdb.BuilderConfig{
							Buckets: []string{},
							Tags: []struct {
								Key                   string   `json:"key"`
								Values                []string `json:"values"`
								AggregateFunctionType string   `json:"aggregateFunctionType"`
							}{},
							Functions: []struct {
								Name string `json:"name"`
							}{},
						},
					},
					Tags: []influxdb.Tag{
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
				TimeSince:  mustDuration("33s"),
				ReportZero: true,
				Level:      notification.Warn,
			},
		},
		{
			name: "simple threshold",
			src: &check.Threshold{
				Base: check.Base{
					ID:      influxTesting.MustIDBase16(id1),
					Name:    "name1",
					OwnerID: influxTesting.MustIDBase16(id2),
					OrgID:   influxTesting.MustIDBase16(id3),
					Every:   mustDuration("1h"),
					Query: influxdb.DashboardQuery{
						BuilderConfig: influxdb.BuilderConfig{
							Buckets: []string{},
							Tags: []struct {
								Key                   string   `json:"key"`
								Values                []string `json:"values"`
								AggregateFunctionType string   `json:"aggregateFunctionType"`
							}{},
							Functions: []struct {
								Name string `json:"name"`
							}{},
						},
					},
					Tags: []influxdb.Tag{
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
		fn := func(t *testing.T) {
			b, err := json.Marshal(c.src)
			if err != nil {
				t.Fatalf("%s marshal failed, err: %s", c.name, err.Error())
			}
			got, err := check.UnmarshalJSON(b)
			if err != nil {
				t.Fatalf("%s unmarshal failed, err: %s", c.name, err.Error())
			}
			if diff := cmp.Diff(got, c.src, cmpopts.IgnoreFields(notification.Duration{}, "BaseNode")); diff != "" {
				t.Errorf("failed %s, Check are different -got/+want\ndiff %s", c.name, diff)
			}
		}
		t.Run(c.name, fn)
	}
}

func mustFormatPackage(t *testing.T, pkg *ast.Package) string {
	if len(pkg.Files) == 0 {
		t.Fatal("package expected to have at least one file")
	}
	v, err := astutil.Format(pkg.Files[0])
	require.NoError(t, err)
	return v
}
