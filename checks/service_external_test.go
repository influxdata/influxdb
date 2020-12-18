package checks

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/check"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	orgOneID = "020f755c3c083000"
	orgTwoID = "020f755c3c083001"

	oneID   = "020f755c3c082000"
	twoID   = "020f755c3c082001"
	threeID = "020f755c3c082002"
	fourID  = "020f755c3c082003"
	fiveID  = "020f755c3c082004"
	sixID   = "020f755c3c082005"

	checkOneID = "020f755c3c082000"
	checkTwoID = "020f755c3c082001"
)

var script = `data = from(bucket: "telegraf") |> range(start: -1m) |> filter(fn: (r) => r._field == "usage_user")`

var deadman1 = &check.Deadman{
	Base: check.Base{
		Name:        "name1",
		ID:          MustIDBase16(checkOneID),
		OrgID:       MustIDBase16(orgOneID),
		OwnerID:     MustIDBase16(sixID),
		Description: "desc1",
		TaskID:      1,
		Query: influxdb.DashboardQuery{
			Text: script,
			BuilderConfig: influxdb.BuilderConfig{
				Buckets: []string{},
				Tags: []struct {
					Key                   string   `json:"key"`
					Values                []string `json:"values"`
					AggregateFunctionType string   `json:"aggregateFunctionType"`
				}{
					{
						Key:                   "_field",
						Values:                []string{"usage_user"},
						AggregateFunctionType: "filter",
					},
				},
				Functions: []struct {
					Name string `json:"name"`
				}{},
			},
		},
		Every:                 mustDuration("1m"),
		StatusMessageTemplate: "msg1",
		Tags: []influxdb.Tag{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
		},
		CRUDLog: influxdb.CRUDLog{
			CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
			UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
		},
	},
	TimeSince:  mustDuration("21s"),
	StaleTime:  mustDuration("1h"),
	ReportZero: true,
	Level:      notification.Critical,
}

var threshold1 = &check.Threshold{
	Base: check.Base{
		Name:                  "name2",
		ID:                    MustIDBase16(checkTwoID),
		OrgID:                 MustIDBase16(orgTwoID),
		OwnerID:               MustIDBase16(sixID),
		TaskID:                1,
		Description:           "desc2",
		StatusMessageTemplate: "msg2",
		Every:                 mustDuration("1m"),
		Query: influxdb.DashboardQuery{
			Text: script,
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
			{Key: "k11", Value: "v11"},
		},
		CRUDLog: influxdb.CRUDLog{
			CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
			UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
		},
	},
	Thresholds: []check.ThresholdConfig{
		&check.Lesser{
			ThresholdConfigBase: check.ThresholdConfigBase{
				Level: notification.Ok,
			},
			Value: 1000,
		},
		&check.Greater{
			ThresholdConfigBase: check.ThresholdConfigBase{
				Level: notification.Warn,
			},
			Value: 2000,
		},
		&check.Range{
			ThresholdConfigBase: check.ThresholdConfigBase{
				Level: notification.Info,
			},
			Min:    1500,
			Max:    1900,
			Within: true,
		},
	},
}

var checkCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmpopts.IgnoreFields(check.Base{}, "TaskID"),
	cmp.Transformer("Sort", func(in []influxdb.Check) []influxdb.Check {
		out := append([]influxdb.Check(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].GetID() > out[j].GetID()
		})
		return out
	}),
}

var taskCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	// skip comparing permissions
	cmpopts.IgnoreFields(
		influxdb.Task{},
		"LatestCompleted",
		"LatestScheduled",
		"CreatedAt",
		"UpdatedAt",
	),
	cmp.Transformer("Sort", func(in []*influxdb.Task) []*influxdb.Task {
		out := append([]*influxdb.Task{}, in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID > out[j].ID
		})
		return out
	}),
}

// CheckFields will include the IDGenerator, and checks
type CheckFields struct {
	IDGenerator   influxdb.IDGenerator
	TimeGenerator influxdb.TimeGenerator
	TaskService   influxdb.TaskService
	Checks        []influxdb.Check
	Organizations []*influxdb.Organization
	Tasks         []influxdb.TaskCreate
}

type checkServiceFactory func(CheckFields, *testing.T) (influxdb.CheckService, influxdb.TaskService, string, func())

type checkServiceF func(
	init checkServiceFactory,
	t *testing.T,
)

// CheckService tests all the service functions.
func CheckService(
	init checkServiceFactory,
	t *testing.T,
) {
	tests := []struct {
		name string
		fn   checkServiceF
	}{
		{
			name: "CreateCheck",
			fn:   CreateCheck,
		},
		{
			name: "FindCheckByID",
			fn:   FindCheckByID,
		},
		{
			name: "FindChecks",
			fn:   FindChecks,
		},
		{
			name: "FindCheck",
			fn:   FindCheck,
		},
		{
			name: "PatchCheck",
			fn:   PatchCheck,
		},
		{
			name: "UpdateCheck",
			fn:   UpdateCheck,
		},
		{
			name: "DeleteCheck",
			fn:   DeleteCheck,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// CreateCheck testing
func CreateCheck(
	init checkServiceFactory,
	t *testing.T,
) {
	type args struct {
		userID influxdb.ID
		check  influxdb.Check
	}
	type wants struct {
		err    *influxdb.Error
		checks []influxdb.Check
		tasks  []*influxdb.Task
	}

	tests := []struct {
		name   string
		fields CheckFields
		args   args
		wants  wants
	}{
		{
			name: "create checks with empty set",
			fields: CheckFields{
				IDGenerator:   mock.NewIDGenerator(checkOneID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Checks:        []influxdb.Check{},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				userID: MustIDBase16(twoID),
				check: &check.Deadman{
					Base: check.Base{
						Name:        "name1",
						OrgID:       MustIDBase16(orgOneID),
						Description: "desc1",
						Query: influxdb.DashboardQuery{
							Text: script,
							BuilderConfig: influxdb.BuilderConfig{
								Tags: []struct {
									Key                   string   `json:"key"`
									Values                []string `json:"values"`
									AggregateFunctionType string   `json:"aggregateFunctionType"`
								}{
									{
										Key:                   "_field",
										Values:                []string{"usage_user"},
										AggregateFunctionType: "filter",
									},
								},
							},
						},
						Every:                 mustDuration("1m"),
						StatusMessageTemplate: "msg1",
						Tags: []influxdb.Tag{
							{Key: "k1", Value: "v1"},
							{Key: "k2", Value: "v2"},
						},
					},
					TimeSince:  mustDuration("21s"),
					StaleTime:  mustDuration("1h"),
					ReportZero: true,
					Level:      notification.Critical,
				},
			},
			wants: wants{
				tasks: []*influxdb.Task{
					{
						ID:             MustIDBase16("020f755c3c082000"),
						Name:           "name1",
						Type:           "deadman",
						OrganizationID: MustIDBase16("020f755c3c083000"),
						Organization:   "theorg",
						OwnerID:        MustIDBase16("020f755c3c082001"),
						Status:         "active",
						Flux:           "package main\nimport \"influxdata/influxdb/monitor\"\nimport \"experimental\"\nimport \"influxdata/influxdb/v1\"\n\ndata = from(bucket: \"telegraf\")\n\t|> range(start: -1h)\n\t|> filter(fn: (r) =>\n\t\t(r._field == \"usage_user\"))\n\noption task = {name: \"name1\", every: 1m}\n\ncheck = {\n\t_check_id: \"020f755c3c082000\",\n\t_check_name: \"name1\",\n\t_type: \"deadman\",\n\ttags: {k1: \"v1\", k2: \"v2\"},\n}\ncrit = (r) =>\n\t(r[\"dead\"])\nmessageFn = (r) =>\n\t(\"msg1\")\n\ndata\n\t|> v1[\"fieldsAsCols\"]()\n\t|> monitor[\"deadman\"](t: experimental[\"subDuration\"](from: now(), d: 21s))\n\t|> monitor[\"check\"](data: check, messageFn: messageFn, crit: crit)",
						Every:          "1m",
					},
				},
				checks: []influxdb.Check{
					&check.Deadman{
						Base: check.Base{
							Name:    "name1",
							ID:      MustIDBase16(checkOneID),
							OrgID:   MustIDBase16(orgOneID),
							OwnerID: MustIDBase16(twoID),
							Query: influxdb.DashboardQuery{
								Text: script,
								BuilderConfig: influxdb.BuilderConfig{
									Buckets: []string{},
									Tags: []struct {
										Key                   string   `json:"key"`
										Values                []string `json:"values"`
										AggregateFunctionType string   `json:"aggregateFunctionType"`
									}{
										{
											Key:                   "_field",
											Values:                []string{"usage_user"},
											AggregateFunctionType: "filter",
										},
									},
									Functions: []struct {
										Name string `json:"name"`
									}{},
								},
							},
							Every:                 mustDuration("1m"),
							Description:           "desc1",
							StatusMessageTemplate: "msg1",
							Tags: []influxdb.Tag{
								{Key: "k1", Value: "v1"},
								{Key: "k2", Value: "v2"},
							},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
								UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							},
						},
						TimeSince:  mustDuration("21s"),
						StaleTime:  mustDuration("1h"),
						ReportZero: true,
						Level:      notification.Critical,
					},
				},
			},
		},
		{
			name: "basic create check",
			fields: CheckFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() influxdb.ID {
						return MustIDBase16(checkTwoID)
					},
				},
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Checks: []influxdb.Check{
					deadman1,
				},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "otherorg",
						ID:   MustIDBase16(orgTwoID),
					},
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				check: &check.Threshold{
					Base: check.Base{
						Name:                  "name2",
						OrgID:                 MustIDBase16(orgTwoID),
						OwnerID:               MustIDBase16(twoID),
						Description:           "desc2",
						StatusMessageTemplate: "msg2",
						Every:                 mustDuration("1m"),
						Query: influxdb.DashboardQuery{
							Text: script,
						},
						Tags: []influxdb.Tag{
							{Key: "k11", Value: "v11"},
						},
					},
					Thresholds: []check.ThresholdConfig{
						&check.Lesser{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Ok,
							},
							Value: 1000,
						},
						&check.Greater{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Warn,
							},
							Value: 2000,
						},
						&check.Range{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Info,
							},
							Min:    1500,
							Max:    1900,
							Within: true,
						},
					},
				},
			},
			wants: wants{
				checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
				tasks: []*influxdb.Task{
					{
						ID:             MustIDBase16("020f755c3c082001"),
						Name:           "name2",
						Type:           "threshold",
						OrganizationID: MustIDBase16("020f755c3c083001"),
						Organization:   "otherorg",
						OwnerID:        MustIDBase16("020f755c3c082005"),
						Status:         "active",
						Every:          "1m",
						Flux:           "package main\nimport \"influxdata/influxdb/monitor\"\nimport \"influxdata/influxdb/v1\"\n\ndata = from(bucket: \"telegraf\")\n\t|> range(start: -1m)\n\t|> filter(fn: (r) =>\n\t\t(r._field == \"usage_user\"))\n\noption task = {name: \"name2\", every: 1m}\n\ncheck = {\n\t_check_id: \"020f755c3c082001\",\n\t_check_name: \"name2\",\n\t_type: \"threshold\",\n\ttags: {k11: \"v11\"},\n}\nok = (r) =>\n\t(r[\"usage_user\"] < 1000.0)\nwarn = (r) =>\n\t(r[\"usage_user\"] > 2000.0)\ninfo = (r) =>\n\t(r[\"usage_user\"] < 1900.0 and r[\"usage_user\"] > 1500.0)\nmessageFn = (r) =>\n\t(\"msg2\")\n\ndata\n\t|> v1[\"fieldsAsCols\"]()\n\t|> monitor[\"check\"](\n\t\tdata: check,\n\t\tmessageFn: messageFn,\n\t\tok: ok,\n\t\twarn: warn,\n\t\tinfo: info,\n\t)",
					},
				},
			},
		},
		{
			name: "names should be unique within an organization",
			fields: CheckFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() influxdb.ID {
						return MustIDBase16(checkTwoID)
					},
				},
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Checks: []influxdb.Check{
					deadman1,
				},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "otherorg",
						ID:   MustIDBase16(orgTwoID),
					},
				},
			},
			args: args{
				userID: MustIDBase16(twoID),
				check: &check.Threshold{
					Base: check.Base{
						Name:        "name1",
						OrgID:       MustIDBase16(orgOneID),
						Description: "desc1",
						Every:       mustDuration("1m"),
						Query: influxdb.DashboardQuery{
							Text: script,
							BuilderConfig: influxdb.BuilderConfig{
								Tags: []struct {
									Key                   string   `json:"key"`
									Values                []string `json:"values"`
									AggregateFunctionType string   `json:"aggregateFunctionType"`
								}{
									{
										Key:                   "_field",
										Values:                []string{"usage_user"},
										AggregateFunctionType: "filter",
									},
								},
							},
						},
						StatusMessageTemplate: "msg1",
						Tags: []influxdb.Tag{
							{Key: "k1", Value: "v1"},
							{Key: "k2", Value: "v2"},
						},
					},
				},
			},
			wants: wants{
				checks: []influxdb.Check{
					deadman1,
				},
				err: &influxdb.Error{
					Code: influxdb.EConflict,
					Op:   influxdb.OpCreateCheck,
					Msg:  "check is not unique",
				},
			},
		},
		{
			name: "names should not be unique across organizations",
			fields: CheckFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() influxdb.ID {
						return MustIDBase16(checkTwoID)
					},
				},
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "otherorg",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
				},
			},
			args: args{
				userID: MustIDBase16(twoID),
				check: &check.Threshold{
					Base: check.Base{
						Name:        "name1",
						OrgID:       MustIDBase16(orgTwoID),
						Description: "desc2",
						Every:       mustDuration("1m"),
						Query: influxdb.DashboardQuery{
							Text: script,
							BuilderConfig: influxdb.BuilderConfig{
								Tags: []struct {
									Key                   string   `json:"key"`
									Values                []string `json:"values"`
									AggregateFunctionType string   `json:"aggregateFunctionType"`
								}{
									{
										Key:                   "_field",
										Values:                []string{"usage_user"},
										AggregateFunctionType: "filter",
									},
								},
							},
						},
						StatusMessageTemplate: "msg2",
						Tags: []influxdb.Tag{
							{Key: "k11", Value: "v11"},
							{Key: "k22", Value: "v22"},
						},
					},
				},
			},
			wants: wants{
				tasks: []*influxdb.Task{
					{
						ID:             MustIDBase16("020f755c3c082001"),
						Name:           "name1",
						Type:           "threshold",
						OrganizationID: MustIDBase16("020f755c3c083001"),
						Organization:   "otherorg",
						OwnerID:        MustIDBase16("020f755c3c082001"),
						Status:         "active",
						Every:          "1m",
						Flux:           "package main\nimport \"influxdata/influxdb/monitor\"\nimport \"influxdata/influxdb/v1\"\n\ndata = from(bucket: \"telegraf\")\n\t|> range(start: -1m)\n\t|> filter(fn: (r) =>\n\t\t(r._field == \"usage_user\"))\n\noption task = {name: \"name1\", every: 1m}\n\ncheck = {\n\t_check_id: \"020f755c3c082001\",\n\t_check_name: \"name1\",\n\t_type: \"threshold\",\n\ttags: {k11: \"v11\", k22: \"v22\"},\n}\nmessageFn = (r) =>\n\t(\"msg2\")\n\ndata\n\t|> v1[\"fieldsAsCols\"]()\n\t|> monitor[\"check\"](data: check, messageFn: messageFn)",
					},
				},
				checks: []influxdb.Check{
					deadman1,
					&check.Threshold{
						Base: check.Base{
							ID:    MustIDBase16(checkTwoID),
							Name:  "name1",
							OrgID: MustIDBase16(orgTwoID),
							Every: mustDuration("1m"),
							Query: influxdb.DashboardQuery{
								Text: script,
								BuilderConfig: influxdb.BuilderConfig{
									Buckets: []string{},
									Tags: []struct {
										Key                   string   `json:"key"`
										Values                []string `json:"values"`
										AggregateFunctionType string   `json:"aggregateFunctionType"`
									}{
										{
											Key:                   "_field",
											Values:                []string{"usage_user"},
											AggregateFunctionType: "filter",
										},
									},
									Functions: []struct {
										Name string `json:"name"`
									}{},
								},
							},
							OwnerID:               MustIDBase16(twoID),
							Description:           "desc2",
							StatusMessageTemplate: "msg2",
							Tags: []influxdb.Tag{
								{Key: "k11", Value: "v11"},
								{Key: "k22", Value: "v22"},
							},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
								UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							},
						},
					},
				},
			},
		},
		{
			name: "create check with orgID not exist",
			fields: CheckFields{
				IDGenerator:   mock.NewIDGenerator(checkOneID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Checks:        []influxdb.Check{},
				Organizations: []*influxdb.Organization{},
			},
			args: args{
				userID: MustIDBase16(twoID),
				check: &check.Threshold{
					Base: check.Base{
						Name:  "name1",
						OrgID: MustIDBase16(orgOneID),
						Every: mustDuration("1m"),
						Query: influxdb.DashboardQuery{
							Text: script,
							BuilderConfig: influxdb.BuilderConfig{
								Tags: []struct {
									Key                   string   `json:"key"`
									Values                []string `json:"values"`
									AggregateFunctionType string   `json:"aggregateFunctionType"`
								}{
									{
										Key:                   "_field",
										Values:                []string{"usage_user"},
										AggregateFunctionType: "filter",
									},
								},
							},
						},
						Description:           "desc2",
						StatusMessageTemplate: "msg2",
						Tags: []influxdb.Tag{
							{Key: "k11", Value: "v11"},
							{Key: "k22", Value: "v22"},
						},
					},
				},
			},
			wants: wants{
				checks: []influxdb.Check{},
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  "organization not found",
					Op:   influxdb.OpCreateCheck,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, tasks, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			createCheck := influxdb.CheckCreate{Check: tt.args.check, Status: influxdb.Active}
			err := s.CreateCheck(ctx, createCheck, tt.args.userID)
			influxErrsEqual(t, tt.wants.err, err)

			defer s.DeleteCheck(ctx, tt.args.check.GetID())

			checks, _, err := s.FindChecks(ctx, influxdb.CheckFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve checks: %v", err)
			}
			if diff := cmp.Diff(checks, tt.wants.checks, checkCmpOptions...); diff != "" {
				t.Errorf("checks are different -got/+want\ndiff %s", diff)
			}

			foundTasks, _, err := tasks.FindTasks(ctx, influxdb.TaskFilter{})
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(foundTasks, tt.wants.tasks, taskCmpOptions); diff != "" {
				t.Errorf("tasks are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindCheckByID testing
func FindCheckByID(
	init checkServiceFactory,
	t *testing.T,
) {
	type args struct {
		id influxdb.ID
	}
	type wants struct {
		err   *influxdb.Error
		check influxdb.Check
	}

	tests := []struct {
		name   string
		fields CheckFields
		args   args
		wants  wants
	}{
		{
			name: "basic find check by id",
			fields: CheckFields{
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				id: MustIDBase16(checkTwoID),
			},
			wants: wants{
				check: threshold1,
			},
		},
		{
			name: "find check by id not exist",
			fields: CheckFields{
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				id: MustIDBase16(threeID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpFindCheckByID,
					Msg:  "check not found",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			check, err := s.FindCheckByID(ctx, tt.args.id)
			influxErrsEqual(t, tt.wants.err, err)

			if diff := cmp.Diff(check, tt.wants.check, checkCmpOptions...); diff != "" {
				t.Errorf("check is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindChecks testing
func FindChecks(
	init checkServiceFactory,
	t *testing.T,
) {
	type args struct {
		ID           influxdb.ID
		name         string
		organization string
		OrgID        influxdb.ID
		userID       influxdb.ID
		findOptions  influxdb.FindOptions
	}

	type wants struct {
		checks []influxdb.Check
		err    error
	}
	tests := []struct {
		name   string
		fields CheckFields
		args   args
		wants  wants
	}{
		{
			name: "find all checks",
			fields: CheckFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "otherorg",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
			},
			wants: wants{
				checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
			},
		},
		{
			name: "find all checks by offset and limit",
			fields: CheckFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
			},
			args: args{
				findOptions: influxdb.FindOptions{
					Offset: 1,
					Limit:  1,
				},
			},
			wants: wants{
				checks: []influxdb.Check{
					threshold1,
				},
			},
		},
		{
			name: "find all checks by descending",
			fields: CheckFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				findOptions: influxdb.FindOptions{
					Limit:      1,
					Descending: true,
				},
			},
			wants: wants{
				checks: []influxdb.Check{
					threshold1,
				},
			},
		},
		{
			name: "find checks by organization name",
			fields: CheckFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "otherorg",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
			},
			args: args{
				userID:       MustIDBase16(sixID),
				organization: "theorg",
			},
			wants: wants{
				checks: []influxdb.Check{
					deadman1,
				},
			},
		},
		{
			name: "find checks by organization id",
			fields: CheckFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "otherorg",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				OrgID:  MustIDBase16(orgTwoID),
			},
			wants: wants{
				checks: []influxdb.Check{
					threshold1,
				},
			},
		},
		{
			name: "find check by name",
			fields: CheckFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				name:   "name2",
			},
			wants: wants{
				checks: []influxdb.Check{
					threshold1,
				},
			},
		},
		{
			name: "missing check returns no checks",
			fields: CheckFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Checks: []influxdb.Check{},
			},
			args: args{
				userID: MustIDBase16(sixID),
				name:   "xyz",
			},
			wants: wants{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			filter := influxdb.CheckFilter{}
			if tt.args.ID.Valid() {
				filter.ID = &tt.args.ID
			}
			if tt.args.OrgID.Valid() {
				filter.OrgID = &tt.args.OrgID
			}
			if tt.args.organization != "" {
				filter.Org = &tt.args.organization
			}
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}

			checks, _, err := s.FindChecks(ctx, filter, tt.args.findOptions)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(checks, tt.wants.checks, checkCmpOptions...); diff != "" {
				t.Errorf("checks are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteCheck testing
func DeleteCheck(
	init checkServiceFactory,
	t *testing.T,
) {
	type args struct {
		ID     string
		userID influxdb.ID
	}
	type wants struct {
		err    *influxdb.Error
		checks []influxdb.Check
	}

	tests := []struct {
		name   string
		fields CheckFields
		args   args
		wants  wants
	}{
		{
			name: "delete checks using exist id",
			fields: CheckFields{
				IDGenerator: mock.NewIDGenerator("0000000000000001", t),
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Tasks: []influxdb.TaskCreate{
					{
						Flux: `option task = { every: 10s, name: "foo" }
data = from(bucket: "telegraf") |> range(start: -1m)`,
						OrganizationID: MustIDBase16(orgOneID),
						OwnerID:        MustIDBase16(sixID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
			},
			args: args{
				ID:     checkOneID,
				userID: MustIDBase16(sixID),
			},
			wants: wants{
				checks: []influxdb.Check{
					threshold1,
				},
			},
		},
		{
			name: "delete checks using id that does not exist",
			fields: CheckFields{
				IDGenerator: mock.NewIDGenerator("0000000000000001", t),
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Tasks: []influxdb.TaskCreate{
					{
						Flux: `option task = { every: 10s, name: "foo" }
		data = from(bucket: "telegraf") |> range(start: -1m)`,
						OrganizationID: MustIDBase16(orgOneID),
						OwnerID:        MustIDBase16(sixID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
			},
			args: args{
				ID:     "1234567890654321",
				userID: MustIDBase16(sixID),
			},
			wants: wants{
				err: &influxdb.Error{
					Op:   influxdb.OpDeleteCheck,
					Msg:  "check not found",
					Code: influxdb.ENotFound,
				},
				checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteCheck(ctx, MustIDBase16(tt.args.ID))
			influxErrsEqual(t, tt.wants.err, err)

			filter := influxdb.CheckFilter{}
			checks, _, err := s.FindChecks(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve checks: %v", err)
			}
			if diff := cmp.Diff(checks, tt.wants.checks, checkCmpOptions...); diff != "" {
				t.Errorf("checks are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindCheck testing
func FindCheck(
	init checkServiceFactory,
	t *testing.T,
) {
	type args struct {
		name  string
		OrgID influxdb.ID
	}

	type wants struct {
		check influxdb.Check
		err   *influxdb.Error
	}

	tests := []struct {
		name   string
		fields CheckFields
		args   args
		wants  wants
	}{
		{
			name: "find check by name",
			fields: CheckFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "theorg2",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
			},
			args: args{
				name:  "name1",
				OrgID: MustIDBase16(orgOneID),
			},
			wants: wants{
				check: deadman1,
			},
		},
		{
			name: "mixed filter",
			fields: CheckFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Checks: []influxdb.Check{},
			},
			args: args{
				name:  "name2",
				OrgID: MustIDBase16(orgOneID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpFindCheck,
					Msg:  "check not found",
				},
			},
		},
		{
			name: "missing check returns error",
			fields: CheckFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Checks: []influxdb.Check{},
			},
			args: args{
				name:  "xyz",
				OrgID: MustIDBase16(orgOneID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpFindCheck,
					Msg:  "check not found",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, _, done := init(tt.fields, t)
			defer done()

			var filter influxdb.CheckFilter
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}
			if tt.args.OrgID.Valid() {
				filter.OrgID = &tt.args.OrgID
			}

			check, err := s.FindCheck(context.Background(), filter)
			influxErrsEqual(t, tt.wants.err, err)

			if diff := cmp.Diff(check, tt.wants.check, checkCmpOptions...); diff != "" {
				t.Errorf("checks are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateCheck testing
func UpdateCheck(
	init checkServiceFactory,
	t *testing.T,
) {
	type args struct {
		id    influxdb.ID
		check influxdb.Check
	}
	type wants struct {
		err   error
		check influxdb.Check
	}

	tests := []struct {
		name   string
		fields CheckFields
		args   args
		wants  wants
	}{
		{
			name: "mixed update",
			fields: CheckFields{
				IDGenerator:   mock.NewIDGenerator("0000000000000001", t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2007, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Tasks: []influxdb.TaskCreate{
					{
						Flux: `option task = { every: 10s, name: "foo" }
data = from(bucket: "telegraf") |> range(start: -1m)`,
						OrganizationID: MustIDBase16(orgOneID),
						OwnerID:        MustIDBase16(sixID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
				},
			},
			args: args{
				id: MustIDBase16(checkOneID),
				check: &check.Threshold{
					Base: check.Base{
						ID:      MustIDBase16(checkTwoID),
						OrgID:   MustIDBase16(orgOneID),
						OwnerID: MustIDBase16(twoID),
						Every:   mustDuration("1m"),
						Query: influxdb.DashboardQuery{
							Text: script,
							BuilderConfig: influxdb.BuilderConfig{
								Tags: []struct {
									Key                   string   `json:"key"`
									Values                []string `json:"values"`
									AggregateFunctionType string   `json:"aggregateFunctionType"`
								}{
									{
										Key:                   "_field",
										Values:                []string{"usage_user"},
										AggregateFunctionType: "filter",
									},
								},
							},
						},
						Name:                  "changed",
						Description:           "desc changed",
						StatusMessageTemplate: "msg2",
						TaskID:                1,
						Tags: []influxdb.Tag{
							{Key: "k11", Value: "v11"},
							{Key: "k22", Value: "v22"},
							{Key: "k33", Value: "v33"},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2001, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2002, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
					Thresholds: []check.ThresholdConfig{
						&check.Lesser{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Ok,
							},
							Value: 1000,
						},
						&check.Greater{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Warn,
							},
							Value: 2000,
						},
						&check.Range{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Info,
							},
							Min:    1500,
							Max:    1900,
							Within: true,
						},
					},
				},
			},
			wants: wants{
				check: &check.Threshold{
					Base: check.Base{
						ID:      MustIDBase16(checkOneID),
						OrgID:   MustIDBase16(orgOneID),
						Name:    "changed",
						Every:   mustDuration("1m"),
						OwnerID: MustIDBase16(sixID),
						Query: influxdb.DashboardQuery{
							Text: script,
							BuilderConfig: influxdb.BuilderConfig{
								Tags: []struct {
									Key                   string   `json:"key"`
									Values                []string `json:"values"`
									AggregateFunctionType string   `json:"aggregateFunctionType"`
								}{
									{
										Key:                   "_field",
										Values:                []string{"usage_user"},
										AggregateFunctionType: "filter",
									},
								},
							},
						},
						Description:           "desc changed",
						StatusMessageTemplate: "msg2",
						Tags: []influxdb.Tag{
							{Key: "k11", Value: "v11"},
							{Key: "k22", Value: "v22"},
							{Key: "k33", Value: "v33"},
						},
						TaskID: 1,
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2007, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
					Thresholds: []check.ThresholdConfig{
						&check.Lesser{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Ok,
							},
							Value: 1000,
						},
						&check.Greater{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Warn,
							},
							Value: 2000,
						},
						&check.Range{
							ThresholdConfigBase: check.ThresholdConfigBase{
								Level: notification.Info,
							},
							Min:    1500,
							Max:    1900,
							Within: true,
						},
					},
				},
			},
		},
		{
			name: "update name unique",
			fields: CheckFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					&check.Deadman{
						Base: check.Base{
							ID:    MustIDBase16(checkTwoID),
							OrgID: MustIDBase16(orgOneID),
							Every: mustDuration("1m"),
							Query: influxdb.DashboardQuery{
								Text: script,
								BuilderConfig: influxdb.BuilderConfig{
									Tags: []struct {
										Key                   string   `json:"key"`
										Values                []string `json:"values"`
										AggregateFunctionType string   `json:"aggregateFunctionType"`
									}{
										{
											Key:                   "_field",
											Values:                []string{"usage_user"},
											AggregateFunctionType: "filter",
										},
									},
								},
							},
							TaskID:                1,
							Name:                  "check2",
							OwnerID:               MustIDBase16(twoID),
							StatusMessageTemplate: "msg1",
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(checkOneID),
				check: &check.Deadman{
					Base: check.Base{
						OrgID:                 MustIDBase16(orgOneID),
						OwnerID:               MustIDBase16(twoID),
						Name:                  "check2",
						Description:           "desc changed",
						TaskID:                1,
						Every:                 mustDuration("1m"),
						StatusMessageTemplate: "msg2",
						Tags: []influxdb.Tag{
							{Key: "k11", Value: "v11"},
							{Key: "k22", Value: "v22"},
							{Key: "k33", Value: "v33"},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2001, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2002, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
					TimeSince:  mustDuration("12s"),
					StaleTime:  mustDuration("1h"),
					ReportZero: false,
					Level:      notification.Warn,
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EConflict,
					Msg:  "check name is not unique",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			checkCreate := influxdb.CheckCreate{Check: tt.args.check, Status: influxdb.Active}

			check, err := s.UpdateCheck(ctx, tt.args.id, checkCreate)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(check, tt.wants.check, checkCmpOptions...); diff != "" {
				t.Errorf("check is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// PatchCheck testing
func PatchCheck(
	init checkServiceFactory,
	t *testing.T,
) {
	type args struct {
		id  influxdb.ID
		upd influxdb.CheckUpdate
	}
	type wants struct {
		err   *influxdb.Error
		check influxdb.Check
	}

	inactive := influxdb.Inactive

	tests := []struct {
		name   string
		fields CheckFields
		args   args
		wants  wants
	}{
		{
			name: "mixed patch",
			fields: CheckFields{
				IDGenerator:   mock.NewIDGenerator("0000000000000001", t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2007, 5, 4, 1, 2, 3, 0, time.UTC)},
				Tasks: []influxdb.TaskCreate{
					{
						Flux: `option task = { every: 10s, name: "foo" }
data = from(bucket: "telegraf") |> range(start: -1m)`,
						OrganizationID: MustIDBase16(orgOneID),
						OwnerID:        MustIDBase16(sixID),
					},
				},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
				},
			},
			args: args{
				id: MustIDBase16(checkOneID),
				upd: influxdb.CheckUpdate{
					Name:        strPtr("changed"),
					Description: strPtr("desc changed"),
					Status:      &inactive,
				},
			},
			wants: wants{
				check: &check.Deadman{
					Base: check.Base{
						ID:          MustIDBase16(checkOneID),
						OrgID:       MustIDBase16(orgOneID),
						Name:        "changed",
						OwnerID:     MustIDBase16(sixID),
						Every:       mustDuration("1m"),
						Description: "desc changed",
						Query: influxdb.DashboardQuery{
							Text: script,
							BuilderConfig: influxdb.BuilderConfig{
								Buckets: []string{},
								Tags: []struct {
									Key                   string   `json:"key"`
									Values                []string `json:"values"`
									AggregateFunctionType string   `json:"aggregateFunctionType"`
								}{
									{
										Key:                   "_field",
										Values:                []string{"usage_user"},
										AggregateFunctionType: "filter",
									},
								},
								Functions: []struct {
									Name string `json:"name"`
								}{},
							},
						},
						StatusMessageTemplate: "msg1",
						Tags: []influxdb.Tag{
							{Key: "k1", Value: "v1"},
							{Key: "k2", Value: "v2"},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2007, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
					TimeSince:  mustDuration("21s"),
					StaleTime:  mustDuration("1h"),
					ReportZero: true,
					Level:      notification.Critical,
				},
			},
		},
		{
			name: "update name unique",
			fields: CheckFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					&check.Deadman{
						Base: check.Base{
							ID:      MustIDBase16(checkTwoID),
							OrgID:   MustIDBase16(orgOneID),
							Every:   mustDuration("1m"),
							Name:    "check2",
							OwnerID: MustIDBase16(sixID),
							Query: influxdb.DashboardQuery{
								Text: script,
								BuilderConfig: influxdb.BuilderConfig{
									Tags: []struct {
										Key                   string   `json:"key"`
										Values                []string `json:"values"`
										AggregateFunctionType string   `json:"aggregateFunctionType"`
									}{
										{
											Key:                   "_field",
											Values:                []string{"usage_user"},
											AggregateFunctionType: "filter",
										},
									},
								},
							},
							StatusMessageTemplate: "msg1",
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(checkOneID),
				upd: influxdb.CheckUpdate{
					Name: strPtr("check2"),
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EConflict,
					Msg:  "check entity update conflicts with an existing entity",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			check, err := s.PatchCheck(ctx, tt.args.id, tt.args.upd)
			influxErrsEqual(t, tt.wants.err, err)

			if diff := cmp.Diff(check, tt.wants.check, checkCmpOptions...); diff != "" {
				t.Errorf("check is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// MustIDBase16 is an helper to ensure a correct ID is built during testing.
func MustIDBase16(s string) influxdb.ID {
	id, err := influxdb.IDFromString(s)
	if err != nil {
		panic(err)
	}
	return *id
}

func diffPlatformErrors(name string, actual, expected error, opPrefix string, t *testing.T) {
	t.Helper()
	ErrorsEqual(t, actual, expected)
}

// ErrorsEqual checks to see if the provided errors are equivalent.
func ErrorsEqual(t *testing.T, actual, expected error) {
	t.Helper()
	if expected == nil && actual == nil {
		return
	}

	if expected == nil && actual != nil {
		t.Errorf("unexpected error %s", actual.Error())
	}

	if expected != nil && actual == nil {
		t.Errorf("expected error %s but received nil", expected.Error())
	}

	if influxdb.ErrorCode(expected) != influxdb.ErrorCode(actual) {
		t.Logf("\nexpected: %v\nactual: %v\n\n", expected, actual)
		t.Errorf("expected error code %q but received %q", influxdb.ErrorCode(expected), influxdb.ErrorCode(actual))
	}

	if influxdb.ErrorMessage(expected) != influxdb.ErrorMessage(actual) {
		t.Logf("\nexpected: %v\nactual: %v\n\n", expected, actual)
		t.Errorf("expected error message %q but received %q", influxdb.ErrorMessage(expected), influxdb.ErrorMessage(actual))
	}
}

func influxErrsEqual(t *testing.T, expected *influxdb.Error, actual error) {
	t.Helper()

	if expected != nil {
		require.Error(t, actual)
	}

	if actual == nil {
		return
	}

	if expected == nil {
		require.NoError(t, actual)
		return
	}
	iErr, ok := actual.(*influxdb.Error)
	require.True(t, ok)
	assert.Equal(t, expected.Code, iErr.Code)
	assert.Truef(t, strings.HasPrefix(iErr.Error(), expected.Error()), "expected: %s got err: %s", expected.Error(), actual.Error())
}

func mustDuration(d string) *notification.Duration {
	dur, err := time.ParseDuration(d)
	if err != nil {
		panic(err)
	}

	ndur, err := notification.FromTimeDuration(dur)
	if err != nil {
		panic(err)
	}

	// Filter out the zero values from the duration.
	durs := make([]ast.Duration, 0, len(ndur.Values))
	for _, d := range ndur.Values {
		if d.Magnitude != 0 {
			durs = append(durs, d)
		}
	}
	ndur.Values = durs
	return &ndur
}
