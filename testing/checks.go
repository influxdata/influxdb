package testing

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/notification"
	"github.com/influxdata/influxdb/notification/check"
)

func mustDuration(d string) *check.Duration {
	dur, err := parser.ParseDuration(d)
	if err != nil {
		panic(err)
	}

	return (*check.Duration)(dur)
}

const (
	checkOneID = "020f755c3c082000"
	checkTwoID = "020f755c3c082001"
)

var script = `data = from(bucket: "telegraf") |> range(start: -1m)`

var deadman1 = &check.Deadman{
	Base: check.Base{
		Name:            "name1",
		ID:              MustIDBase16(checkOneID),
		OrgID:           MustIDBase16(orgOneID),
		AuthorizationID: MustIDBase16(twoID),
		Description:     "desc1",
		TaskID:          1,
		Status:          influxdb.Active,
		Query: influxdb.DashboardQuery{
			Text: script,
		},
		Every:                 mustDuration("1m"),
		StatusMessageTemplate: "msg1",
		Tags: []notification.Tag{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
		},
		CRUDLog: influxdb.CRUDLog{
			CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
			UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
		},
	},
	TimeSince:  21,
	ReportZero: true,
	Level:      notification.Critical,
}

var threshold1 = &check.Threshold{
	Base: check.Base{
		Name:                  "name2",
		ID:                    MustIDBase16(checkTwoID),
		OrgID:                 MustIDBase16(orgTwoID),
		AuthorizationID:       MustIDBase16(twoID),
		TaskID:                1,
		Description:           "desc2",
		Status:                influxdb.Active,
		StatusMessageTemplate: "msg2",
		Every:                 mustDuration("1m"),
		Query: influxdb.DashboardQuery{
			Text: script,
		},
		Tags: []notification.Tag{
			{Key: "k11", Value: "v11"},
		},
		CRUDLog: influxdb.CRUDLog{
			CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
			UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
		},
	},
	Thresholds: []check.ThresholdConfig{
		{Level: 0, LowerBound: FloatPtr(1000)},
		{Level: 1, UpperBound: FloatPtr(2000)},
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

// CheckFields will include the IDGenerator, and checks
type CheckFields struct {
	IDGenerator          influxdb.IDGenerator
	TimeGenerator        influxdb.TimeGenerator
	Checks               []influxdb.Check
	Organizations        []*influxdb.Organization
	UserResourceMappings []*influxdb.UserResourceMapping
	Authorizations       []*influxdb.Authorization
	Tasks                []influxdb.TaskCreate
}

type checkServiceF func(
	init func(CheckFields, *testing.T) (influxdb.CheckService, string, func()),
	t *testing.T,
)

// CheckService tests all the service functions.
func CheckService(
	init func(CheckFields, *testing.T) (influxdb.CheckService, string, func()),
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
	init func(CheckFields, *testing.T) (influxdb.CheckService, string, func()),
	t *testing.T,
) {
	type args struct {
		check influxdb.Check
	}
	type wants struct {
		err    error
		checks []influxdb.Check
		urms   []*influxdb.UserResourceMapping
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
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(orgOneID),
						ResourceType: influxdb.OrgsResourceType,
						UserID:       MustIDBase16(fiveID),
						UserType:     influxdb.Owner,
					},
					{
						ResourceID:   MustIDBase16(orgOneID),
						ResourceType: influxdb.OrgsResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:    MustIDBase16(twoID),
						Token: "abc123",
						OrgID: MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				check: &check.Deadman{
					Base: check.Base{
						Name:            "name1",
						OrgID:           MustIDBase16(orgOneID),
						AuthorizationID: MustIDBase16(twoID),
						Description:     "desc1",
						Query: influxdb.DashboardQuery{
							Text: script,
						},
						Every:                 mustDuration("1m"),
						Status:                influxdb.Active,
						StatusMessageTemplate: "msg1",
						Tags: []notification.Tag{
							{Key: "k1", Value: "v1"},
							{Key: "k2", Value: "v2"},
						},
					},
					TimeSince:  21,
					ReportZero: true,
					Level:      notification.Critical,
				},
			},
			wants: wants{
				urms: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(checkOneID),
						ResourceType: influxdb.ChecksResourceType,
						UserID:       MustIDBase16(fiveID),
						UserType:     influxdb.Owner,
					},
					{
						ResourceID:   MustIDBase16(checkOneID),
						ResourceType: influxdb.ChecksResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
				},
				checks: []influxdb.Check{
					&check.Deadman{
						Base: check.Base{
							Name:            "name1",
							ID:              MustIDBase16(checkOneID),
							OrgID:           MustIDBase16(orgOneID),
							AuthorizationID: MustIDBase16(twoID),
							Query: influxdb.DashboardQuery{
								Text: script,
							},
							Every:                 mustDuration("1m"),
							Description:           "desc1",
							Status:                influxdb.Active,
							StatusMessageTemplate: "msg1",
							Tags: []notification.Tag{
								{Key: "k1", Value: "v1"},
								{Key: "k2", Value: "v2"},
							},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
								UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							},
						},
						TimeSince:  21,
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
				Authorizations: []*influxdb.Authorization{
					{
						ID:    MustIDBase16(twoID),
						Token: "abc123",
						OrgID: MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				check: &check.Threshold{
					Base: check.Base{
						Name:                  "name2",
						OrgID:                 MustIDBase16(orgTwoID),
						AuthorizationID:       MustIDBase16(twoID),
						Description:           "desc2",
						Status:                influxdb.Active,
						StatusMessageTemplate: "msg2",
						Every:                 mustDuration("1m"),
						Query: influxdb.DashboardQuery{
							Text: script,
						},
						Tags: []notification.Tag{
							{Key: "k11", Value: "v11"},
						},
					},
					Thresholds: []check.ThresholdConfig{
						{Level: 0, LowerBound: FloatPtr(1000)},
						{Level: 1, UpperBound: FloatPtr(2000)},
					},
				},
			},
			wants: wants{
				checks: []influxdb.Check{
					deadman1,
					threshold1,
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
				Authorizations: []*influxdb.Authorization{
					{
						ID:    MustIDBase16(twoID),
						Token: "abc123",
						OrgID: MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				check: &check.Threshold{
					Base: check.Base{
						Name:            "name1",
						OrgID:           MustIDBase16(orgOneID),
						AuthorizationID: MustIDBase16(twoID),
						Description:     "desc1",
						Status:          influxdb.Active,
						Every:           mustDuration("1m"),
						Query: influxdb.DashboardQuery{
							Text: script,
						},
						StatusMessageTemplate: "msg1",
						Tags: []notification.Tag{
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
					Msg:  fmt.Sprintf("check with name name1 already exists"),
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
				Authorizations: []*influxdb.Authorization{
					{
						ID:    MustIDBase16(twoID),
						Token: "abc123",
						OrgID: MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				check: &check.Threshold{
					Base: check.Base{
						Name:            "name1",
						OrgID:           MustIDBase16(orgTwoID),
						AuthorizationID: MustIDBase16(twoID),
						Description:     "desc2",
						Every:           mustDuration("1m"),
						Query: influxdb.DashboardQuery{
							Text: script,
						},
						Status:                influxdb.Inactive,
						StatusMessageTemplate: "msg2",
						Tags: []notification.Tag{
							{Key: "k11", Value: "v11"},
							{Key: "k22", Value: "v22"},
						},
					},
				},
			},
			wants: wants{
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
							},
							AuthorizationID:       MustIDBase16(twoID),
							Description:           "desc2",
							Status:                influxdb.Inactive,
							StatusMessageTemplate: "msg2",
							Tags: []notification.Tag{
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
				Authorizations: []*influxdb.Authorization{
					{
						ID:    MustIDBase16(twoID),
						Token: "abc123",
						OrgID: MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				check: &check.Threshold{
					Base: check.Base{
						Name:            "name1",
						OrgID:           MustIDBase16(orgOneID),
						AuthorizationID: MustIDBase16(twoID),
						Every:           mustDuration("1m"),
						Query: influxdb.DashboardQuery{
							Text: script,
						},
						Description:           "desc2",
						Status:                influxdb.Inactive,
						StatusMessageTemplate: "msg2",
						Tags: []notification.Tag{
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
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.CreateCheck(ctx, tt.args.check)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			defer s.DeleteCheck(ctx, tt.args.check.GetID())

			checks, _, err := s.FindChecks(ctx, influxdb.CheckFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve checks: %v", err)
			}
			if diff := cmp.Diff(checks, tt.wants.checks, checkCmpOptions...); diff != "" {
				t.Errorf("checks are different -got/+want\ndiff %s", diff)
			}
			urmFilter := influxdb.UserResourceMappingFilter{
				ResourceType: influxdb.ChecksResourceType,
			}
			urms, _, err := s.FindUserResourceMappings(ctx, urmFilter)
			if err != nil {
				t.Fatalf("failed to retrieve user resource mappings: %v", err)
			}
			if diff := cmp.Diff(urms, tt.wants.urms, userResourceMappingCmpOptions...); diff != "" {
				t.Errorf("user resource mappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindCheckByID testing
func FindCheckByID(
	init func(CheckFields, *testing.T) (influxdb.CheckService, string, func()),
	t *testing.T,
) {
	type args struct {
		id influxdb.ID
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
			name: "basic find check by id",
			fields: CheckFields{
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:    MustIDBase16(twoID),
						Token: "abc123",
						OrgID: MustIDBase16(orgOneID),
					},
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
				Authorizations: []*influxdb.Authorization{
					{
						ID:    MustIDBase16(twoID),
						Token: "abc123",
						OrgID: MustIDBase16(orgOneID),
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
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			check, err := s.FindCheckByID(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(check, tt.wants.check, checkCmpOptions...); diff != "" {
				t.Errorf("check is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindChecks testing
func FindChecks(
	init func(CheckFields, *testing.T) (influxdb.CheckService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID           influxdb.ID
		name         string
		organization string
		OrgID        influxdb.ID
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
			args: args{},
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
				OrgID: MustIDBase16(orgTwoID),
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
				name: "name2",
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
				name: "xyz",
			},
			wants: wants{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
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
	init func(CheckFields, *testing.T) (influxdb.CheckService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID string
	}
	type wants struct {
		err    error
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
						Token:          "abc123",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:    MustIDBase16(twoID),
						Token: "abc123",
						OrgID: MustIDBase16(orgOneID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
			},
			args: args{
				ID: checkOneID,
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
						Token:          "abc123",
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					threshold1,
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:    MustIDBase16(twoID),
						Token: "abc123",
						OrgID: MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				ID: "1234567890654321",
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
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteCheck(ctx, MustIDBase16(tt.args.ID))
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

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
	init func(CheckFields, *testing.T) (influxdb.CheckService, string, func()),
	t *testing.T,
) {
	type args struct {
		name  string
		OrgID influxdb.ID
	}

	type wants struct {
		check influxdb.Check
		err   error
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
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			filter := influxdb.CheckFilter{}
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}
			if tt.args.OrgID.Valid() {
				filter.OrgID = &tt.args.OrgID
			}

			check, err := s.FindCheck(ctx, filter)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(check, tt.wants.check, checkCmpOptions...); diff != "" {
				t.Errorf("checks are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateCheck testing
func UpdateCheck(
	init func(CheckFields, *testing.T) (influxdb.CheckService, string, func()),
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

	inactive := influxdb.Inactive

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
				Checks: []influxdb.Check{
					deadman1,
				},
				Tasks: []influxdb.TaskCreate{
					{
						Flux: `option task = { every: 10s, name: "foo" }
data = from(bucket: "telegraf") |> range(start: -1m)`,
						OrganizationID: MustIDBase16(orgOneID),
						Token:          "abc123",
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:    MustIDBase16(twoID),
						Token: "abc123",
						OrgID: MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				id: MustIDBase16(checkOneID),
				check: &check.Deadman{
					Base: check.Base{
						ID:              MustIDBase16(checkTwoID),
						OrgID:           MustIDBase16(orgOneID),
						AuthorizationID: MustIDBase16(twoID),
						Every:           mustDuration("1m"),
						Query: influxdb.DashboardQuery{
							Text: script,
						},
						Name:                  "changed",
						Description:           "desc changed",
						Status:                inactive,
						StatusMessageTemplate: "msg2",
						TaskID:                1,
						Tags: []notification.Tag{
							{Key: "k11", Value: "v11"},
							{Key: "k22", Value: "v22"},
							{Key: "k33", Value: "v33"},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2001, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2002, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
					TimeSince:  12,
					ReportZero: false,
					Level:      notification.Warn,
				},
			},
			wants: wants{
				check: &check.Deadman{
					Base: check.Base{
						ID:              MustIDBase16(checkOneID),
						OrgID:           MustIDBase16(orgOneID),
						Name:            "changed",
						Every:           mustDuration("1m"),
						AuthorizationID: MustIDBase16(twoID),
						Query: influxdb.DashboardQuery{
							Text: script,
						},
						Description:           "desc changed",
						Status:                influxdb.Inactive,
						StatusMessageTemplate: "msg2",
						Tags: []notification.Tag{
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
					TimeSince:  12,
					ReportZero: false,
					Level:      notification.Warn,
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
							},
							TaskID:                1,
							Name:                  "check2",
							AuthorizationID:       MustIDBase16(twoID),
							Status:                influxdb.Inactive,
							StatusMessageTemplate: "msg1",
						},
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:    MustIDBase16(twoID),
						Token: "abc123",
						OrgID: MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				id: MustIDBase16(checkOneID),
				check: &check.Deadman{
					Base: check.Base{
						OrgID:                 MustIDBase16(orgOneID),
						AuthorizationID:       MustIDBase16(twoID),
						Name:                  "check2",
						Description:           "desc changed",
						Status:                inactive,
						TaskID:                1,
						Every:                 mustDuration("1m"),
						StatusMessageTemplate: "msg2",
						Tags: []notification.Tag{
							{Key: "k11", Value: "v11"},
							{Key: "k22", Value: "v22"},
							{Key: "k33", Value: "v33"},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2001, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2002, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
					TimeSince:  12,
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
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			check, err := s.UpdateCheck(ctx, tt.args.id, tt.args.check)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(check, tt.wants.check, checkCmpOptions...); diff != "" {
				t.Errorf("check is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// PatchCheck testing
func PatchCheck(
	init func(CheckFields, *testing.T) (influxdb.CheckService, string, func()),
	t *testing.T,
) {
	type args struct {
		id  influxdb.ID
		upd influxdb.CheckUpdate
	}
	type wants struct {
		err   error
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
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2007, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:    MustIDBase16(twoID),
						Token: "abc123",
						OrgID: MustIDBase16(orgOneID),
					},
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
						ID:              MustIDBase16(checkOneID),
						OrgID:           MustIDBase16(orgOneID),
						Name:            "changed",
						AuthorizationID: MustIDBase16(twoID),
						Every:           mustDuration("1m"),
						Description:     "desc changed",
						Status:          influxdb.Inactive,
						Query: influxdb.DashboardQuery{
							Text: script,
						},
						StatusMessageTemplate: "msg1",
						Tags: []notification.Tag{
							{Key: "k1", Value: "v1"},
							{Key: "k2", Value: "v2"},
						},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2007, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
					TimeSince:  21,
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
				Authorizations: []*influxdb.Authorization{
					{
						ID:    MustIDBase16(twoID),
						Token: "abc123",
						OrgID: MustIDBase16(orgOneID),
					},
				},
				Checks: []influxdb.Check{
					deadman1,
					&check.Deadman{
						Base: check.Base{
							ID:              MustIDBase16(checkTwoID),
							OrgID:           MustIDBase16(orgOneID),
							Every:           mustDuration("1m"),
							Name:            "check2",
							AuthorizationID: MustIDBase16(twoID),
							Status:          influxdb.Inactive,
							Query: influxdb.DashboardQuery{
								Text: script,
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
					Msg:  "check name is not unique",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			check, err := s.PatchCheck(ctx, tt.args.id, tt.args.upd)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(check, tt.wants.check, checkCmpOptions...); diff != "" {
				t.Errorf("check is different -got/+want\ndiff %s", diff)
			}
		})
	}
}
