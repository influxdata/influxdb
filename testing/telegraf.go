package testing

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/telegraf/plugins/inputs"
	"github.com/influxdata/influxdb/v2/telegraf/plugins/outputs"
)

// TelegrafConfigFields includes prepopulated data for mapping tests.
type TelegrafConfigFields struct {
	IDGenerator          platform.IDGenerator
	TelegrafConfigs      []*platform.TelegrafConfig
	UserResourceMappings []*platform.UserResourceMapping
}

var telegrafCmpOptions = cmp.Options{
	cmpopts.IgnoreUnexported(
		inputs.CPUStats{},
		inputs.MemStats{},
		inputs.Kubernetes{},
		inputs.File{},
		outputs.File{},
		outputs.InfluxDBV2{},
	),
	cmp.Transformer("Sort", func(in []*platform.TelegrafConfig) []*platform.TelegrafConfig {
		out := append([]*platform.TelegrafConfig(nil), in...)
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID > out[j].ID
		})
		return out
	}),
}

// TelegrafConfigStore tests all the service functions.
func TelegrafConfigStore(
	init func(TelegrafConfigFields, *testing.T) (platform.TelegrafConfigStore, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(TelegrafConfigFields, *testing.T) (platform.TelegrafConfigStore, func()),
			t *testing.T)
	}{
		{
			name: "CreateTelegrafConfig",
			fn:   CreateTelegrafConfig,
		},
		{
			name: "FindTelegrafConfigByID",
			fn:   FindTelegrafConfigByID,
		},
		{
			name: "FindTelegrafConfigs",
			fn:   FindTelegrafConfigs,
		},
		{
			name: "UpdateTelegrafConfig",
			fn:   UpdateTelegrafConfig,
		},
		{
			name: "DeleteTelegrafConfig",
			fn:   DeleteTelegrafConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt := tt
			t.Parallel()
			tt.fn(init, t)
		})
	}
}

// CreateTelegrafConfig testing.
func CreateTelegrafConfig(
	init func(TelegrafConfigFields, *testing.T) (platform.TelegrafConfigStore, func()),
	t *testing.T,
) {
	type args struct {
		telegrafConfig *platform.TelegrafConfig
		userID         platform.ID
	}
	type wants struct {
		err                 error
		telegrafs           []*platform.TelegrafConfig
		userResourceMapping []*platform.UserResourceMapping
	}

	tests := []struct {
		name   string
		fields TelegrafConfigFields
		args   args
		wants  wants
	}{
		{
			name: "create telegraf config without organization ID should error",
			fields: TelegrafConfigFields{
				IDGenerator:          mock.NewIDGenerator(oneID, t),
				TelegrafConfigs:      []*platform.TelegrafConfig{},
				UserResourceMappings: []*platform.UserResourceMapping{},
			},
			args: args{
				telegrafConfig: &platform.TelegrafConfig{},
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.EEmptyValue,
					Msg:  platform.ErrTelegrafConfigInvalidOrgID,
				},
			},
		},
		{
			name: "create telegraf config with empty set",
			fields: TelegrafConfigFields{
				IDGenerator:          mock.NewIDGenerator(oneID, t),
				TelegrafConfigs:      []*platform.TelegrafConfig{},
				UserResourceMappings: []*platform.UserResourceMapping{},
			},
			args: args{
				userID: MustIDBase16(threeID),
				telegrafConfig: &platform.TelegrafConfig{
					OrgID:    MustIDBase16(twoID),
					Name:     "name1",
					Config:   "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
					Metadata: map[string]interface{}{"buckets": []interface{}{}},
				},
			},
			wants: wants{
				userResourceMapping: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
				},
				telegrafs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(twoID),
						Name:     "name1",
						Config:   "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
		},
		{
			name: "basic create telegraf config",
			fields: TelegrafConfigFields{
				IDGenerator: mock.NewIDGenerator(twoID, t),
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(twoID),
						Name:     "tc1",
						Config:   "[[inputs.mem_stats]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
				},
			},
			args: args{
				userID: MustIDBase16(threeID),
				telegrafConfig: &platform.TelegrafConfig{
					OrgID:    MustIDBase16(twoID),
					Name:     "name2",
					Config:   "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
					Metadata: map[string]interface{}{"buckets": []interface{}{}}, // for inmem test as it doesn't unmarshal..
				},
			},
			wants: wants{
				telegrafs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(twoID),
						Name:     "tc1",
						Config:   "[[inputs.mem_stats]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(twoID),
						Name:     "name2",
						Config:   "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
				userResourceMapping: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.CreateTelegrafConfig(ctx, tt.args.telegrafConfig, tt.args.userID)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}
			if tt.wants.err == nil && !tt.args.telegrafConfig.ID.Valid() {
				t.Fatalf("telegraf config ID not set from CreateTelegrafConfig")
			}

			if err != nil && tt.wants.err != nil {
				if platform.ErrorCode(err) != platform.ErrorCode(tt.wants.err) {
					t.Fatalf("expected error messages to match '%v' got '%v'", platform.ErrorCode(tt.wants.err), platform.ErrorCode(err))
				}
			}

			filter := platform.TelegrafConfigFilter{
				UserResourceMappingFilter: platform.UserResourceMappingFilter{
					UserID:       MustIDBase16(threeID),
					ResourceType: platform.TelegrafsResourceType,
				},
			}
			tcs, _, err := s.FindTelegrafConfigs(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve telegraf configs: %v", err)
			}
			if diff := cmp.Diff(tcs, tt.wants.telegrafs, telegrafCmpOptions...); diff != "" {
				t.Errorf("telegraf configs are different -got/+want\ndiff %s", diff)
			}

			urms, _, err := s.FindUserResourceMappings(ctx, platform.UserResourceMappingFilter{
				UserID:       tt.args.userID,
				ResourceType: platform.TelegrafsResourceType,
			})
			if err != nil {
				t.Fatalf("failed to retrieve user resource mappings: %v", err)
			}
			if diff := cmp.Diff(urms, tt.wants.userResourceMapping, userResourceMappingCmpOptions...); diff != "" {
				t.Errorf("user resource mappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindTelegrafConfigByID testing.
func FindTelegrafConfigByID(
	init func(TelegrafConfigFields, *testing.T) (platform.TelegrafConfigStore, func()),
	t *testing.T,
) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err            error
		telegrafConfig *platform.TelegrafConfig
	}

	tests := []struct {
		name   string
		fields TelegrafConfigFields
		args   args
		wants  wants
	}{
		{
			name: "bad id",
			fields: TelegrafConfigFields{
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(twoID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(twoID),
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				id: platform.ID(0),
			},
			wants: wants{
				err: fmt.Errorf("provided telegraf configuration ID has invalid format"),
			},
		},
		{
			name: "not found",
			fields: TelegrafConfigFields{
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:     MustIDBase16(oneID),
						OrgID:  MustIDBase16(twoID),
						Name:   "tc1",
						Config: "[[inputs.cpu]]\n",
					},
					{
						ID:     MustIDBase16(twoID),
						OrgID:  MustIDBase16(twoID),
						Name:   "tc2",
						Config: "[[inputs.file]]\n[[inputs.mem]]\n",
					},
				},
			},
			args: args{
				id: MustIDBase16(threeID),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Msg:  "telegraf configuration not found",
				},
			},
		},
		{
			name: "basic find telegraf config by id",
			fields: TelegrafConfigFields{
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(threeID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(threeID),
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				id: MustIDBase16(twoID),
			},
			wants: wants{
				telegrafConfig: &platform.TelegrafConfig{
					ID:       MustIDBase16(twoID),
					OrgID:    MustIDBase16(threeID),
					Name:     "tc2",
					Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
					Metadata: map[string]interface{}{"buckets": []interface{}{}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			tc, err := s.FindTelegrafConfigByID(ctx, tt.args.id)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if want, got := tt.wants.err.Error(), err.Error(); want != got {
					t.Fatalf("expected error '%s' got '%s'", want, got)
				}
			}
			if diff := cmp.Diff(tc, tt.wants.telegrafConfig, telegrafCmpOptions...); diff != "" {
				t.Errorf("telegraf configs are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindTelegrafConfigs testing
func FindTelegrafConfigs(
	init func(TelegrafConfigFields, *testing.T) (platform.TelegrafConfigStore, func()),
	t *testing.T,
) {
	type args struct {
		filter platform.TelegrafConfigFilter
	}

	type wants struct {
		telegrafConfigs []*platform.TelegrafConfig
		err             error
	}
	tests := []struct {
		name   string
		fields TelegrafConfigFields
		args   args
		wants  wants
	}{
		{
			name: "find nothing (empty set)",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{},
				TelegrafConfigs:      []*platform.TelegrafConfig{},
			},
			args: args{
				filter: platform.TelegrafConfigFilter{
					UserResourceMappingFilter: platform.UserResourceMappingFilter{
						ResourceType: platform.TelegrafsResourceType,
					},
				},
			},
			wants: wants{
				telegrafConfigs: []*platform.TelegrafConfig{},
			},
		},
		{
			name: "find all telegraf configs",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(threeID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(threeID),
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				filter: platform.TelegrafConfigFilter{
					UserResourceMappingFilter: platform.UserResourceMappingFilter{
						UserID:       MustIDBase16(threeID),
						ResourceType: platform.TelegrafsResourceType,
					},
				},
			},
			wants: wants{
				telegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(threeID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(threeID),
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
		},
		{
			name: "find owners only",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(fourID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(fourID),
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				filter: platform.TelegrafConfigFilter{
					UserResourceMappingFilter: platform.UserResourceMappingFilter{
						UserID:       MustIDBase16(threeID),
						ResourceType: platform.TelegrafsResourceType,
						UserType:     platform.Owner,
					},
				},
			},
			wants: wants{
				telegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(fourID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
		},
		{
			name: "filter by organization only",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
					{
						ResourceID:   MustIDBase16(fourID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(fourID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(fourID),
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(fourID),
						OrgID:    MustIDBase16(oneID),
						Name:     "tc3",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				filter: platform.TelegrafConfigFilter{
					OrgID: idPtr(MustIDBase16(oneID)),
				},
			},
			wants: wants{
				telegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(fourID),
						OrgID:    MustIDBase16(oneID),
						Name:     "tc3",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
		},
		{
			name: "find owners and restrict by organization",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
					{
						ResourceID:   MustIDBase16(fourID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(fourID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(fourID),
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(fourID),
						OrgID:    MustIDBase16(oneID),
						Name:     "tc3",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				filter: platform.TelegrafConfigFilter{
					OrgID: idPtr(MustIDBase16(oneID)),
					UserResourceMappingFilter: platform.UserResourceMappingFilter{
						UserID:       MustIDBase16(threeID),
						ResourceType: platform.TelegrafsResourceType,
						UserType:     platform.Owner,
					},
				},
			},
			wants: wants{
				telegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(fourID),
						OrgID:    MustIDBase16(oneID),
						Name:     "tc3",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
		},
		{
			name: "look for organization not bound to any telegraf config",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:     MustIDBase16(oneID),
						OrgID:  MustIDBase16(threeID),
						Name:   "tc1",
						Config: "[[inputs.cpu]]\n",
					},
					{
						ID:     MustIDBase16(twoID),
						OrgID:  MustIDBase16(threeID),
						Name:   "tc2",
						Config: "[[inputs.file]]\n[[inputs.mem]]\n",
					},
				},
			},
			args: args{
				filter: platform.TelegrafConfigFilter{
					OrgID: idPtr(MustIDBase16(oneID)),
				},
			},
			wants: wants{
				telegrafConfigs: []*platform.TelegrafConfig{},
			},
		},
		{
			name: "find nothing",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: platform.TelegrafsResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:     MustIDBase16(oneID),
						OrgID:  MustIDBase16(threeID),
						Name:   "tc1",
						Config: "[[inputs.cpu]]\n",
					},
					{
						ID:     MustIDBase16(twoID),
						OrgID:  MustIDBase16(threeID),
						Name:   "tc2",
						Config: "[[inputs.file]]\n[[inputs.mem]]\n",
					},
				},
			},
			args: args{
				filter: platform.TelegrafConfigFilter{
					UserResourceMappingFilter: platform.UserResourceMappingFilter{
						UserID:       MustIDBase16(fourID),
						ResourceType: platform.TelegrafsResourceType,
					},
				},
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			tcs, n, err := s.FindTelegrafConfigs(ctx, tt.args.filter)
			if err != nil && tt.wants.err == nil {
				t.Fatalf("expected errors to be nil got '%v'", err)
			}

			if err != nil && tt.wants.err != nil {
				if platform.ErrorCode(err) != platform.ErrorCode(tt.wants.err) {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}
			if n != len(tt.wants.telegrafConfigs) {
				t.Fatalf("telegraf configs length is different got %d, want %d", n, len(tt.wants.telegrafConfigs))
			}

			if diff := cmp.Diff(tcs, tt.wants.telegrafConfigs, telegrafCmpOptions...); diff != "" {
				t.Errorf("telegraf configs are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateTelegrafConfig testing.
func UpdateTelegrafConfig(
	init func(TelegrafConfigFields, *testing.T) (platform.TelegrafConfigStore, func()),
	t *testing.T,
) {
	type args struct {
		userID         platform.ID
		id             platform.ID
		telegrafConfig *platform.TelegrafConfig
	}

	type wants struct {
		telegrafConfig *platform.TelegrafConfig
		err            error
	}
	tests := []struct {
		name   string
		fields TelegrafConfigFields
		args   args
		wants  wants
	}{
		{
			name: "can't find the id",
			fields: TelegrafConfigFields{
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:     MustIDBase16(oneID),
						OrgID:  MustIDBase16(fourID),
						Name:   "tc1",
						Config: "[[inputs.cpu]]\n",
					},
					{
						ID:     MustIDBase16(twoID),
						OrgID:  MustIDBase16(fourID),
						Name:   "tc2",
						Config: "[[inputs.file]]\n[[inputs.mem]]\n",
					},
				},
			},
			args: args{
				userID: MustIDBase16(threeID),
				id:     MustIDBase16(fourID),
				telegrafConfig: &platform.TelegrafConfig{
					Name:   "tc2",
					Config: "[[inputs.file]]\n[[inputs.mem]]\n",
				},
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Msg:  fmt.Sprintf("telegraf config with ID %v not found", MustIDBase16(fourID)),
				},
			},
		},
		{
			fields: TelegrafConfigFields{
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(fourID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(fourID),
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				userID: MustIDBase16(fourID),
				id:     MustIDBase16(twoID),
				telegrafConfig: &platform.TelegrafConfig{
					OrgID:  MustIDBase16(oneID), // notice this get ignored - ie., resulting TelegrafConfig will have OrgID equal to fourID
					Name:   "tc2",
					Config: "[[inputs.file]]\n[[inputs.mem]]\n",
				},
			},
			wants: wants{
				telegrafConfig: &platform.TelegrafConfig{
					ID:     MustIDBase16(twoID),
					OrgID:  MustIDBase16(fourID),
					Name:   "tc2",
					Config: "[[inputs.file]]\n[[inputs.mem]]\n",
				},
			},
		},
		{
			name: "config update",
			fields: TelegrafConfigFields{
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:     MustIDBase16(oneID),
						OrgID:  MustIDBase16(oneID),
						Name:   "tc1",
						Config: "[[inputs.cpu]]\n",
					},
					{
						ID:     MustIDBase16(twoID),
						OrgID:  MustIDBase16(oneID),
						Name:   "tc2",
						Config: "[[inputs.file]]\n[[inputs.kubernetes]]\n[[inputs.kubernetes]]\n",
					},
				},
			},
			args: args{
				userID: MustIDBase16(fourID),
				id:     MustIDBase16(twoID),
				telegrafConfig: &platform.TelegrafConfig{
					Name:   "tc2",
					Config: "[[inputs.file]]\n[[inputs.kubernetes]]\n[[inputs.kubernetes]]\n",
				},
			},
			wants: wants{
				telegrafConfig: &platform.TelegrafConfig{
					ID:     MustIDBase16(twoID),
					OrgID:  MustIDBase16(oneID),
					Name:   "tc2",
					Config: "[[inputs.file]]\n[[inputs.kubernetes]]\n[[inputs.kubernetes]]\n",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			tc, err := s.UpdateTelegrafConfig(ctx, tt.args.id,
				tt.args.telegrafConfig, tt.args.userID)
			if err != nil && tt.wants.err == nil {
				t.Fatalf("expected errors to be nil got '%v'", err)
			}
			if err != nil && tt.wants.err != nil {
				if platform.ErrorCode(err) != platform.ErrorCode(tt.wants.err) {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}
			if diff := cmp.Diff(tc, tt.wants.telegrafConfig, telegrafCmpOptions...); tt.wants.err == nil && diff != "" {
				fmt.Println(tc.Metadata, tt.wants.telegrafConfig.Metadata)
				t.Errorf("telegraf configs are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteTelegrafConfig testing.
func DeleteTelegrafConfig(
	init func(TelegrafConfigFields, *testing.T) (platform.TelegrafConfigStore, func()),
	t *testing.T,
) {
	type args struct {
		id     platform.ID
		userID platform.ID
	}

	type wants struct {
		telegrafConfigs      []*platform.TelegrafConfig
		userResourceMappings []*platform.UserResourceMapping
		err                  error
	}
	tests := []struct {
		name   string
		fields TelegrafConfigFields
		args   args
		wants  wants
	}{
		{
			name: "bad id",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
						ResourceType: platform.TelegrafsResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
						ResourceType: platform.TelegrafsResourceType,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(fourID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(fourID),
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				id:     platform.ID(0),
				userID: MustIDBase16(threeID),
			},
			wants: wants{
				err: fmt.Errorf("provided telegraf configuration ID has invalid format"),
				userResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
						ResourceType: platform.TelegrafsResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
						ResourceType: platform.TelegrafsResourceType,
					},
				},
				telegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(fourID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(fourID),
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
		},
		{
			name: "none existing config",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
						ResourceType: platform.TelegrafsResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
						ResourceType: platform.TelegrafsResourceType,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(threeID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(threeID),
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				id:     MustIDBase16(fourID),
				userID: MustIDBase16(threeID),
			},
			wants: wants{
				err: fmt.Errorf("telegraf configuration not found"),
				userResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
						ResourceType: platform.TelegrafsResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
						ResourceType: platform.TelegrafsResourceType,
					},
				},
				telegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(threeID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(threeID),
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
		},
		{
			name: "regular delete",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
						ResourceType: platform.TelegrafsResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
						ResourceType: platform.TelegrafsResourceType,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(twoID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       MustIDBase16(twoID),
						OrgID:    MustIDBase16(twoID),
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				id:     MustIDBase16(twoID),
				userID: MustIDBase16(threeID),
			},
			wants: wants{
				userResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
						ResourceType: platform.TelegrafsResourceType,
					},
				},
				telegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:       MustIDBase16(oneID),
						OrgID:    MustIDBase16(twoID),
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteTelegrafConfig(ctx, tt.args.id)
			if err != nil && tt.wants.err == nil {
				t.Fatalf("expected errors to be nil got '%v'", err)
			}

			if err != nil && tt.wants.err != nil {
				if want, got := tt.wants.err.Error(), err.Error(); want != got {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			filter := platform.TelegrafConfigFilter{
				UserResourceMappingFilter: platform.UserResourceMappingFilter{
					UserID:       tt.args.userID,
					ResourceType: platform.TelegrafsResourceType,
				},
			}
			tcs, n, err := s.FindTelegrafConfigs(ctx, filter)
			if err != nil && tt.wants.err == nil {
				t.Fatalf("expected errors to be nil got '%v'", err)
			}

			if err != nil && tt.wants.err != nil {
				if want, got := tt.wants.err.Error(), err.Error(); want != got {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if n != len(tt.wants.telegrafConfigs) {
				t.Fatalf("telegraf configs length is different got %d, want %d", n, len(tt.wants.telegrafConfigs))
			}
			if diff := cmp.Diff(tcs, tt.wants.telegrafConfigs, telegrafCmpOptions...); diff != "" {
				t.Errorf("telegraf configs are different -got/+want\ndiff %s", diff)
			}

			urms, _, err := s.FindUserResourceMappings(ctx, platform.UserResourceMappingFilter{
				UserID:       tt.args.userID,
				ResourceType: platform.TelegrafsResourceType,
			})
			if err != nil {
				t.Fatalf("failed to retrieve user resource mappings: %v", err)
			}
			if diff := cmp.Diff(urms, tt.wants.userResourceMappings, userResourceMappingCmpOptions...); diff != "" {
				t.Errorf("user resource mappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
