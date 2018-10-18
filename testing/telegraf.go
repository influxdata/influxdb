package testing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
	"github.com/influxdata/platform/telegraf/plugins/inputs"
	"github.com/influxdata/platform/telegraf/plugins/outputs"
)

// TelegrafConfigFields includes prepopulated data for mapping tests.
type TelegrafConfigFields struct {
	IDGenerator          platform.IDGenerator
	TelegrafConfigs      []*platform.TelegrafConfig
	UserResourceMappings []*platform.UserResourceMapping
}

var telegrafCmpOptions = cmp.Options{
	cmp.Comparer(func(a, b *platform.TelegrafConfig) bool {
		x, _ := json.Marshal(a)
		y, _ := json.Marshal(b)
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.TelegrafConfig) []*platform.TelegrafConfig {
		out := append([]*platform.TelegrafConfig(nil), in...)
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID > out[j].ID
		})
		return out
	}),
}

var userResourceMappingCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.UserResourceMapping) []*platform.UserResourceMapping {
		out := append([]*platform.UserResourceMapping(nil), in...)
		sort.Slice(out, func(i, j int) bool {
			return out[i].ResourceID.String() > out[j].ResourceID.String()
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
			name: "FindTelegrafConfig",
			fn:   FindTelegrafConfig,
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
		now            time.Time
		userID         platform.ID
	}
	type wants struct {
		err                 error
		telegrafs           []*platform.TelegrafConfig
		userResourceMapping []*platform.UserResourceMapping
	}

	now := time.Now().Round(0)

	tests := []struct {
		name   string
		fields TelegrafConfigFields
		args   args
		wants  wants
	}{
		{
			name: "create telegraf config with empty set",
			fields: TelegrafConfigFields{
				IDGenerator:          &loopIDGenerator{s: []string{oneID, twoID}},
				TelegrafConfigs:      []*platform.TelegrafConfig{},
				UserResourceMappings: []*platform.UserResourceMapping{},
			},
			args: args{
				now:    now,
				userID: MustIDBase16(threeID),
				telegrafConfig: &platform.TelegrafConfig{
					Name: "name1",
					Agent: platform.TelegrafAgentConfig{
						Interval: 1000,
					},
					Plugins: []platform.TelegrafPlugin{
						{
							Comment: "comment1",
							Config:  &inputs.CPUStats{},
						},
						{
							Comment: "comment2",
							Config: &outputs.InfluxDBV2{
								URLs:         []string{"localhost/9999"},
								Token:        "token1",
								Organization: "org1",
								Bucket:       "bucket1",
							},
						},
					},
				},
			},
			wants: wants{
				userResourceMapping: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
				},
				telegrafs: []*platform.TelegrafConfig{
					{
						ID:   MustIDBase16(oneID),
						Name: "name1",
						Agent: platform.TelegrafAgentConfig{
							Interval: 1000,
						},
						Created:   now,
						LastModBy: MustIDBase16(threeID),
						LastMod:   now,
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config:  &inputs.CPUStats{},
							},
							{
								Comment: "comment2",
								Config: &outputs.InfluxDBV2{
									URLs:         []string{"localhost/9999"},
									Token:        "token1",
									Organization: "org1",
									Bucket:       "bucket1",
								},
							},
						},
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
						ID:   MustIDBase16(oneID),
						Name: "tc1",
						Agent: platform.TelegrafAgentConfig{
							Interval: 4000,
						},
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config:  &inputs.MemStats{},
							},
						},
					},
				},
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
				},
			},
			args: args{
				now:    now,
				userID: MustIDBase16(threeID),
				telegrafConfig: &platform.TelegrafConfig{
					Name: "name2",
					Agent: platform.TelegrafAgentConfig{
						Interval: 1001,
					},
					Plugins: []platform.TelegrafPlugin{
						{
							Comment: "comment2",
							Config:  &inputs.CPUStats{},
						},
						{
							Comment: "comment3",
							Config: &outputs.InfluxDBV2{
								URLs:         []string{"localhost/9999"},
								Token:        "token3",
								Organization: "org3",
								Bucket:       "bucket3",
							},
						},
					},
				},
			},
			wants: wants{
				telegrafs: []*platform.TelegrafConfig{
					{
						ID:   MustIDBase16(oneID),
						Name: "tc1",
						Agent: platform.TelegrafAgentConfig{
							Interval: 4000,
						},
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config:  &inputs.MemStats{},
							},
						},
					},
					{
						ID:   MustIDBase16(twoID),
						Name: "name2",
						Agent: platform.TelegrafAgentConfig{
							Interval: 1001,
						},
						Created:   now,
						LastMod:   now,
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment2",
								Config:  &inputs.CPUStats{},
							},
							{
								Comment: "comment3",
								Config: &outputs.InfluxDBV2{
									URLs:         []string{"localhost/9999"},
									Token:        "token3",
									Organization: "org3",
									Bucket:       "bucket3",
								},
							},
						},
					},
				},
				userResourceMapping: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: platform.TelegrafResourceType,
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
			err := s.CreateTelegrafConfig(ctx, tt.args.telegrafConfig, tt.args.userID, tt.args.now)
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

			tcs, _, err := s.FindTelegrafConfigs(ctx, platform.UserResourceMappingFilter{
				UserID:       MustIDBase16(threeID),
				ResourceType: platform.TelegrafResourceType,
			})
			if err != nil {
				t.Fatalf("failed to retrieve telegraf configs: %v", err)
			}
			if diff := cmp.Diff(tcs, tt.wants.telegrafs, telegrafCmpOptions...); diff != "" {
				t.Errorf("telegraf configs are different -got/+want\ndiff %s", diff)
			}

			urms, _, err := s.FindUserResourceMappings(ctx, platform.UserResourceMappingFilter{
				UserID:       tt.args.userID,
				ResourceType: platform.TelegrafResourceType,
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
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
					},
				},
			},
			args: args{
				id: platform.ID(0),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.EEmptyValue,
					Err:  platform.ErrInvalidID,
				},
			},
		},
		{
			name: "not found",
			fields: TelegrafConfigFields{
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(threeID),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Msg:  fmt.Sprintf("telegraf config with ID %v not found", MustIDBase16(threeID)),
				},
			},
		},
		{
			name: "basic find telegraf config by id",
			fields: TelegrafConfigFields{
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(twoID),
			},
			wants: wants{
				telegrafConfig: &platform.TelegrafConfig{
					ID:        MustIDBase16(twoID),
					Name:      "tc2",
					LastModBy: MustIDBase16(threeID),
					Plugins: []platform.TelegrafPlugin{
						{
							Comment: "comment1",
							Config: &inputs.File{
								Files: []string{"f1", "f2"},
							},
						},
						{
							Comment: "comment2",
							Config:  &inputs.MemStats{},
						},
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

			tc, err := s.FindTelegrafConfigByID(ctx, tt.args.id)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if platform.ErrorCode(err) != platform.ErrorCode(tt.wants.err) {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}
			if diff := cmp.Diff(tc, tt.wants.telegrafConfig, telegrafCmpOptions...); diff != "" {
				t.Errorf("telegraf configs are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindTelegrafConfig testing
func FindTelegrafConfig(
	init func(TelegrafConfigFields, *testing.T) (platform.TelegrafConfigStore, func()),
	t *testing.T,
) {
	type args struct {
		filter platform.UserResourceMappingFilter
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
			name: "find telegraf config",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: platform.TelegrafResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
					},
				},
			},
			args: args{
				filter: platform.UserResourceMappingFilter{
					UserID:       MustIDBase16(threeID),
					ResourceType: platform.TelegrafResourceType,
					UserType:     platform.Member,
				},
			},
			wants: wants{
				telegrafConfig: &platform.TelegrafConfig{

					ID:        MustIDBase16(twoID),
					Name:      "tc2",
					LastModBy: MustIDBase16(threeID),
					Plugins: []platform.TelegrafPlugin{
						{
							Comment: "comment1",
							Config: &inputs.File{
								Files: []string{"f1", "f2"},
							},
						},
						{
							Comment: "comment2",
							Config:  &inputs.MemStats{},
						},
					},
				},
			},
		},
		{
			name: "find nothing",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: platform.TelegrafResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
					},
				},
			},
			args: args{
				filter: platform.UserResourceMappingFilter{
					UserID:       MustIDBase16(fourID),
					ResourceType: platform.TelegrafResourceType,
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

			tc, err := s.FindTelegrafConfig(ctx, tt.args.filter)
			if err != nil && tt.wants.err == nil {
				t.Fatalf("expected errors to be nil got '%v'", err)
			}

			if err != nil && tt.wants.err != nil {
				if platform.ErrorCode(err) != platform.ErrorCode(tt.wants.err) {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
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
		filter platform.UserResourceMappingFilter
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
			name: "find all telegraf configs",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: platform.TelegrafResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
					},
				},
			},
			args: args{
				filter: platform.UserResourceMappingFilter{
					UserID:       MustIDBase16(threeID),
					ResourceType: platform.TelegrafResourceType,
				},
			},
			wants: wants{
				telegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
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
						ResourceType: platform.TelegrafResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: platform.TelegrafResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
					},
				},
			},
			args: args{
				filter: platform.UserResourceMappingFilter{
					UserID:       MustIDBase16(threeID),
					ResourceType: platform.TelegrafResourceType,
					UserType:     platform.Owner,
				},
			},
			wants: wants{
				telegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
				},
			},
		},
		{
			name: "find nothing",
			fields: TelegrafConfigFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: platform.TelegrafResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: platform.TelegrafResourceType,
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
					},
				},
			},
			args: args{
				filter: platform.UserResourceMappingFilter{
					UserID:       MustIDBase16(fourID),
					ResourceType: platform.TelegrafResourceType,
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
	now := time.Now().Round(0)
	type args struct {
		userID         platform.ID
		now            time.Time
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
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
					},
				},
			},
			args: args{
				userID: MustIDBase16(threeID),
				now:    now,
				id:     MustIDBase16(fourID),
				telegrafConfig: &platform.TelegrafConfig{
					Name:      "tc2",
					LastModBy: MustIDBase16(threeID),
					Plugins: []platform.TelegrafPlugin{
						{
							Comment: "comment1",
							Config: &inputs.File{
								Files: []string{"f1", "f2"},
							},
						},
						{
							Comment: "comment2",
							Config:  &inputs.MemStats{},
						},
					},
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
			name: "regular update",
			fields: TelegrafConfigFields{
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
					},
				},
			},
			args: args{
				userID: MustIDBase16(fourID),
				now:    now,
				id:     MustIDBase16(twoID),
				telegrafConfig: &platform.TelegrafConfig{
					Name:      "tc2",
					LastModBy: MustIDBase16(threeID),
					Plugins: []platform.TelegrafPlugin{
						{
							Comment: "comment3",
							Config:  &inputs.CPUStats{},
						},
						{
							Comment: "comment2",
							Config:  &inputs.MemStats{},
						},
					},
				},
			},
			wants: wants{
				telegrafConfig: &platform.TelegrafConfig{
					ID:        MustIDBase16(twoID),
					Name:      "tc2",
					LastModBy: MustIDBase16(fourID),
					LastMod:   now,
					Plugins: []platform.TelegrafPlugin{
						{
							Comment: "comment3",
							Config:  &inputs.CPUStats{},
						},
						{
							Comment: "comment2",
							Config:  &inputs.MemStats{},
						},
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

			tc, err := s.UpdateTelegrafConfig(ctx, tt.args.id,
				tt.args.telegrafConfig, tt.args.userID, tt.args.now)
			if err != nil && tt.wants.err == nil {
				t.Fatalf("expected errors to be nil got '%v'", err)
			}
			if err != nil && tt.wants.err != nil {
				if platform.ErrorCode(err) != platform.ErrorCode(tt.wants.err) {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}
			if diff := cmp.Diff(tc, tt.wants.telegrafConfig, telegrafCmpOptions...); tt.wants.err == nil && diff != "" {
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
						ResourceType: platform.TelegrafResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
						ResourceType: platform.TelegrafResourceType,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
					},
				},
			},
			args: args{
				id:     platform.ID(0),
				userID: MustIDBase16(threeID),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.EEmptyValue,
					Err:  platform.ErrInvalidID,
				},
				userResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
						ResourceType: platform.TelegrafResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
						ResourceType: platform.TelegrafResourceType,
					},
				},
				telegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
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
						ResourceType: platform.TelegrafResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
						ResourceType: platform.TelegrafResourceType,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
					},
				},
			},
			args: args{
				id:     MustIDBase16(fourID),
				userID: MustIDBase16(threeID),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
				},
				userResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Owner,
						ResourceType: platform.TelegrafResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
						ResourceType: platform.TelegrafResourceType,
					},
				},
				telegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
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
						ResourceType: platform.TelegrafResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(threeID),
						UserType:     platform.Member,
						ResourceType: platform.TelegrafResourceType,
					},
				},
				TelegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
					},
					{
						ID:        MustIDBase16(twoID),
						Name:      "tc2",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Comment: "comment1",
								Config: &inputs.File{
									Files: []string{"f1", "f2"},
								},
							},
							{
								Comment: "comment2",
								Config:  &inputs.MemStats{},
							},
						},
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
						ResourceType: platform.TelegrafResourceType,
					},
				},
				telegrafConfigs: []*platform.TelegrafConfig{
					{
						ID:        MustIDBase16(oneID),
						Name:      "tc1",
						LastModBy: MustIDBase16(threeID),
						Plugins: []platform.TelegrafPlugin{
							{
								Config: &inputs.CPUStats{},
							},
						},
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
				if platform.ErrorCode(err) != platform.ErrorCode(tt.wants.err) {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}
			tcs, n, err := s.FindTelegrafConfigs(ctx, platform.UserResourceMappingFilter{
				UserID:       tt.args.userID,
				ResourceType: platform.TelegrafResourceType,
			})
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

			urms, _, err := s.FindUserResourceMappings(ctx, platform.UserResourceMappingFilter{
				UserID:       tt.args.userID,
				ResourceType: platform.TelegrafResourceType,
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
