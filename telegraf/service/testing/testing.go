package testing

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	influxdb "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/telegraf/plugins/inputs"
	"github.com/influxdata/influxdb/v2/telegraf/plugins/outputs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	oneID   = platform.ID(1)
	twoID   = platform.ID(2)
	threeID = platform.ID(3)
	fourID  = platform.ID(4)
)

// TelegrafConfigFields includes prepopulated data for mapping tests.
type TelegrafConfigFields struct {
	IDGenerator     platform.IDGenerator
	TelegrafConfigs []*influxdb.TelegrafConfig
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
	cmp.Transformer("Sort", func(in []*influxdb.TelegrafConfig) []*influxdb.TelegrafConfig {
		out := append([]*influxdb.TelegrafConfig(nil), in...)
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID > out[j].ID
		})
		return out
	}),
}

type telegrafTestFactoryFunc func(TelegrafConfigFields, *testing.T) (influxdb.TelegrafConfigStore, func())

// TelegrafConfigStore tests all the service functions.
func TelegrafConfigStore(
	init telegrafTestFactoryFunc, t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init telegrafTestFactoryFunc,
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
	init telegrafTestFactoryFunc,
	t *testing.T,
) {
	type args struct {
		telegrafConfig *influxdb.TelegrafConfig
		userID         platform.ID
	}
	type wants struct {
		err       error
		telegrafs []*influxdb.TelegrafConfig
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
				IDGenerator:     mock.NewStaticIDGenerator(oneID),
				TelegrafConfigs: []*influxdb.TelegrafConfig{},
			},
			args: args{
				telegrafConfig: &influxdb.TelegrafConfig{},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EEmptyValue,
					Msg:  influxdb.ErrTelegrafConfigInvalidOrgID,
				},
			},
		},
		{
			name: "create telegraf config with empty set",
			fields: TelegrafConfigFields{
				IDGenerator:     mock.NewStaticIDGenerator(oneID),
				TelegrafConfigs: []*influxdb.TelegrafConfig{},
			},
			args: args{
				userID: threeID,
				telegrafConfig: &influxdb.TelegrafConfig{
					OrgID:    twoID,
					Name:     "name1",
					Config:   "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
					Metadata: map[string]interface{}{"buckets": []interface{}{}},
				},
			},
			wants: wants{
				telegrafs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    twoID,
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
				IDGenerator: mock.NewStaticIDGenerator(twoID),
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    twoID,
						Name:     "tc1",
						Config:   "[[inputs.mem_stats]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				userID: threeID,
				telegrafConfig: &influxdb.TelegrafConfig{
					OrgID:    twoID,
					Name:     "name2",
					Config:   "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
					Metadata: map[string]interface{}{"buckets": []interface{}{}}, // for inmem test as it doesn't unmarshal..
				},
			},
			wants: wants{
				telegrafs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    twoID,
						Name:     "tc1",
						Config:   "[[inputs.mem_stats]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       twoID,
						OrgID:    twoID,
						Name:     "name2",
						Config:   "[[inputs.cpu]]\n[[outputs.influxdb_v2]]\n",
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
			err := s.CreateTelegrafConfig(ctx, tt.args.telegrafConfig, tt.args.userID)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}
			if tt.wants.err == nil && !tt.args.telegrafConfig.ID.Valid() {
				t.Fatalf("telegraf config ID not set from CreateTelegrafConfig")
			}

			if err != nil && tt.wants.err != nil {
				if errors.ErrorCode(err) != errors.ErrorCode(tt.wants.err) {
					t.Fatalf("expected error messages to match '%v' got '%v'", errors.ErrorCode(tt.wants.err), errors.ErrorCode(err))
				}
			}

			filter := influxdb.TelegrafConfigFilter{}
			tcs, _, err := s.FindTelegrafConfigs(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve telegraf configs: %v", err)
			}
			if diff := cmp.Diff(tcs, tt.wants.telegrafs, telegrafCmpOptions...); diff != "" {
				t.Errorf("telegraf configs are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindTelegrafConfigByID testing.
func FindTelegrafConfigByID(
	init telegrafTestFactoryFunc,
	t *testing.T,
) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err            error
		telegrafConfig *influxdb.TelegrafConfig
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
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    twoID,
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       twoID,
						OrgID:    twoID,
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
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:     oneID,
						OrgID:  twoID,
						Name:   "tc1",
						Config: "[[inputs.cpu]]\n",
					},
					{
						ID:     twoID,
						OrgID:  twoID,
						Name:   "tc2",
						Config: "[[inputs.file]]\n[[inputs.mem]]\n",
					},
				},
			},
			args: args{
				id: threeID,
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Msg:  "telegraf configuration not found",
				},
			},
		},
		{
			name: "basic find telegraf config by id",
			fields: TelegrafConfigFields{
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    threeID,
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       twoID,
						OrgID:    threeID,
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				id: twoID,
			},
			wants: wants{
				telegrafConfig: &influxdb.TelegrafConfig{
					ID:       twoID,
					OrgID:    threeID,
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
	init telegrafTestFactoryFunc,
	t *testing.T,
) {
	type args struct {
		filter influxdb.TelegrafConfigFilter
		opts   []influxdb.FindOptions
	}

	type wants struct {
		telegrafConfigs []*influxdb.TelegrafConfig
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
				TelegrafConfigs: []*influxdb.TelegrafConfig{},
			},
			args: args{
				filter: influxdb.TelegrafConfigFilter{},
			},
			wants: wants{
				telegrafConfigs: []*influxdb.TelegrafConfig{},
			},
		},
		{
			name: "find all telegraf configs (across orgs)",
			fields: TelegrafConfigFields{
				IDGenerator: mock.NewIncrementingIDGenerator(oneID),
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    twoID,
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       twoID,
						OrgID:    threeID,
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				filter: influxdb.TelegrafConfigFilter{},
			},
			wants: wants{
				telegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    twoID,
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       twoID,
						OrgID:    threeID,
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
		},
		{
			name: "filter by organization only",
			fields: TelegrafConfigFields{
				IDGenerator: mock.NewIncrementingIDGenerator(oneID),
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    fourID,
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       twoID,
						OrgID:    fourID,
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       threeID,
						OrgID:    oneID,
						Name:     "tc3",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       fourID,
						OrgID:    oneID,
						Name:     "tc4",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				filter: influxdb.TelegrafConfigFilter{
					OrgID: &oneID,
				},
			},
			wants: wants{
				telegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       threeID,
						OrgID:    oneID,
						Name:     "tc3",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       fourID,
						OrgID:    oneID,
						Name:     "tc4",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
		},
		{
			name: "empty for provided org",
			fields: TelegrafConfigFields{
				IDGenerator: mock.NewIncrementingIDGenerator(oneID),
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:     oneID,
						OrgID:  threeID,
						Name:   "tc1",
						Config: "[[inputs.cpu]]\n",
					},
					{
						ID:     twoID,
						OrgID:  threeID,
						Name:   "tc2",
						Config: "[[inputs.file]]\n[[inputs.mem]]\n",
					},
				},
			},
			args: args{
				filter: influxdb.TelegrafConfigFilter{
					OrgID: &oneID,
				},
			},
			wants: wants{
				telegrafConfigs: []*influxdb.TelegrafConfig{},
			},
		},
		{
			name: "find with limit and offset",
			fields: TelegrafConfigFields{
				IDGenerator: mock.NewIncrementingIDGenerator(oneID),
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    fourID,
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       twoID,
						OrgID:    fourID,
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       threeID,
						OrgID:    oneID,
						Name:     "tc3",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       fourID,
						OrgID:    oneID,
						Name:     "tc4",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				opts: []influxdb.FindOptions{
					{Limit: 2, Offset: 1},
				},
			},
			wants: wants{
				telegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       twoID,
						OrgID:    fourID,
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       threeID,
						OrgID:    oneID,
						Name:     "tc3",
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

			tcs, _, err := s.FindTelegrafConfigs(ctx, tt.args.filter, tt.args.opts...)
			if err != nil && tt.wants.err == nil {
				t.Fatalf("expected errors to be nil got '%v'", err)
			}

			require.Equal(t, tt.wants.err, err)
			assert.Equal(t, tt.wants.telegrafConfigs, tcs)
		})
	}
}

// UpdateTelegrafConfig testing.
func UpdateTelegrafConfig(
	init telegrafTestFactoryFunc,
	t *testing.T,
) {
	type args struct {
		userID         platform.ID
		id             platform.ID
		telegrafConfig *influxdb.TelegrafConfig
	}

	type wants struct {
		telegrafConfig *influxdb.TelegrafConfig
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
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:     oneID,
						OrgID:  fourID,
						Name:   "tc1",
						Config: "[[inputs.cpu]]\n",
					},
					{
						ID:     twoID,
						OrgID:  fourID,
						Name:   "tc2",
						Config: "[[inputs.file]]\n[[inputs.mem]]\n",
					},
				},
			},
			args: args{
				userID: threeID,
				id:     fourID,
				telegrafConfig: &influxdb.TelegrafConfig{
					Name:   "tc2",
					Config: "[[inputs.file]]\n[[inputs.mem]]\n",
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Msg:  fmt.Sprintf("telegraf config with ID %v not found", fourID),
				},
			},
		},
		{
			fields: TelegrafConfigFields{
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    fourID,
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       twoID,
						OrgID:    fourID,
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				userID: fourID,
				id:     twoID,
				telegrafConfig: &influxdb.TelegrafConfig{
					OrgID:  oneID, // notice this get ignored - ie., resulting TelegrafConfig will have OrgID equal to fourID
					Name:   "tc2",
					Config: "[[inputs.file]]\n[[inputs.mem]]\n",
				},
			},
			wants: wants{
				telegrafConfig: &influxdb.TelegrafConfig{
					ID:     twoID,
					OrgID:  fourID,
					Name:   "tc2",
					Config: "[[inputs.file]]\n[[inputs.mem]]\n",
				},
			},
		},
		{
			name: "config update",
			fields: TelegrafConfigFields{
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:     oneID,
						OrgID:  oneID,
						Name:   "tc1",
						Config: "[[inputs.cpu]]\n",
					},
					{
						ID:     twoID,
						OrgID:  oneID,
						Name:   "tc2",
						Config: "[[inputs.file]]\n[[inputs.kubernetes]]\n[[inputs.kubernetes]]\n",
					},
				},
			},
			args: args{
				userID: fourID,
				id:     twoID,
				telegrafConfig: &influxdb.TelegrafConfig{
					Name:   "tc2",
					Config: "[[inputs.file]]\n[[inputs.kubernetes]]\n[[inputs.kubernetes]]\n",
				},
			},
			wants: wants{
				telegrafConfig: &influxdb.TelegrafConfig{
					ID:     twoID,
					OrgID:  oneID,
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
				if errors.ErrorCode(err) != errors.ErrorCode(tt.wants.err) {
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
	init telegrafTestFactoryFunc,
	t *testing.T,
) {
	type args struct {
		id platform.ID
	}

	type wants struct {
		telegrafConfigs []*influxdb.TelegrafConfig
		err             error
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
				IDGenerator: mock.NewIncrementingIDGenerator(oneID),
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    fourID,
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       twoID,
						OrgID:    fourID,
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
				telegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    fourID,
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       twoID,
						OrgID:    fourID,
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
				IDGenerator: mock.NewIncrementingIDGenerator(oneID),
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    threeID,
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       twoID,
						OrgID:    threeID,
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				id: fourID,
			},
			wants: wants{
				err: fmt.Errorf("telegraf configuration not found"),
				telegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    threeID,
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       twoID,
						OrgID:    threeID,
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
				IDGenerator: mock.NewIncrementingIDGenerator(oneID),
				TelegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    twoID,
						Name:     "tc1",
						Config:   "[[inputs.cpu]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
					{
						ID:       twoID,
						OrgID:    twoID,
						Name:     "tc2",
						Config:   "[[inputs.file]]\n[[inputs.mem]]\n",
						Metadata: map[string]interface{}{"buckets": []interface{}{}},
					},
				},
			},
			args: args{
				id: twoID,
			},
			wants: wants{
				telegrafConfigs: []*influxdb.TelegrafConfig{
					{
						ID:       oneID,
						OrgID:    twoID,
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

			tcs, _, err := s.FindTelegrafConfigs(ctx, influxdb.TelegrafConfigFilter{})
			require.NoError(t, err)
			assert.Equal(t, tt.wants.telegrafConfigs, tcs)
		})
	}
}
