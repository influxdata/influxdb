package server

import (
	"net/http"
	"testing"

	"github.com/influxdata/chronograf"
)

func TestService_GetDatabases(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore      chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				SourcesStore:     tt.fields.SourcesStore,
				ServersStore:     tt.fields.ServersStore,
				LayoutsStore:      tt.fields.LayoutsStore,
				UsersStore:       tt.fields.UsersStore,
				DashboardsStore:  tt.fields.DashboardsStore,
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.GetDatabases(tt.args.w, tt.args.r)
		})
	}
}

func TestService_NewDatabase(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore      chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				SourcesStore:     tt.fields.SourcesStore,
				ServersStore:     tt.fields.ServersStore,
				LayoutsStore:      tt.fields.LayoutsStore,
				UsersStore:       tt.fields.UsersStore,
				DashboardsStore:  tt.fields.DashboardsStore,
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.NewDatabase(tt.args.w, tt.args.r)
		})
	}
}

func TestService_DropDatabase(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore      chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				SourcesStore:     tt.fields.SourcesStore,
				ServersStore:     tt.fields.ServersStore,
				LayoutsStore:      tt.fields.LayoutsStore,
				UsersStore:       tt.fields.UsersStore,
				DashboardsStore:  tt.fields.DashboardsStore,
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.DropDatabase(tt.args.w, tt.args.r)
		})
	}
}

func TestService_RetentionPolicies(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore      chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				SourcesStore:     tt.fields.SourcesStore,
				ServersStore:     tt.fields.ServersStore,
				LayoutsStore:      tt.fields.LayoutsStore,
				UsersStore:       tt.fields.UsersStore,
				DashboardsStore:  tt.fields.DashboardsStore,
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.RetentionPolicies(tt.args.w, tt.args.r)
		})
	}
}

func TestService_NewRetentionPolicy(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore      chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				SourcesStore:     tt.fields.SourcesStore,
				ServersStore:     tt.fields.ServersStore,
				LayoutsStore:      tt.fields.LayoutsStore,
				UsersStore:       tt.fields.UsersStore,
				DashboardsStore:  tt.fields.DashboardsStore,
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.NewRetentionPolicy(tt.args.w, tt.args.r)
		})
	}
}

func TestService_UpdateRetentionPolicy(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore      chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				SourcesStore:     tt.fields.SourcesStore,
				ServersStore:     tt.fields.ServersStore,
				LayoutsStore:      tt.fields.LayoutsStore,
				UsersStore:       tt.fields.UsersStore,
				DashboardsStore:  tt.fields.DashboardsStore,
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.UpdateRetentionPolicy(tt.args.w, tt.args.r)
		})
	}
}

func TestService_DropRetentionPolicy(t *testing.T) {
	type fields struct {
		SourcesStore     chronograf.SourcesStore
		ServersStore     chronograf.ServersStore
		LayoutsStore      chronograf.LayoutsStore
		UsersStore       chronograf.UsersStore
		DashboardsStore  chronograf.DashboardsStore
		TimeSeriesClient TimeSeriesClient
		Logger           chronograf.Logger
		UseAuth          bool
		Databases        chronograf.Databases
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Service{
				SourcesStore:     tt.fields.SourcesStore,
				ServersStore:     tt.fields.ServersStore,
				LayoutsStore:      tt.fields.LayoutsStore,
				UsersStore:       tt.fields.UsersStore,
				DashboardsStore:  tt.fields.DashboardsStore,
				TimeSeriesClient: tt.fields.TimeSeriesClient,
				Logger:           tt.fields.Logger,
				UseAuth:          tt.fields.UseAuth,
				Databases:        tt.fields.Databases,
			}
			h.DropRetentionPolicy(tt.args.w, tt.args.r)
		})
	}
}

func TestValidDatabaseRequest(t *testing.T) {
	type args struct {
		d *chronograf.Database
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidDatabaseRequest(tt.args.d); (err != nil) != tt.wantErr {
				t.Errorf("ValidDatabaseRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidRetentionPolicyRequest(t *testing.T) {
	type args struct {
		rp *chronograf.RetentionPolicy
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidRetentionPolicyRequest(tt.args.rp); (err != nil) != tt.wantErr {
				t.Errorf("ValidRetentionPolicyRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
