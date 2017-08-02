package server

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/mocks"
)

func Test_newSourceResponse(t *testing.T) {
	tests := []struct {
		name string
		src  chronograf.Source
		want sourceResponse
	}{
		{
			name: "Test empty telegraf",
			src: chronograf.Source{
				ID:       1,
				Telegraf: "",
			},
			want: sourceResponse{
				Source: chronograf.Source{
					ID:       1,
					Telegraf: "telegraf",
				},
				Links: sourceLinks{
					Self:        "/chronograf/v1/sources/1",
					Proxy:       "/chronograf/v1/sources/1/proxy",
					Queries:     "/chronograf/v1/sources/1/queries",
					Write:       "/chronograf/v1/sources/1/write",
					Kapacitors:  "/chronograf/v1/sources/1/kapacitors",
					Users:       "/chronograf/v1/sources/1/users",
					Permissions: "/chronograf/v1/sources/1/permissions",
					Databases:   "/chronograf/v1/sources/1/dbs",
				},
			},
		},
		{
			name: "Test non-default telegraf",
			src: chronograf.Source{
				ID:       1,
				Telegraf: "howdy",
			},
			want: sourceResponse{
				Source: chronograf.Source{
					ID:       1,
					Telegraf: "howdy",
				},
				Links: sourceLinks{
					Self:        "/chronograf/v1/sources/1",
					Proxy:       "/chronograf/v1/sources/1/proxy",
					Queries:     "/chronograf/v1/sources/1/queries",
					Write:       "/chronograf/v1/sources/1/write",
					Kapacitors:  "/chronograf/v1/sources/1/kapacitors",
					Users:       "/chronograf/v1/sources/1/users",
					Permissions: "/chronograf/v1/sources/1/permissions",
					Databases:   "/chronograf/v1/sources/1/dbs",
				},
			},
		},
	}
	for _, tt := range tests {
		if got := newSourceResponse(tt.src); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. newSourceResponse() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestService_newSourceKapacitor(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
		ServersStore chronograf.ServersStore
		Logger       chronograf.Logger
	}
	type args struct {
		ctx  context.Context
		src  chronograf.Source
		kapa chronograf.Server
	}
	srcCount := 0
	srvCount := 0
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantSrc int
		wantSrv int
		wantErr bool
	}{
		{
			name: "Add when no existing sources",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AllF: func(ctx context.Context) ([]chronograf.Source, error) {
						return []chronograf.Source{}, nil
					},
					AddF: func(ctx context.Context, src chronograf.Source) (chronograf.Source, error) {
						srcCount++
						src.ID = srcCount
						return src, nil
					},
				},
				ServersStore: &mocks.ServersStore{
					AddF: func(ctx context.Context, srv chronograf.Server) (chronograf.Server, error) {
						srvCount++
						return srv, nil
					},
				},
			},
			args: args{
				ctx: context.Background(),
				src: chronograf.Source{
					Name: "Influx 1",
				},
				kapa: chronograf.Server{
					Name: "Kapa 1",
				},
			},
			wantSrc: 1,
			wantSrv: 1,
		},
		{
			name: "Should not add if existing source",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AllF: func(ctx context.Context) ([]chronograf.Source, error) {
						return []chronograf.Source{
							{
								Name: "Influx 1",
							},
						}, nil
					},
					AddF: func(ctx context.Context, src chronograf.Source) (chronograf.Source, error) {
						srcCount++
						src.ID = srcCount
						return src, nil
					},
				},
				ServersStore: &mocks.ServersStore{
					AddF: func(ctx context.Context, srv chronograf.Server) (chronograf.Server, error) {
						srvCount++
						return srv, nil
					},
				},
				Logger: &mocks.TestLogger{},
			},
			args: args{
				ctx: context.Background(),
				src: chronograf.Source{
					Name: "Influx 1",
				},
				kapa: chronograf.Server{
					Name: "Kapa 1",
				},
			},
			wantSrc: 0,
			wantSrv: 0,
		},
		{
			name: "Error if All returns error",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AllF: func(ctx context.Context) ([]chronograf.Source, error) {
						return nil, fmt.Errorf("error")
					},
				},
				Logger: &mocks.TestLogger{},
			},
			args: args{
				ctx: context.Background(),
			},
			wantErr: true,
		},
		{
			name: "Error if Add returns error",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AllF: func(ctx context.Context) ([]chronograf.Source, error) {
						return []chronograf.Source{}, nil
					},
					AddF: func(ctx context.Context, src chronograf.Source) (chronograf.Source, error) {
						return chronograf.Source{}, fmt.Errorf("error")
					},
				},
				Logger: &mocks.TestLogger{},
			},
			args: args{
				ctx: context.Background(),
			},
			wantErr: true,
		},
		{
			name: "Error if kapa add is error",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AllF: func(ctx context.Context) ([]chronograf.Source, error) {
						return []chronograf.Source{}, nil
					},
					AddF: func(ctx context.Context, src chronograf.Source) (chronograf.Source, error) {
						srcCount++
						src.ID = srcCount
						return src, nil
					},
				},
				ServersStore: &mocks.ServersStore{
					AddF: func(ctx context.Context, srv chronograf.Server) (chronograf.Server, error) {
						srvCount++
						return chronograf.Server{}, fmt.Errorf("error")
					},
				},
				Logger: &mocks.TestLogger{},
			},
			args: args{
				ctx: context.Background(),
				src: chronograf.Source{
					Name: "Influx 1",
				},
				kapa: chronograf.Server{
					Name: "Kapa 1",
				},
			},
			wantSrc: 1,
			wantSrv: 1,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcCount = 0
			srvCount = 0
			h := &Service{
				SourcesStore: tt.fields.SourcesStore,
				ServersStore: tt.fields.ServersStore,
				Logger:       tt.fields.Logger,
			}
			if err := h.newSourceKapacitor(tt.args.ctx, tt.args.src, tt.args.kapa); (err != nil) != tt.wantErr {
				t.Errorf("Service.newSourceKapacitor() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantSrc != srcCount {
				t.Errorf("Service.newSourceKapacitor() count = %d, wantSrc %d", srcCount, tt.wantSrc)
			}
		})
	}
}
