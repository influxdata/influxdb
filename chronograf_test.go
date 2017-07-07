package chronograf_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/mocks"
)

func Test_NewSources(t *testing.T) {
	t.Parallel()

	srcsKaps := []chronograf.SourceAndKapacitor{
		{
			Source: chronograf.Source{
				Default:            true,
				InsecureSkipVerify: false,
				MetaURL:            "http://metaurl.com",
				Name:               "Influx 1",
				Password:           "pass1",
				Telegraf:           "telegraf",
				URL:                "http://localhost:8086",
				Username:           "user1",
			},
			Kapacitor: chronograf.Server{
				Active: true,
				Name:   "Kapa 1",
				URL:    "http://localhost:9092",
			},
		},
	}
	saboteurSrcsKaps := []chronograf.SourceAndKapacitor{
		{
			Source: chronograf.Source{
				Name: "Influx 1",
			},
			Kapacitor: chronograf.Server{
				Name: "Kapa Aspiring Saboteur",
			},
		},
	}

	ctx := context.Background()
	srcs := []chronograf.Source{}
	srcsStore := mocks.SourcesStore{
		AllF: func(ctx context.Context) ([]chronograf.Source, error) {
			return srcs, nil
		},
		AddF: func(ctx context.Context, src chronograf.Source) (chronograf.Source, error) {
			srcs = append(srcs, src)
			return src, nil
		},
	}
	srvs := []chronograf.Server{}
	srvsStore := mocks.ServersStore{
		AddF: func(ctx context.Context, srv chronograf.Server) (chronograf.Server, error) {
			srvs = append(srvs, srv)
			return srv, nil
		},
	}

	err := chronograf.NewSources(ctx, &srcsStore, &srvsStore, srcsKaps, &mocks.TestLogger{})
	if err != nil {
		t.Fatal("Expected no error when creating New Sources. Error:", err)
	}
	if len(srcs) != 1 {
		t.Error("Expected one source in sourcesStore")
	}
	if len(srvs) != 1 {
		t.Error("Expected one source in serversStore")
	}

	err = chronograf.NewSources(ctx, &srcsStore, &srvsStore, saboteurSrcsKaps, &mocks.TestLogger{})
	if err != nil {
		t.Fatal("Expected no error when creating New Sources. Error:", err)
	}
	if len(srcs) != 1 {
		t.Error("Expected one source in sourcesStore")
	}
	if len(srvs) != 1 {
		t.Error("Expected one source in serversStore")
	}
	if !reflect.DeepEqual(srcs[0], srcsKaps[0].Source) {
		t.Error("Expected source in sourceStore to remain unchanged")
	}
}
