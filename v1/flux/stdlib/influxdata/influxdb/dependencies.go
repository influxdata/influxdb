package influxdb

import (
	"context"
	"errors"

	"github.com/influxdata/flux"
)

type key int

const dependenciesKey key = iota

type StorageDependencies struct {
	Reader      Reader
	MetaClient  MetaClient
	Authorizer  Authorizer
	AuthEnabled bool
}

func (d StorageDependencies) Inject(ctx context.Context) context.Context {
	return context.WithValue(ctx, dependenciesKey, d)
}

func (d StorageDependencies) Validate() error {
	if d.Reader == nil {
		return errors.New("missing reader dependency")
	}
	if d.MetaClient == nil {
		return errors.New("missing meta client dependency")
	}
	if d.AuthEnabled && d.Authorizer == nil {
		return errors.New("missing authorizer dependency")
	}
	return nil
}

func GetStorageDependencies(ctx context.Context) StorageDependencies {
	return ctx.Value(dependenciesKey).(StorageDependencies)
}

type Dependencies struct {
	StorageDeps StorageDependencies
	FluxDeps    flux.Dependencies
}

func (d Dependencies) Inject(ctx context.Context) context.Context {
	ctx = d.FluxDeps.Inject(ctx)
	return d.StorageDeps.Inject(ctx)
}

func NewDependencies(
	mc MetaClient,
	reader Reader,
	auth Authorizer,
	authEnabled bool,
) (Dependencies, error) {
	fdeps := flux.NewDefaultDependencies()
	deps := Dependencies{FluxDeps: fdeps}
	deps.StorageDeps = StorageDependencies{
		Reader:      reader,
		MetaClient:  mc,
		Authorizer:  auth,
		AuthEnabled: authEnabled,
	}
	if err := deps.StorageDeps.Validate(); err != nil {
		return Dependencies{}, err
	}
	return deps, nil
}
