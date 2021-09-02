package influxdb

import (
	"context"
	"errors"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/coordinator"
)

type key int

const dependenciesKey key = iota

type PointsWriter interface {
	WritePointsInto(request *coordinator.IntoWriteRequest) error
}

type StorageDependencies struct {
	Reader       Reader
	MetaClient   MetaClient
	Authorizer   Authorizer
	AuthEnabled  bool
	PointsWriter PointsWriter
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
	if d.PointsWriter == nil {
		return errors.New("missing points writer dependency")
	}
	return nil
}

func GetStorageDependencies(ctx context.Context) StorageDependencies {
	return ctx.Value(dependenciesKey).(StorageDependencies)
}

type Dependencies struct {
	StorageDeps StorageDependencies
	FluxDeps    flux.Dependency
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
	writer PointsWriter,
) (Dependencies, error) {
	fdeps := flux.NewDefaultDependencies()
	deps := Dependencies{FluxDeps: fdeps}
	deps.StorageDeps = StorageDependencies{
		Reader:       reader,
		MetaClient:   mc,
		Authorizer:   auth,
		AuthEnabled:  authEnabled,
		PointsWriter: writer,
	}
	if err := deps.StorageDeps.Validate(); err != nil {
		return Dependencies{}, err
	}
	return deps, nil
}
