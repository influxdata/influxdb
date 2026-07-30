package influxdb

import (
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies/url"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/stretchr/testify/require"
)

// stub implementations satisfy the dependency interfaces well enough to pass
// StorageDependencies.Validate (which only checks for non-nil values).
type stubReader struct{ Reader }

type stubMetaClient struct{}

func (stubMetaClient) Databases() []meta.DatabaseInfo     { return nil }
func (stubMetaClient) Database(string) *meta.DatabaseInfo { return nil }

type stubPointsWriter struct{}

func (stubPointsWriter) WritePointsInto(*coordinator.IntoWriteRequest) error { return nil }

func TestWithURLValidator(t *testing.T) {
	var d flux.Deps
	WithURLValidator(url.PrivateIPValidator{})(&d)

	v, err := d.URLValidator()
	require.NoError(t, err)
	require.IsType(t, url.PrivateIPValidator{}, v)
	require.NotNil(t, d.Deps.HTTPClient, "expected HTTP client to be set alongside the validator")
}

func TestNewDependencies_AcceptsURLValidatorOption(t *testing.T) {
	// Both the default (no option) and hardened (PrivateIPValidator) paths
	// must construct without error; server.go selects between them based on
	// the hardening-enabled config option.
	for _, opts := range [][]FluxDepOption{
		nil,
		{WithURLValidator(url.PrivateIPValidator{})},
	} {
		_, err := NewDependencies(stubMetaClient{}, stubReader{}, nil, false, stubPointsWriter{}, opts...)
		require.NoError(t, err)
	}
}
