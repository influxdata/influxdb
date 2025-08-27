package authorization_test

import (
	"testing"

	"github.com/go-crypt/crypt/algorithm"
	"github.com/influxdata/influxdb/v2/authorization"
	influxdb2_algo "github.com/influxdata/influxdb/v2/pkg/crypt/algorithm/influxdb2"
	"github.com/stretchr/testify/require"
)

func Test_NewAuthorizationHasher_EmptyDecoderVariants(t *testing.T) {
	hasher, err := authorization.NewAuthorizationHasher(
		authorization.WithDecoderVariants([]influxdb2_algo.Variant{}),
	)

	require.ErrorIs(t, err, authorization.ErrNoDecoders)
	require.Nil(t, hasher)
}

func TestNewAuthorizationHasher_WithInvalidDecoderVariant(t *testing.T) {
	// Test that using an invalid decoder variant returns an error
	hasher, err := authorization.NewAuthorizationHasher(
		authorization.WithDecoderVariants([]influxdb2_algo.Variant{
			influxdb2_algo.Variant(-1), // Invalid variant
		}),
	)

	// Should return an error and nil hasher
	require.ErrorIs(t, err, algorithm.ErrParameterInvalid)
	require.Contains(t, err.Error(), "registering variant")
	require.Nil(t, hasher)
}

func TestNewAuthorizationHasher_WithHasherVariantInvalid(t *testing.T) {
	// Test that using VariantNone returns an error
	hasher, err := authorization.NewAuthorizationHasher(
		authorization.WithHasherVariant(influxdb2_algo.Variant(-1)),
	)

	// Should return an error and nil hasher
	require.ErrorIs(t, err, algorithm.ErrParameterInvalid)
	require.ErrorContains(t, err, "creating hasher")
	require.Nil(t, hasher)
}
