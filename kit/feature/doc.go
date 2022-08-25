// Package feature provides feature flagging capabilities for InfluxDB servers.
// This document describes this package and how it is used to control
// experimental features in `influxd`.
//
// Flags are configured in `flags.yml` at the top of this repository.
// Running `make flags` generates Go code based on this configuration
// to programmatically test flag values in a given request context.
// Boolean flags are the most common case, but integers, floats and
// strings are supported for more complicated experiments.
//
// The `Flagger` interface is the crux of this package.
// It computes a map of feature flag values for a given request context.
// The default implementation always returns the flag default configured
// in `flags.yml`. The override implementation allows an operator to
// override feature flag defaults at startup. Changing these overrides
// requires a restart.
//
// In `influxd`, a `Flagger` instance is provided to a `Handler` middleware
// configured to intercept all API requests and annotate their request context
// with a map of feature flags.
//
// A flag can opt in to be exposed externally in `flags.yml`. If exposed,
// this flag will be included in the response from the `/api/v2/flags`
// endpoint. This allows the UI and other API clients to control their
// behavior according to the flag in addition to the server itself.
//
// A concrete example to illustrate the above:
//
// I have a feature called "My Feature" that will involve turning on new code
// in both the UI and the server.
//
// First, I add an entry to `flags.yml`.
//
// ```yaml
//   - name: My Feature
//     description: My feature is awesome
//     key: myFeature
//     default: false
//     expose: true
//     contact: My Name
//
// ```
//
// My flag type is inferred to be boolean by my default of `false` when I run
// `make flags` and the `feature` package now includes `func MyFeature() BoolFlag`.
//
// # I use this to control my backend code with
//
// ```go
//
//	if feature.MyFeature.Enabled(ctx) {
//	  // new code...
//	} else {
//
//	  // new code...
//	}
//
// ```
//
// and the `/api/v2/flags` response provides the same information to the frontend.
//
// ```json
//
//	{
//	  "myFeature": false
//	}
//
// ```
//
// While `false` by default, I can turn on my experimental feature by starting
// my server with a flag override.
//
// ```
// env INFLUXD_FEATURE_FLAGS="{\"flag1\":\value1\",\"key2\":\"value2\"}" influxd
// ```
//
// ```
// influxd --feature-flags flag1=value1,flag2=value2
// ```
package feature
