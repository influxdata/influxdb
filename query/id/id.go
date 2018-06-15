// Package id exports one type, ID, which is aliased to github.com/influxdata/platform.ID.
// This package was introduced when the query code was in the separate ifql repository,
// and this type alias is provided only temporarily until other repositories have migrated to platform.ID.
package id

import "github.com/influxdata/platform"

type ID = platform.ID
