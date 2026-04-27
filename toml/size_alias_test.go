package toml_test

import (
	itoml "github.com/influxdata/influxdb/v2/toml"
)

// Compile-time check that the branch-specific aliases in size_alias.go
// resolve to the expected V1 implementations on the 1.x branch. If this
// file is cherry-picked to the 2.x branch (where the aliases should point
// at SizeV2/SSizeV2), compilation will fail — the mismatch is a signal
// that the aliases or this assertion diverged, not a bug to fix by
// weakening the check.
var (
	_ itoml.SizeV2  = itoml.Size(0)
	_ itoml.SSizeV2 = itoml.SSize(0)
)
