package migrations

import "embed"

//go:embed *up.sql
var AllUp embed.FS

//go:embed *down.sql
var AllDown embed.FS
