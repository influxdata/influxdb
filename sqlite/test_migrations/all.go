package test_migrations

import "embed"

//go:embed *.up.sql
var AllUp embed.FS

//go:embed *.down.sql
var AllDown embed.FS

//go:embed 0001_create_migrations_table.up.sql
var MigrationTable embed.FS

//go:embed 0001_create_migrations_table.up.sql 0002_create_test_table_1.up.sql
var FirstUp embed.FS

//go:embed 0001_create_migrations_table.down.sql 0002_create_test_table_1.down.sql
var FirstDown embed.FS
