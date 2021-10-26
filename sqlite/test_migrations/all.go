package test_migrations

import "embed"

//go:embed *.sql
var All embed.FS

//go:embed 0001_create_migrations_table.sql
var MigrationTable embed.FS

//go:embed 0001_create_migrations_table.sql 0002_create_test_table_1.sql
var First embed.FS
