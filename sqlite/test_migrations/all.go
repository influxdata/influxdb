package test_migrations

import "embed"

//go:embed *.sql
var All embed.FS

//go:embed 0001_create_test_table_1.sql
var First embed.FS

//go:embed 0002_rename_test_table_id_1.sql 0003_create_test_table_2.sql
var Rest embed.FS
