-- Avoid seqscan when filtering columns by their table ID.
CREATE INDEX IF NOT EXISTS column_name_table_idx ON column_name (table_id);
