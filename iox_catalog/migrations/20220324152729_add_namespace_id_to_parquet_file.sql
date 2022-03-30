ALTER TABLE
  IF EXISTS parquet_file
  ADD COLUMN namespace_id INT;

ALTER TABLE
  IF EXISTS parquet_file
  ADD FOREIGN KEY (namespace_id)
  REFERENCES namespace (id) MATCH SIMPLE
  ON UPDATE NO ACTION
  ON DELETE NO ACTION
  NOT VALID;

UPDATE parquet_file p
  SET namespace_id=t.namespace_id
  FROM table_name t
  WHERE t.id=p.table_id;

ALTER TABLE parquet_file
  ALTER COLUMN namespace_id SET NOT NULL;
