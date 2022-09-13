CREATE TABLE IF NOT EXISTS billing_summary (
    namespace_id INT NOT NULL,
    total_file_size_bytes BIGINT NOT NULL,
    PRIMARY KEY (namespace_id)
);

CREATE INDEX IF NOT EXISTS billing_summary_namespace_idx ON billing_summary (namespace_id);

ALTER TABLE
    IF EXISTS billing_summary
ADD
    FOREIGN KEY (namespace_id) REFERENCES namespace (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

CREATE OR REPLACE FUNCTION increment_billing_summary()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
    INSERT INTO billing_summary (namespace_id, total_file_size_bytes)
    VALUES (NEW.namespace_id, NEW.file_size_bytes)
    ON CONFLICT (namespace_id) DO UPDATE
    SET total_file_size_bytes = billing_summary.total_file_size_bytes + NEW.file_size_bytes
    WHERE billing_summary.namespace_id = NEW.namespace_id;
    RETURN NEW;
END;
$$ ;

CREATE TRIGGER update_billing
    AFTER INSERT
    ON parquet_file
    FOR EACH ROW
    EXECUTE PROCEDURE increment_billing_summary();

CREATE OR REPLACE FUNCTION maybe_decrement_billing_summary()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
    AS
$$
BEGIN
    IF OLD.to_delete IS NULL AND NEW.to_delete IS NOT NULL THEN
        UPDATE billing_summary
        SET total_file_size_bytes = billing_summary.total_file_size_bytes - OLD.file_size_bytes
        WHERE billing_summary.namespace_id = OLD.namespace_id;
    END IF;
    RETURN OLD;
END;
$$ ;

CREATE TRIGGER decrement_summary
    AFTER UPDATE
    ON parquet_file
    FOR EACH ROW
    EXECUTE PROCEDURE maybe_decrement_billing_summary();
