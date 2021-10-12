-- Demonstrate soft deleted rows will not be return to queries
-- IOX_SETUP: TwoDeletesMultiExprsOneChunk

-- select *
SELECT * from cpu;

SELECT foo from cpu;

SELECT * from cpu where cast(time as bigint) > 30;

SELECT count(bar) from cpu where cast(time as bigint) > 30;

SELECT * from cpu where cast(time as bigint) > 40;

SELECT max(time) from cpu where cast(time as bigint) > 40;
