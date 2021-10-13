-- Demonstrate soft deleted rows will not be return to queries
-- IOX_SETUP: OneDeleteSimpleExprOneChunkDeleteAll

-- select *
SELECT * from cpu;

-- select one specific column
SELECT time from cpu;

-- select aggregate of every column inlcuding star
SELECT count(*), count(bar), count(time) from cpu;

-- select aggregate of every column
SELECT min(bar), max(bar), min(time), max(time) from cpu;

-- select aggregate of one column
SELECT max(bar) from cpu;