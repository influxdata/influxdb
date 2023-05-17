-- Test for dedup across different domains (like time range, partitions, et.c)
-- IOX_SETUP: DuplicateDifferentDomains

select * from m order by time;

-- IOX_COMPARE: uuid
explain select * from m order by time;
