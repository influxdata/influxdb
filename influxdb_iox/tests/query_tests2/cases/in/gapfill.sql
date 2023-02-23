-- Gap-filling tests
-- IOX_SETUP: OneMeasurementTwoSeries

-- Input data
-- region=a 2000-05-05T12:20:00Z
-- region=a 2000-05-05T12:40:00Z
-- region=b 2000-05-05T12:31:00Z
-- region=b 2000-05-05T12:39:00Z

-- IOX_COMPARE: uuid
EXPLAIN SELECT
  date_bin_gapfill(interval '10 minute', time, timestamp '1970-01-01T00:00:00Z') as minute,
  count(cpu.user)
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by minute;

-- Gap filling with no other group keys
SELECT
  date_bin_gapfill(interval '10 minute', time, timestamp '1970-01-01T00:00:00Z') as minute,
  count(cpu.user)
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by minute;

-- Gap filling with no other group keys and no aggregates
SELECT
  date_bin_gapfill(interval '10 minute', time, timestamp '1970-01-01T00:00:00Z') as minute
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by minute;

-- gap filling with a group key
SELECT
  date_bin_gapfill(interval '10 minute', time, timestamp '1970-01-01T00:00:00Z') as minute,
  region,
  count(cpu.user)
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by minute, region;

-- gap filling with an origin argument that is not the epoch
SELECT
  date_bin_gapfill(interval '10 minute', time, timestamp '1970-01-01T00:00:07Z') as minute,
  region,
  count(cpu.user)
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by minute, region;

