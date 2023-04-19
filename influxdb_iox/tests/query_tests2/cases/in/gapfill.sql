-- Gap-filling tests
-- IOX_SETUP: OneMeasurementTwoSeries

-- Input data (by region, time)
SELECT *
FROM cpu
ORDER BY REGION, TIME;

-- Input data (by time)
SELECT *
FROM cpu
ORDER BY TIME;

-- IOX_COMPARE: uuid
EXPLAIN SELECT
  date_bin_gapfill(interval '10 minute', time) as minute,
  count(cpu.user)
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by minute;

-- Gap filling with no other group keys
SELECT
  date_bin_gapfill(interval '10 minute', time) as minute,
  count(cpu.user)
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by minute;

-- Gap filling with no other group keys and no aggregates
SELECT
  date_bin_gapfill(interval '10 minute', time) as minute
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by minute;

-- gap filling with a group key
SELECT
  date_bin_gapfill(interval '10 minute', time) as minute,
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

-- gap filling with previous value using LOCF
-- IOX_COMPARE: uuid
EXPLAIN SELECT
  region,
  date_bin_gapfill(interval '10 minute', time) as minute,
  locf(avg(cpu.user))
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by region, minute;

SELECT
  region,
  date_bin_gapfill(interval '5 minute', time) as minute,
  locf(min(cpu.user))
from cpu
where time between timestamp '2000-05-05T12:15:00Z' and timestamp '2000-05-05T12:59:00Z'
group by region, minute;

-- cpu.idle has a null value at 12:31. It should propagate the value from 12:20 forward,
-- overwriting the null value.
SELECT
  date_bin_gapfill(interval '1 minute', time) as minute,
  locf(min(cpu.idle))
from cpu
where time between timestamp '2000-05-05T12:19:00Z' and timestamp '2000-05-05T12:40:00Z'
group by minute;

-- cpu.idle has a null value at 12:31. Interpolation should still occur,
-- overwriting the null value.
SELECT
  date_bin_gapfill(interval '4 minutes', time) as four_minute,
  interpolate(min(cpu.idle)),
  interpolate(min(cpu."user"))
from cpu
where time between timestamp '2000-05-05T12:19:00Z' and timestamp '2000-05-05T12:40:00Z'
group by four_minute;

