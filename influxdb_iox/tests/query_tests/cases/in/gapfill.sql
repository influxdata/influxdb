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

-- Missing time bounds
-- Expect to fail because missing both time bounds
SELECT
  region,
  date_bin_gapfill('10 minute', time) as minute,
  locf(avg(cpu.user))
from cpu
group by region, minute;

-- Expect to fail because missing upper time bound
SELECT
  region,
  date_bin_gapfill('10 minute', time) as minute,
  locf(avg(cpu.user))
from cpu
where time >= timestamp '2000-05-05T12:00:00Z'
group by region, minute;

-- Expect to fail because missing lower time bound
SELECT
  region,
  date_bin_gapfill('10 minute', time) as minute,
  locf(avg(cpu.user))
from cpu
where time < timestamp '2000-05-05T13:00:00Z'
group by region, minute;

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
  interpolate(min(cpu."user")),
  count(*)
from cpu
where time between timestamp '2000-05-05T12:19:00Z' and timestamp '2000-05-05T12:40:00Z'
group by four_minute;

-- A version of the above query that shows gap filling works with nanosecond precision.
SELECT
  date_bin_gapfill(interval '4 minutes 1 nanosecond', time, timestamp '2000-05-05T12:15:59.999999999') as four_minute,
  interpolate(min(cpu.idle)),
  interpolate(min(cpu."user")),
  count(*)
from cpu
where time between timestamp '2000-05-05T12:19:00Z' and timestamp '2000-05-05T12:44:00Z'
group by four_minute;

-- With an aliased aggregate column
SELECT
  region,
  date_bin_gapfill('10 minute', time) as minute,
  locf(avg(cpu.user)) as locf_avg_user
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by region, minute;

-- With a VALUES clause, which affects how the range is found
-- Fix for https://github.com/influxdata/idpe/issues/17880
SELECT
  date_bin_gapfill(INTERVAL '1 minute', time) as _time,
  pod,
  locf(selector_last(image, time))
FROM
  (VALUES ('2023-06-10T12:00:00Z'::timestamp, 'pod1', 'imageA'),
          ('2023-06-10T12:00:00Z'::timestamp, 'pod2', 'imageA'),
          ('2023-06-10T12:00:01Z'::timestamp, 'pod1', 'imageB'),
          ('2023-06-10T12:00:02Z'::timestamp, 'pod1', 'imageB'),
          ('2023-06-10T12:00:02Z'::timestamp, 'pod2', 'imageB')
  ) AS data(time, pod, image)
WHERE time >= timestamp '2023-06-10T11:55:00Z' AND time < timestamp '2023-06-10T12:05:00Z'
GROUP BY _time, pod;

-- This is not supported since the grouping is not on the values produced by
-- date_bin_gapfill. The query should fail with a reasonable message.
select
  date_bin_gapfill('60 seconds'::interval, time)::bigint as time,
  sum(idle)
from cpu
WHERE time >= '2020-06-11T16:52:00Z' AND time < '2020-06-11T16:54:00Z'
group by 1;

-- interpolation of selector functions.
SELECT
  date_bin_gapfill(interval '4 minutes', time) as four_minute,
  interpolate(selector_last(cpu.idle, time))['value'] as last,
  interpolate(selector_first(cpu.idle, time))['value'] as first,
  count(*)
from cpu
where time between timestamp '2000-05-05T12:19:00Z' and timestamp '2000-05-05T12:40:00Z'
group by four_minute;