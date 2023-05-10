-- Date_bin tests
-- IOX_SETUP: OneMeasurementTwoSeries

-- CONSTANT DATA or ARRAY DATA

-- 1 month
select date_bin(INTERVAL '1 month', column1)
 from (values
   (timestamp '2022-01-01 00:00:00'),
   (timestamp '2022-01-01 01:00:00'),
   (timestamp '2022-01-02 00:00:00'),
   (timestamp '2022-02-02 00:00:00'),
   (timestamp '2022-02-15 00:00:00'),
   (timestamp '2022-03-31 00:00:00')
 ) as sq;

-- 1 year
 select date_bin('1 year', column1)
 from (values
   (timestamp '2022-01-01 00:00:00'),
   (timestamp '2023-01-01 01:00:00'),
   (timestamp '2022-01-02 00:00:00'),
   (timestamp '2022-02-02 00:00:00'),
   (timestamp '2022-02-15 00:00:00'),
   (timestamp '2022-03-31 00:00:00')
 ) as sq;

-- origin is last date of the month 1970-12-31T00:15:00Z and not at midnight
 select date_bin('1 month', column1, '1970-12-31T00:15:00Z')
 from (values
   (timestamp '2022-01-01 00:00:00'),
   (timestamp '2022-01-01 01:00:00'),
   (timestamp '2022-01-02 00:00:00'),
   (timestamp '2022-02-02 00:00:00'),
   (timestamp '2022-02-15 00:00:00'),
   (timestamp '2022-03-31 00:00:00')
 ) as sq;

 -- five months interval on constant
 SELECT DATE_BIN('5 month', '2022-01-01T00:00:00Z');

 -- origin is May 31 (last date of the month) to produce bin on Feb 28
 SELECT DATE_BIN('3 month', '2022-04-01T00:00:00Z', '2021-05-31T00:04:00Z');

-- origin is on Feb 29 and interval is one month. The bins will be:
--  # '2000-02-29T00:00:00'
--  # '2000-01-29T00:00:00'
--  # '1999-12-29T00:00:00'
--  # ....
--  # Reason: Even though 29 (or 28 for non-leap year) is the last date of Feb but it
--  # is not last date of other month. Months' chrono consider a month before or after that
--  # will land on the same 29th date.
select date_bin('1 month', timestamp '2000-01-31T00:00:00', timestamp '2000-02-29T00:00:00');

-- similar for the origin March 29
select date_bin('1 month', timestamp '2000-01-31T00:00:00', timestamp '2000-03-29T00:00:00');

-- 3 year 1 months = 37 months
SELECT DATE_BIN('3 years 1 months', '2022-09-01 00:00:00Z');

-- DATA FORM TABLE

-- Input data (by region, time)
SELECT *
FROM cpu
ORDER BY REGION, TIME;

-- Input data (by time)
SELECT *
FROM cpu
ORDER BY TIME;

-- 1 month
SELECT
  date_bin('1 month', time) as month,
  count(cpu.user)
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by month;

-- 1 month with origin
SELECT
  date_bin('1 month', time, '1970-12-31T00:15:00Z') as month,
  count(cpu.user)
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by month;

-- 3 months with origin on the last date of the month
select 
    date_bin('2 month', time, timestamp '2000-02-29T00:00:00') as month,
    count(cpu.user)
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by month;

-- EXPLAIN
-- IOX_COMPARE: uuid
EXPLAIN SELECT
  date_bin('1 month', time, '1970-12-31T00:15:00Z') as month,
  count(cpu.user)
from cpu
where time between timestamp '2000-05-05T12:00:00Z' and timestamp '2000-05-05T12:59:00Z'
group by month;