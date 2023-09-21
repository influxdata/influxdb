-- IOX_SETUP: TwoMeasurementsManyNulls

-- Validate distinct aggregates work against dictionary columns which have nulls in them
-- IOX_COMPARE: sorted
SELECT count(DISTINCT city) FROM o2;

-- Validate aggregates work on dictionary columns which have nulls in them
-- IOX_COMPARE: sorted
SELECT count(*), city FROM o2 GROUP BY city;
