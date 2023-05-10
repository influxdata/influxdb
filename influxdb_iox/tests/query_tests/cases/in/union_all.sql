-- IOX_SETUP: TwoMeasurementsManyFields

-- Validate name resolution works for UNION ALL queries
-- IOX_COMPARE: sorted
SELECT state AS name FROM h2o UNION ALL SELECT city AS name FROM h2o;
