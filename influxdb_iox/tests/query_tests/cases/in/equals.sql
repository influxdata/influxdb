-- Basic query tests for measurement names that have equals in their names
-- IOX_SETUP: EqualInMeasurements

-- query data
SELECT * from "measurement=one";



-- projection
SELECT tag from "measurement=one";
