-- Basic query tests for measurement names that have periods in their names
-- IOX_SETUP: PeriodsInNames

-- query data
SELECT * from "measurement.one";



-- projection
SELECT "tag.one" from "measurement.one";

-- predicate
SELECT "tag.one" from "measurement.one" where "field.two" is TRUE;
