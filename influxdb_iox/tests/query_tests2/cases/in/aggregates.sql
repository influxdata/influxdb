-- IOX_SETUP: OneMeasurementWithTags

-- IOX_COMPARE: sorted
SELECT count(time), count(*), count(bar), min(bar), max(bar), min(time), max(time) FROM cpu;

-- IOX_COMPARE: sorted
SELECT max(foo) FROM cpu;

-- IOX_COMPARE: sorted
SELECT min(foo) FROM cpu;