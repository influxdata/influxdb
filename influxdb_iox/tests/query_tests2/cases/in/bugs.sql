-- IOX_SETUP: Bugs

-- https://github.com/influxdata/influxdb_iox/issues/7644
-- internal error in select list
SELECT id FROM checks WHERE id in ('2','3','4','5');

SELECT id FROM checks WHERE id in ('1', '2','3','4','5');
