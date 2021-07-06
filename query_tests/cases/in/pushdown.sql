-- Test for predicate push down explains
-- IOX_SETUP: TwoMeasurementsPredicatePushDown

-- Test 1: Select everything
EXPLAIN VERBOSE SELECT * from restaurant;

-- Test 2: One push-down expression: count > 200
-- TODO: Make push-down predicates shown in explain verbose. Ticket #1538
EXPLAIN VERBOSE SELECT * from restaurant where count > 200;

-- Test 2.2: One push-down expression: count > 200.0
EXPLAIN VERBOSE SELECT * from restaurant where count > 200.0;

-- Test 2.3: One push-down expression: system > 4.0
EXPLAIN VERBOSE SELECT * from restaurant where system > 4.0;


-- Test 3: Two push-down expression: count > 200 and town != 'tewsbury'
EXPLAIN VERBOSE SELECT * from restaurant where count > 200 and town != 'tewsbury';

-- Test 4: Still two push-down expression: count > 200 and town != 'tewsbury'
-- even though the results are different
EXPLAIN VERBOSE SELECT * from restaurant where count > 200 and town != 'tewsbury' and (system =5 or town = 'lawrence');

-- Test 5: three push-down expression: count > 200 and town != 'tewsbury' and count < 40000
EXPLAIN VERBOSE SELECT * from restaurant where count > 200 and town != 'tewsbury' and (system =5 or town = 'lawrence') and count < 40000;

-- Test 6: two push-down expression: count > 200 and count < 40000
EXPLAIN VERBOSE SELECT * from restaurant where count > 200  and count < 40000;

-- Test 7: two push-down expression on float: system > 4.0 and system < 7.0
EXPLAIN VERBOSE SELECT * from restaurant where system > 4.0 and system < 7.0;

-- Test 8: two push-down expression on float: system > 5.0 and system < 7.0
EXPLAIN VERBOSE SELECT * from restaurant where system > 5.0 and system < 7.0;

-- Test 9: three push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0
EXPLAIN VERBOSE SELECT * from restaurant where system > 5.0 and town != 'tewsbury' and 7.0 > system;

-- Test 10: three push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0
EXPLAIN VERBOSE SELECT * from restaurant where system > 5.0 and 'tewsbury' != town and system < 7.0 and (count = 632 or town = 'reading');

-- Test 11: four push-down expression: system > 5.0 and town != 'tewsbury' and system < 7.0 and
-- time > to_timestamp('1970-01-01T00:00:00.000000120+00:00') rewritten to time GT INT(130)
EXPLAIN VERBOSE SELECT * from restaurant where 5.0 < system and town != 'tewsbury' and system < 7.0 and (count = 632 or town = 'reading') and time > to_timestamp('1970-01-01T00:00:00.000000130+00:00');
