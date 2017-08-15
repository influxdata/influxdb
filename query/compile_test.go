package query_test

import (
	"math/rand"
	"testing"
	"time"

	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
)

// Second represents a helper for type converting durations.
const Second = int64(time.Second)

func TestCompile_Success(t *testing.T) {
	for _, tt := range []string{
		`SELECT time, value FROM cpu`,
		`SELECT value FROM cpu`,
		`SELECT value, host FROM cpu`,
		`SELECT * FROM cpu`,
		`SELECT time, * FROM cpu`,
		`SELECT value, * FROM cpu`,
		`SELECT max(value) FROM cpu`,
		`SELECT max(value), host FROM cpu`,
		`SELECT max(value), * FROM cpu`,
		`SELECT max(*) FROM cpu`,
		`SELECT max(/val/) FROM cpu`,
		`SELECT min(value) FROM cpu`,
		`SELECT min(value), host FROM cpu`,
		`SELECT min(value), * FROM cpu`,
		`SELECT min(*) FROM cpu`,
		`SELECT min(/val/) FROM cpu`,
		`SELECT first(value) FROM cpu`,
		`SELECT first(value), host FROM cpu`,
		`SELECT first(value), * FROM cpu`,
		`SELECT first(*) FROM cpu`,
		`SELECT first(/val/) FROM cpu`,
		`SELECT last(value) FROM cpu`,
		`SELECT last(value), host FROM cpu`,
		`SELECT last(value), * FROM cpu`,
		`SELECT last(*) FROM cpu`,
		`SELECT last(/val/) FROM cpu`,
		`SELECT count(value) FROM cpu`,
		`SELECT count(distinct(value)) FROM cpu`,
		`SELECT count(distinct value) FROM cpu`,
		`SELECT count(*) FROM cpu`,
		`SELECT count(/val/) FROM cpu`,
		`SELECT mean(value) FROM cpu`,
		`SELECT mean(*) FROM cpu`,
		`SELECT mean(/val/) FROM cpu`,
		`SELECT min(value), max(value) FROM cpu`,
		`SELECT min(*), max(*) FROM cpu`,
		`SELECT min(/val/), max(/val/) FROM cpu`,
		`SELECT first(value), last(value) FROM cpu`,
		`SELECT first(*), last(*) FROM cpu`,
		`SELECT first(/val/), last(/val/) FROM cpu`,
		`SELECT count(value) FROM cpu WHERE time >= now() - 1h GROUP BY time(10m)`,
		`SELECT distinct value FROM cpu`,
		`SELECT distinct(value) FROM cpu`,
		`SELECT value / total FROM cpu`,
		`SELECT min(value) / total FROM cpu`,
		`SELECT max(value) / total FROM cpu`,
		`SELECT top(value, 1) FROM cpu`,
		`SELECT top(value, host, 1) FROM cpu`,
		`SELECT top(value, 1), host FROM cpu`,
		`SELECT min(top) FROM (SELECT top(value, host, 1) FROM cpu) GROUP BY region`,
		`SELECT bottom(value, 1) FROM cpu`,
		`SELECT bottom(value, host, 1) FROM cpu`,
		`SELECT bottom(value, 1), host FROM cpu`,
		`SELECT max(bottom) FROM (SELECT bottom(value, host, 1) FROM cpu) GROUP BY region`,
		`SELECT percentile(value, 75) FROM cpu`,
		`SELECT percentile(value, 75.0) FROM cpu`,
		`SELECT sample(value, 2) FROM cpu`,
		`SELECT sample(*, 2) FROM cpu`,
		`SELECT sample(/val/, 2) FROM cpu`,
		`SELECT elapsed(value) FROM cpu`,
		`SELECT elapsed(value, 10s) FROM cpu`,
		`SELECT integral(value) FROM cpu`,
		`SELECT integral(value, 10s) FROM cpu`,
		`SELECT max(value) FROM cpu WHERE time >= now() - 1m GROUP BY time(10s, 5s)`,
		`SELECT max(value) FROM cpu WHERE time >= now() - 1m GROUP BY time(10s, '2000-01-01T00:00:05Z')`,
		`SELECT max(value) FROM cpu WHERE time >= now() - 1m GROUP BY time(10s, now())`,
		`SELECT max(mean) FROM (SELECT mean(value) FROM cpu GROUP BY host)`,
		`SELECT max(derivative) FROM (SELECT derivative(mean(value)) FROM cpu) WHERE time >= now() - 1m GROUP BY time(10s)`,
		`SELECT max(value) FROM (SELECT value + total FROM cpu) WHERE time >= now() - 1m GROUP BY time(10s)`,
		`SELECT value FROM cpu WHERE time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T01:00:00Z'`,
	} {
		t.Run(tt, func(t *testing.T) {
			stmt, err := influxql.ParseStatement(tt)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			s := stmt.(*influxql.SelectStatement)

			opt := query.CompileOptions{}
			if _, err := query.Compile(s, opt); err != nil {
				t.Errorf("unexpected error: %s", err)
			}
		})
	}
}

func TestCompile_Failures(t *testing.T) {
	for _, tt := range []struct {
		s   string
		err string
	}{
		{s: `SELECT time FROM cpu`, err: `at least 1 non-time field must be queried`},
		{s: `SELECT value, mean(value) FROM cpu`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT value, max(value), min(value) FROM cpu`, err: `mixing multiple selector functions with tags or fields is not supported`},
		{s: `SELECT top(value, 10), max(value) FROM cpu`, err: `selector function top() cannot be combined with other functions`},
		{s: `SELECT bottom(value, 10), max(value) FROM cpu`, err: `selector function bottom() cannot be combined with other functions`},
		{s: `SELECT count() FROM cpu`, err: `invalid number of arguments for count, expected 1, got 0`},
		{s: `SELECT count(value, host) FROM cpu`, err: `invalid number of arguments for count, expected 1, got 2`},
		{s: `SELECT min() FROM cpu`, err: `invalid number of arguments for min, expected 1, got 0`},
		{s: `SELECT min(value, host) FROM cpu`, err: `invalid number of arguments for min, expected 1, got 2`},
		{s: `SELECT max() FROM cpu`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT max(value, host) FROM cpu`, err: `invalid number of arguments for max, expected 1, got 2`},
		{s: `SELECT sum() FROM cpu`, err: `invalid number of arguments for sum, expected 1, got 0`},
		{s: `SELECT sum(value, host) FROM cpu`, err: `invalid number of arguments for sum, expected 1, got 2`},
		{s: `SELECT first() FROM cpu`, err: `invalid number of arguments for first, expected 1, got 0`},
		{s: `SELECT first(value, host) FROM cpu`, err: `invalid number of arguments for first, expected 1, got 2`},
		{s: `SELECT last() FROM cpu`, err: `invalid number of arguments for last, expected 1, got 0`},
		{s: `SELECT last(value, host) FROM cpu`, err: `invalid number of arguments for last, expected 1, got 2`},
		{s: `SELECT mean() FROM cpu`, err: `invalid number of arguments for mean, expected 1, got 0`},
		{s: `SELECT mean(value, host) FROM cpu`, err: `invalid number of arguments for mean, expected 1, got 2`},
		{s: `SELECT distinct(value), max(value) FROM cpu`, err: `aggregate function distinct() cannot be combined with other functions or fields`},
		{s: `SELECT count(distinct(value)), max(value) FROM cpu`, err: `aggregate function distinct() cannot be combined with other functions or fields`},
		{s: `SELECT count(distinct()) FROM cpu`, err: `distinct function requires at least one argument`},
		{s: `SELECT count(distinct(value, host)) FROM cpu`, err: `distinct function can only have one argument`},
		{s: `SELECT count(distinct(2)) FROM cpu`, err: `expected field argument in distinct()`},
		{s: `SELECT value FROM cpu GROUP BY now()`, err: `only time() calls allowed in dimensions`},
		{s: `SELECT value FROM cpu GROUP BY time()`, err: `time dimension expected 1 or 2 arguments`},
		{s: `SELECT value FROM cpu GROUP BY time(5m, 30s, 1ms)`, err: `time dimension expected 1 or 2 arguments`},
		{s: `SELECT value FROM cpu GROUP BY time('unexpected')`, err: `time dimension must have duration argument`},
		{s: `SELECT value FROM cpu GROUP BY time(5m), time(1m)`, err: `multiple time dimensions not allowed`},
		{s: `SELECT value FROM cpu GROUP BY time(5m, unexpected())`, err: `time dimension offset function must be now()`},
		{s: `SELECT value FROM cpu GROUP BY time(5m, now(1m))`, err: `time dimension offset now() function requires no arguments`},
		{s: `SELECT value FROM cpu GROUP BY time(5m, 'unexpected')`, err: `time dimension offset must be duration or now()`},
		{s: `SELECT value FROM cpu GROUP BY 'unexpected'`, err: `only time and tag dimensions allowed`},
		{s: `SELECT top(value) FROM cpu`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT top('unexpected', 5) FROM cpu`, err: `expected first argument to be a field in top(), found 'unexpected'`},
		{s: `SELECT top(value, 'unexpected', 5) FROM cpu`, err: `only fields or tags are allowed in top(), found 'unexpected'`},
		{s: `SELECT top(value, 2.5) FROM cpu`, err: `expected integer as last argument in top(), found 2.500`},
		{s: `SELECT top(value, -1) FROM cpu`, err: `limit (-1) in top function must be at least 1`},
		{s: `SELECT top(value, 3) FROM cpu LIMIT 2`, err: `limit (3) in top function can not be larger than the LIMIT (2) in the select statement`},
		{s: `SELECT bottom(value) FROM cpu`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT bottom('unexpected', 5) FROM cpu`, err: `expected first argument to be a field in bottom(), found 'unexpected'`},
		{s: `SELECT bottom(value, 'unexpected', 5) FROM cpu`, err: `only fields or tags are allowed in bottom(), found 'unexpected'`},
		{s: `SELECT bottom(value, 2.5) FROM cpu`, err: `expected integer as last argument in bottom(), found 2.500`},
		{s: `SELECT bottom(value, -1) FROM cpu`, err: `limit (-1) in bottom function must be at least 1`},
		{s: `SELECT bottom(value, 3) FROM cpu LIMIT 2`, err: `limit (3) in bottom function can not be larger than the LIMIT (2) in the select statement`},
		{s: `SELECT value FROM cpu WHERE time >= now() - 10m OR time < now() - 5m`, err: `cannot use OR with time conditions`},
		{s: `SELECT value FROM cpu WHERE value`, err: `invalid condition expression: value`},
		{s: `SELECT count(value), * FROM cpu`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT max(*), host FROM cpu`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT count(value), /ho/ FROM cpu`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT max(/val/), * FROM cpu`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT a(value) FROM cpu`, err: `undefined function a()`},
		{s: `SELECT count(max(value)) FROM myseries`, err: `expected field argument in count()`},
		{s: `SELECT count(distinct('value')) FROM myseries`, err: `expected field argument in distinct()`},
		{s: `SELECT distinct('value') FROM myseries`, err: `expected field argument in distinct()`},
		{s: `SELECT min(max(value)) FROM myseries`, err: `expected field argument in min()`},
		{s: `SELECT min(distinct(value)) FROM myseries`, err: `expected field argument in min()`},
		{s: `SELECT max(max(value)) FROM myseries`, err: `expected field argument in max()`},
		{s: `SELECT sum(max(value)) FROM myseries`, err: `expected field argument in sum()`},
		{s: `SELECT first(max(value)) FROM myseries`, err: `expected field argument in first()`},
		{s: `SELECT last(max(value)) FROM myseries`, err: `expected field argument in last()`},
		{s: `SELECT mean(max(value)) FROM myseries`, err: `expected field argument in mean()`},
		{s: `SELECT median(max(value)) FROM myseries`, err: `expected field argument in median()`},
		{s: `SELECT mode(max(value)) FROM myseries`, err: `expected field argument in mode()`},
		{s: `SELECT stddev(max(value)) FROM myseries`, err: `expected field argument in stddev()`},
		{s: `SELECT spread(max(value)) FROM myseries`, err: `expected field argument in spread()`},
		{s: `SELECT top() FROM myseries`, err: `invalid number of arguments for top, expected at least 2, got 0`},
		{s: `SELECT top(field1) FROM myseries`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT top(field1,foo) FROM myseries`, err: `expected integer as last argument in top(), found foo`},
		{s: `SELECT top(field1,host,'server',foo) FROM myseries`, err: `expected integer as last argument in top(), found foo`},
		{s: `SELECT top(field1,5,'server',2) FROM myseries`, err: `only fields or tags are allowed in top(), found 5`},
		{s: `SELECT top(field1,max(foo),'server',2) FROM myseries`, err: `only fields or tags are allowed in top(), found max(foo)`},
		{s: `SELECT top(value, 10) + count(value) FROM myseries`, err: `selector function top() cannot be combined with other functions`},
		{s: `SELECT top(max(value), 10) FROM myseries`, err: `expected first argument to be a field in top(), found max(value)`},
		{s: `SELECT bottom() FROM myseries`, err: `invalid number of arguments for bottom, expected at least 2, got 0`},
		{s: `SELECT bottom(field1) FROM myseries`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT bottom(field1,foo) FROM myseries`, err: `expected integer as last argument in bottom(), found foo`},
		{s: `SELECT bottom(field1,host,'server',foo) FROM myseries`, err: `expected integer as last argument in bottom(), found foo`},
		{s: `SELECT bottom(field1,5,'server',2) FROM myseries`, err: `only fields or tags are allowed in bottom(), found 5`},
		{s: `SELECT bottom(field1,max(foo),'server',2) FROM myseries`, err: `only fields or tags are allowed in bottom(), found max(foo)`},
		{s: `SELECT bottom(value, 10) + count(value) FROM myseries`, err: `selector function bottom() cannot be combined with other functions`},
		{s: `SELECT bottom(max(value), 10) FROM myseries`, err: `expected first argument to be a field in bottom(), found max(value)`},
		{s: `SELECT top(value, 10), bottom(value, 10) FROM cpu`, err: `selector function top() cannot be combined with other functions`},
		{s: `SELECT bottom(value, 10), top(value, 10) FROM cpu`, err: `selector function bottom() cannot be combined with other functions`},
		{s: `SELECT sample(value) FROM myseries`, err: `invalid number of arguments for sample, expected 2, got 1`},
		{s: `SELECT sample(value, 2, 3) FROM myseries`, err: `invalid number of arguments for sample, expected 2, got 3`},
		{s: `SELECT sample(value, 0) FROM myseries`, err: `sample window must be greater than 1, got 0`},
		{s: `SELECT sample(value, 2.5) FROM myseries`, err: `expected integer argument in sample()`},
		{s: `SELECT percentile() FROM myseries`, err: `invalid number of arguments for percentile, expected 2, got 0`},
		{s: `SELECT percentile(field1) FROM myseries`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT percentile(field1, foo) FROM myseries`, err: `expected float argument in percentile()`},
		{s: `SELECT percentile(max(field1), 75) FROM myseries`, err: `expected field argument in percentile()`},
		{s: `SELECT field1 FROM foo group by time(1s)`, err: `GROUP BY requires at least one aggregate function`},
		{s: `SELECT field1 FROM foo fill(none)`, err: `fill(none) must be used with a function`},
		{s: `SELECT field1 FROM foo fill(linear)`, err: `fill(linear) must be used with a function`},
		{s: `SELECT count(value), value FROM foo`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT count(value) FROM foo group by time(1s)`, err: `aggregate functions with GROUP BY time require a WHERE time clause with a lower limit`},
		{s: `SELECT count(value) FROM foo group by time(500ms)`, err: `aggregate functions with GROUP BY time require a WHERE time clause with a lower limit`},
		{s: `SELECT count(value) FROM foo group by time(1s) where host = 'hosta.influxdb.org'`, err: `aggregate functions with GROUP BY time require a WHERE time clause with a lower limit`},
		{s: `SELECT count(value) FROM foo group by time(1s) where time < now()`, err: `aggregate functions with GROUP BY time require a WHERE time clause with a lower limit`},
		{s: `SELECT count(value) FROM foo group by time`, err: `time() is a function and expects at least one argument`},
		{s: `SELECT count(value) FROM foo group by 'time'`, err: `only time and tag dimensions allowed`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time()`, err: `time dimension expected 1 or 2 arguments`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time(b)`, err: `time dimension must have duration argument`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time(1s), time(2s)`, err: `multiple time dimensions not allowed`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time(1s, b)`, err: `time dimension offset must be duration or now()`},
		{s: `SELECT count(value) FROM foo where time > now() and time < now() group by time(1s, '5s')`, err: `time dimension offset must be duration or now()`},
		{s: `SELECT distinct(field1), sum(field1) FROM myseries`, err: `aggregate function distinct() cannot be combined with other functions or fields`},
		{s: `SELECT distinct(field1), field2 FROM myseries`, err: `aggregate function distinct() cannot be combined with other functions or fields`},
		{s: `SELECT distinct(field1, field2) FROM myseries`, err: `distinct function can only have one argument`},
		{s: `SELECT distinct() FROM myseries`, err: `distinct function requires at least one argument`},
		{s: `SELECT distinct field1, field2 FROM myseries`, err: `aggregate function distinct() cannot be combined with other functions or fields`},
		{s: `SELECT count(distinct field1, field2) FROM myseries`, err: `invalid number of arguments for count, expected 1, got 2`},
		{s: `select count(distinct(too, many, arguments)) from myseries`, err: `distinct function can only have one argument`},
		{s: `select count() from myseries`, err: `invalid number of arguments for count, expected 1, got 0`},
		{s: `SELECT derivative(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `select derivative() from myseries`, err: `invalid number of arguments for derivative, expected at least 1 but no more than 2, got 0`},
		{s: `select derivative(mean(value), 1h, 3) from myseries`, err: `invalid number of arguments for derivative, expected at least 1 but no more than 2, got 3`},
		{s: `SELECT derivative(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to derivative`},
		{s: `SELECT derivative(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT derivative(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT derivative(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT derivative(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT derivative(mean(value), 1h) FROM myseries where time < now() and time > now() - 1d`, err: `derivative aggregate requires a GROUP BY interval`},
		{s: `SELECT derivative(value, -2h) FROM myseries`, err: `duration argument must be positive, got -2h`},
		{s: `SELECT derivative(value, 10) FROM myseries`, err: `second argument to derivative must be a duration, got *influxql.IntegerLiteral`},
		{s: `SELECT derivative(f, true) FROM myseries`, err: `second argument to derivative must be a duration, got *influxql.BooleanLiteral`},
		{s: `SELECT non_negative_derivative(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `select non_negative_derivative() from myseries`, err: `invalid number of arguments for non_negative_derivative, expected at least 1 but no more than 2, got 0`},
		{s: `select non_negative_derivative(mean(value), 1h, 3) from myseries`, err: `invalid number of arguments for non_negative_derivative, expected at least 1 but no more than 2, got 3`},
		{s: `SELECT non_negative_derivative(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to non_negative_derivative`},
		{s: `SELECT non_negative_derivative(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT non_negative_derivative(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT non_negative_derivative(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT non_negative_derivative(mean(value), 1h) FROM myseries where time < now() and time > now() - 1d`, err: `non_negative_derivative aggregate requires a GROUP BY interval`},
		{s: `SELECT non_negative_derivative(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT non_negative_derivative(value, -2h) FROM myseries`, err: `duration argument must be positive, got -2h`},
		{s: `SELECT non_negative_derivative(value, 10) FROM myseries`, err: `second argument to non_negative_derivative must be a duration, got *influxql.IntegerLiteral`},
		{s: `SELECT difference(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT difference() from myseries`, err: `invalid number of arguments for difference, expected 1, got 0`},
		{s: `SELECT difference(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to difference`},
		{s: `SELECT difference(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT difference(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT difference(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT difference(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT difference(mean(value)) FROM myseries where time < now() and time > now() - 1d`, err: `difference aggregate requires a GROUP BY interval`},
		{s: `SELECT non_negative_difference(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT non_negative_difference() from myseries`, err: `invalid number of arguments for non_negative_difference, expected 1, got 0`},
		{s: `SELECT non_negative_difference(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to non_negative_difference`},
		{s: `SELECT non_negative_difference(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT non_negative_difference(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT non_negative_difference(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT non_negative_difference(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT non_negative_difference(mean(value)) FROM myseries where time < now() and time > now() - 1d`, err: `non_negative_difference aggregate requires a GROUP BY interval`},
		{s: `SELECT elapsed() FROM myseries`, err: `invalid number of arguments for elapsed, expected at least 1 but no more than 2, got 0`},
		{s: `SELECT elapsed(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to elapsed`},
		{s: `SELECT elapsed(value, 1s, host) FROM myseries`, err: `invalid number of arguments for elapsed, expected at least 1 but no more than 2, got 3`},
		{s: `SELECT elapsed(value, 0s) FROM myseries`, err: `duration argument must be positive, got 0s`},
		{s: `SELECT elapsed(value, -10s) FROM myseries`, err: `duration argument must be positive, got -10s`},
		{s: `SELECT elapsed(value, 10) FROM myseries`, err: `second argument to elapsed must be a duration, got *influxql.IntegerLiteral`},
		{s: `SELECT elapsed(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT elapsed(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT elapsed(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT elapsed(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT elapsed(mean(value)) FROM myseries where time < now() and time > now() - 1d`, err: `elapsed aggregate requires a GROUP BY interval`},
		{s: `SELECT moving_average(field1, 2), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT moving_average(field1, 1), field1 FROM myseries`, err: `moving_average window must be greater than 1, got 1`},
		{s: `SELECT moving_average(field1, 0), field1 FROM myseries`, err: `moving_average window must be greater than 1, got 0`},
		{s: `SELECT moving_average(field1, -1), field1 FROM myseries`, err: `moving_average window must be greater than 1, got -1`},
		{s: `SELECT moving_average(field1, 2.0), field1 FROM myseries`, err: `second argument for moving_average must be an integer, got *influxql.NumberLiteral`},
		{s: `SELECT moving_average() from myseries`, err: `invalid number of arguments for moving_average, expected 2, got 0`},
		{s: `SELECT moving_average(value) FROM myseries`, err: `invalid number of arguments for moving_average, expected 2, got 1`},
		{s: `SELECT moving_average(value, 2) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to moving_average`},
		{s: `SELECT moving_average(top(value), 2) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT moving_average(bottom(value), 2) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT moving_average(max(), 2) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT moving_average(percentile(value), 2) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT moving_average(mean(value), 2) FROM myseries where time < now() and time > now() - 1d`, err: `moving_average aggregate requires a GROUP BY interval`},
		{s: `SELECT cumulative_sum(field1), field1 FROM myseries`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `SELECT cumulative_sum() from myseries`, err: `invalid number of arguments for cumulative_sum, expected 1, got 0`},
		{s: `SELECT cumulative_sum(value) FROM myseries group by time(1h)`, err: `aggregate function required inside the call to cumulative_sum`},
		{s: `SELECT cumulative_sum(top(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for top, expected at least 2, got 1`},
		{s: `SELECT cumulative_sum(bottom(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for bottom, expected at least 2, got 1`},
		{s: `SELECT cumulative_sum(max()) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for max, expected 1, got 0`},
		{s: `SELECT cumulative_sum(percentile(value)) FROM myseries where time < now() and time > now() - 1d group by time(1h)`, err: `invalid number of arguments for percentile, expected 2, got 1`},
		{s: `SELECT cumulative_sum(mean(value)) FROM myseries where time < now() and time > now() - 1d`, err: `cumulative_sum aggregate requires a GROUP BY interval`},
		{s: `SELECT integral() FROM myseries`, err: `invalid number of arguments for integral, expected at least 1 but no more than 2, got 0`},
		{s: `SELECT integral(value, 10s, host) FROM myseries`, err: `invalid number of arguments for integral, expected at least 1 but no more than 2, got 3`},
		{s: `SELECT integral(value, -10s) FROM myseries`, err: `duration argument must be positive, got -10s`},
		{s: `SELECT integral(value, 10) FROM myseries`, err: `second argument must be a duration`},
		{s: `SELECT holt_winters(value) FROM myseries where time < now() and time > now() - 1d`, err: `invalid number of arguments for holt_winters, expected 3, got 1`},
		{s: `SELECT holt_winters(value, 10, 2) FROM myseries where time < now() and time > now() - 1d`, err: `must use aggregate function with holt_winters`},
		{s: `SELECT holt_winters(min(value), 10, 2) FROM myseries where time < now() and time > now() - 1d`, err: `holt_winters aggregate requires a GROUP BY interval`},
		{s: `SELECT holt_winters(min(value), 0, 2) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `second arg to holt_winters must be greater than 0, got 0`},
		{s: `SELECT holt_winters(min(value), false, 2) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `expected integer argument as second arg in holt_winters`},
		{s: `SELECT holt_winters(min(value), 10, 'string') FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `expected integer argument as third arg in holt_winters`},
		{s: `SELECT holt_winters(min(value), 10, -1) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `third arg to holt_winters cannot be negative, got -1`},
		{s: `SELECT holt_winters_with_fit(value) FROM myseries where time < now() and time > now() - 1d`, err: `invalid number of arguments for holt_winters_with_fit, expected 3, got 1`},
		{s: `SELECT holt_winters_with_fit(value, 10, 2) FROM myseries where time < now() and time > now() - 1d`, err: `must use aggregate function with holt_winters_with_fit`},
		{s: `SELECT holt_winters_with_fit(min(value), 10, 2) FROM myseries where time < now() and time > now() - 1d`, err: `holt_winters_with_fit aggregate requires a GROUP BY interval`},
		{s: `SELECT holt_winters_with_fit(min(value), 0, 2) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `second arg to holt_winters_with_fit must be greater than 0, got 0`},
		{s: `SELECT holt_winters_with_fit(min(value), false, 2) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `expected integer argument as second arg in holt_winters_with_fit`},
		{s: `SELECT holt_winters_with_fit(min(value), 10, 'string') FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `expected integer argument as third arg in holt_winters_with_fit`},
		{s: `SELECT holt_winters_with_fit(min(value), 10, -1) FROM myseries where time < now() and time > now() - 1d GROUP BY time(1d)`, err: `third arg to holt_winters_with_fit cannot be negative, got -1`},
		{s: `SELECT mean(value) + value FROM cpu WHERE time < now() and time > now() - 1h GROUP BY time(10m)`, err: `mixing aggregate and non-aggregate queries is not supported`},
		// TODO: Remove this restriction in the future: https://github.com/influxdata/influxdb/issues/5968
		{s: `SELECT mean(cpu_total - cpu_idle) FROM cpu`, err: `expected field argument in mean()`},
		{s: `SELECT derivative(mean(cpu_total - cpu_idle), 1s) FROM cpu WHERE time < now() AND time > now() - 1d GROUP BY time(1h)`, err: `expected field argument in mean()`},
		// TODO: The error message will change when math is allowed inside an aggregate: https://github.com/influxdata/influxdb/pull/5990#issuecomment-195565870
		{s: `SELECT count(foo + sum(bar)) FROM cpu`, err: `expected field argument in count()`},
		{s: `SELECT (count(foo + sum(bar))) FROM cpu`, err: `expected field argument in count()`},
		{s: `SELECT sum(value) + count(foo + sum(bar)) FROM cpu`, err: `expected field argument in count()`},
		{s: `SELECT sum(mean) FROM (SELECT mean(value) FROM cpu GROUP BY time(1h))`, err: `aggregate functions with GROUP BY time require a WHERE time clause with a lower limit`},
		{s: `SELECT top(value, 2), max(value) FROM cpu`, err: `selector function top() cannot be combined with other functions`},
		{s: `SELECT bottom(value, 2), max(value) FROM cpu`, err: `selector function bottom() cannot be combined with other functions`},
		{s: `SELECT min(derivative) FROM (SELECT derivative(mean(value), 1h) FROM myseries) where time < now() and time > now() - 1d`, err: `derivative aggregate requires a GROUP BY interval`},
		{s: `SELECT min(mean) FROM (SELECT mean(value) FROM myseries GROUP BY time)`, err: `time() is a function and expects at least one argument`},
		{s: `SELECT value FROM myseries WHERE value OR time >= now() - 1m`, err: `invalid condition expression: value`},
		{s: `SELECT value FROM myseries WHERE time >= now() - 1m OR value`, err: `invalid condition expression: value`},
	} {
		t.Run(tt.s, func(t *testing.T) {
			stmt, err := influxql.ParseStatement(tt.s)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			s := stmt.(*influxql.SelectStatement)

			opt := query.CompileOptions{}
			if _, err := query.Compile(s, opt); err == nil {
				t.Error("expected error")
			} else if have, want := err.Error(), tt.err; have != want {
				t.Errorf("unexpected error: %s != %s", have, want)
			}
		})
	}
}

func TestCompile_ColumnNames(t *testing.T) {
	mustParseTime := func(value string) time.Time {
		ts, err := time.Parse(time.RFC3339, value)
		if err != nil {
			t.Fatalf("unable to parse time: %s", err)
		}
		return ts
	}

	now := mustParseTime("2000-01-01T00:00:00Z")
	for _, tt := range []struct {
		s       string
		columns []string
	}{
		{s: `SELECT field1 FROM cpu`, columns: []string{"time", "field1"}},
		{s: `SELECT field1, field1, field1_1 FROM cpu`, columns: []string{"time", "field1", "field1_1", "field1_1_1"}},
		{s: `SELECT field1, field1_1, field1 FROM cpu`, columns: []string{"time", "field1", "field1_1", "field1_2"}},
		{s: `SELECT field1, total AS field1, field1 FROM cpu`, columns: []string{"time", "field1_1", "field1", "field1_2"}},
		{s: `SELECT time AS timestamp, field1 FROM cpu`, columns: []string{"timestamp", "field1"}},
		{s: `SELECT mean(field1) FROM cpu`, columns: []string{"time", "mean"}},
		{s: `SELECT * FROM cpu`, columns: []string{"time", "field1", "field2"}},
		{s: `SELECT /2/ FROM cpu`, columns: []string{"time", "field2"}},
		{s: `SELECT mean(*) FROM cpu`, columns: []string{"time", "mean_field1", "mean_field2"}},
		{s: `SELECT mean(/2/) FROM cpu`, columns: []string{"time", "mean_field2"}},
		{s: `SELECT time AS field1, field1 FROM cpu`, columns: []string{"field1", "field1_1"}},
	} {
		t.Run(tt.s, func(t *testing.T) {
			stmt, err := influxql.ParseStatement(tt.s)
			if err != nil {
				t.Fatalf("unable to parse statement: %s", err)
			}

			opt := query.CompileOptions{Now: now}
			c, err := query.Compile(stmt.(*influxql.SelectStatement), opt)
			if err != nil {
				t.Fatalf("unable to compile statement: %s", err)
			}

			linker := mock.NewShardMapper(func(stub *mock.ShardMapper) {
				stub.ShardsByTimeRangeFn = func(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
					return []meta.ShardInfo{{ID: 1}}, nil
				}
				stub.ShardGroupFn = func(ids []uint64) query.ShardGroup {
					if diff := cmp.Diff(ids, []uint64{1}); diff != "" {
						t.Fatalf("unexpected shard ids:\n%s", diff)
					}
					return &mock.ShardGroup{
						Measurements: map[string]mock.ShardMeta{
							"cpu": {
								Fields: map[string]influxql.DataType{
									"field1": influxql.Float,
									"field2": influxql.Float,
								},
							},
						},
					}
				}
			})
			if _, columns, err := c.Select(linker); err != nil {
				t.Fatalf("unable to link statement: %s", err)
			} else if diff := cmp.Diff(tt.columns, columns); diff != "" {
				t.Fatalf("unexpected columns:\n%s", diff)
			}
		})
	}
}

func TestCompile_ColumnTypes(t *testing.T) {
	now, err := time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
	if err != nil {
		t.Fatalf("unable to parse time: %s", err)
	}

	for _, tt := range []struct {
		s       string
		columns []string
	}{
		{s: `SELECT field1 FROM cpu`, columns: []string{"time", "field1"}},
		{s: `SELECT field1, field1, field1_1 FROM cpu`, columns: []string{"time", "field1", "field1_1", "field1_1_1"}},
		{s: `SELECT field1, field1_1, field1 FROM cpu`, columns: []string{"time", "field1", "field1_1", "field1_2"}},
		{s: `SELECT field1, total AS field1, field1 FROM cpu`, columns: []string{"time", "field1_1", "field1", "field1_2"}},
		{s: `SELECT time AS timestamp, field1 FROM cpu`, columns: []string{"timestamp", "field1"}},
		{s: `SELECT mean(field1) FROM cpu`, columns: []string{"time", "mean"}},
		{s: `SELECT * FROM cpu`, columns: []string{"time", "field1", "field2"}},
		{s: `SELECT /2/ FROM cpu`, columns: []string{"time", "field2"}},
		{s: `SELECT mean(*) FROM cpu`, columns: []string{"time", "mean_field1", "mean_field2"}},
		{s: `SELECT mean(/2/) FROM cpu`, columns: []string{"time", "mean_field2"}},
		{s: `SELECT time AS field1, field1 FROM cpu`, columns: []string{"field1", "field1_1"}},
	} {
		t.Run(tt.s, func(t *testing.T) {
			stmt, err := influxql.ParseStatement(tt.s)
			if err != nil {
				t.Fatalf("unable to parse statement: %s", err)
			}

			opt := query.CompileOptions{Now: now}
			c, err := query.Compile(stmt.(*influxql.SelectStatement), opt)
			if err != nil {
				t.Fatalf("unable to compile statement: %s", err)
			}

			linker := mock.NewShardMapper(func(stub *mock.ShardMapper) {
				stub.ShardsByTimeRangeFn = func(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
					return []meta.ShardInfo{{ID: 1}}, nil
				}
				stub.ShardGroupFn = func(ids []uint64) query.ShardGroup {
					if diff := cmp.Diff(ids, []uint64{1}); diff != "" {
						t.Fatalf("unexpected shard ids:\n%s", diff)
					}
					return &mock.ShardGroup{
						Measurements: map[string]mock.ShardMeta{
							"cpu": {
								Fields: map[string]influxql.DataType{
									"field1": influxql.Float,
									"field2": influxql.Float,
								},
							},
						},
					}
				}
			})
			if _, columns, err := c.Select(linker); err != nil {
				t.Fatalf("unable to link statement: %s", err)
			} else if diff := cmp.Diff(tt.columns, columns); diff != "" {
				t.Fatalf("unexpected columns:\n%s", diff)
			}
		})
	}
}

func TestParseCondition(t *testing.T) {
	mustParseTime := func(value string) time.Time {
		ts, err := time.Parse(time.RFC3339, value)
		if err != nil {
			t.Fatalf("unable to parse time: %s", err)
		}
		return ts
	}
	now := mustParseTime("2000-01-01T00:00:00Z")
	valuer := influxql.NowValuer{Now: now}

	for _, tt := range []struct {
		s        string
		cond     string
		min, max time.Time
		err      string
	}{
		{s: `host = 'server01'`, cond: `host = 'server01'`},
		{s: `time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T01:00:00Z'`,
			min: mustParseTime("2000-01-01T00:00:00Z"),
			max: mustParseTime("2000-01-01T01:00:00Z").Add(-1)},
		{s: `host = 'server01' AND (region = 'uswest' AND time >= now() - 10m)`,
			cond: `host = 'server01' AND (region = 'uswest')`,
			min:  mustParseTime("1999-12-31T23:50:00Z")},
		{s: `(host = 'server01' AND region = 'uswest') AND time >= now() - 10m`,
			cond: `(host = 'server01' AND region = 'uswest')`,
			min:  mustParseTime("1999-12-31T23:50:00Z")},
		{s: `host = 'server01' AND (time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T01:00:00Z')`,
			cond: `host = 'server01'`,
			min:  mustParseTime("2000-01-01T00:00:00Z"),
			max:  mustParseTime("2000-01-01T01:00:00Z").Add(-1)},
		{s: `(time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T01:00:00Z') AND host = 'server01'`,
			cond: `host = 'server01'`,
			min:  mustParseTime("2000-01-01T00:00:00Z"),
			max:  mustParseTime("2000-01-01T01:00:00Z").Add(-1)},
		{s: `'2000-01-01T00:00:00Z' <= time AND '2000-01-01T01:00:00Z' > time`,
			min: mustParseTime("2000-01-01T00:00:00Z"),
			max: mustParseTime("2000-01-01T01:00:00Z").Add(-1)},
		{s: `'2000-01-01T00:00:00Z' < time AND '2000-01-01T01:00:00Z' >= time`,
			min: mustParseTime("2000-01-01T00:00:00Z").Add(1),
			max: mustParseTime("2000-01-01T01:00:00Z")},
		{s: `time = '2000-01-01T00:00:00Z'`,
			min: mustParseTime("2000-01-01T00:00:00Z"),
			max: mustParseTime("2000-01-01T00:00:00Z")},
		{s: `time >= 10s`, min: mustParseTime("1970-01-01T00:00:10Z")},
		{s: `time >= 10000000000`, min: mustParseTime("1970-01-01T00:00:10Z")},
		{s: `time >= 10000000000.0`, min: mustParseTime("1970-01-01T00:00:10Z")},
		{s: `time > now()`, min: now.Add(1)},
		{s: `value`, err: `invalid condition expression: value`},
		{s: `4`, err: `invalid condition expression: 4`},
		{s: `time >= 'today'`, err: `invalid operation: time and *influxql.StringLiteral are not compatible`},
		{s: `time != '2000-01-01T00:00:00Z'`, err: `invalid time comparison operator: !=`},
		{s: `host = 'server01' OR (time >= now() - 10m AND host = 'server02')`, err: `cannot use OR with time conditions`},
		{s: `value AND host = 'server01'`, err: `invalid condition expression: value`},
		{s: `host = 'server01' OR (value)`, err: `invalid condition expression: value`},
		{s: `time > '2262-04-11 23:47:17'`, err: `time 2262-04-11T23:47:17Z overflows time literal`},
		{s: `time > '1677-09-20 19:12:43'`, err: `time 1677-09-20T19:12:43Z underflows time literal`},
	} {
		t.Run(tt.s, func(t *testing.T) {
			expr, err := influxql.ParseExpr(tt.s)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			cond, timeRange, err := query.ParseCondition(expr, &valuer)
			if err != nil {
				if tt.err == "" {
					t.Fatalf("unexpected error: %s", err)
				} else if have, want := err.Error(), tt.err; have != want {
					t.Fatalf("unexpected error: %s != %s", have, want)
				}
			}
			if cond != nil {
				if have, want := cond.String(), tt.cond; have != want {
					t.Errorf("unexpected condition:\nhave=%s\nwant=%s", have, want)
				}
			} else {
				if have, want := "", tt.cond; have != want {
					t.Errorf("unexpected condition:\nhave=%s\nwant=%s", have, want)
				}
			}
			if have, want := timeRange.Min, tt.min; !have.Equal(want) {
				t.Errorf("unexpected min time:\nhave=%s\nwant=%s", have, want)
			}
			if have, want := timeRange.Max, tt.max; !have.Equal(want) {
				t.Errorf("unexpected max time:\nhave=%s\nwant=%s", have, want)
			}
		})
	}
}

func TestSelect(t *testing.T) {
	mustParseTime := func(value string) time.Time {
		ts, err := time.Parse(time.RFC3339, value)
		if err != nil {
			t.Fatalf("unable to parse time: %s", err)
		}
		return ts
	}
	now := mustParseTime("2000-01-01T00:00:00Z")

	for _, tt := range []struct {
		name     string
		s        string
		expr     string
		typ      influxql.DataType
		createFn func(t *testing.T, opt influxql.IteratorOptions) []influxql.Iterator
		itrs     []influxql.Iterator
		points   [][]influxql.Point
		err      string
		optimize bool
		skip     bool
	}{
		{
			name: "Min",
			s:    `SELECT min(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 19, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 100, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10, Aggregated: 1}},
			},
		},
		{
			name: "Distinct_Float",
			s:    `SELECT distinct(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 1 * Second, Value: 19},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 11 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 12 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 19}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10}},
			},
		},
		{
			name: "Distinct_Integer",
			s:    `SELECT distinct(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 1 * Second, Value: 19},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 11 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 12 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 20}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 19}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10}},
			},
		},
		{
			name: "Distinct_String",
			s:    `SELECT distinct(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::string`,
			typ:  influxql.String,
			itrs: []influxql.Iterator{
				&mock.StringIterator{Points: []influxql.StringPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: "a"},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 1 * Second, Value: "b"},
				}},
				&mock.StringIterator{Points: []influxql.StringPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: "c"},
				}},
				&mock.StringIterator{Points: []influxql.StringPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: "b"},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: "d"},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 11 * Second, Value: "d"},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 12 * Second, Value: "d"},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: "a"}},
				{&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: "b"}},
				{&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: "d"}},
				{&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: "c"}},
			},
		},
		{
			name: "Distinct_Boolean",
			s:    `SELECT distinct(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::boolean`,
			typ:  influxql.Boolean,
			itrs: []influxql.Iterator{
				&mock.BooleanIterator{Points: []influxql.BooleanPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: true},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 1 * Second, Value: false},
				}},
				&mock.BooleanIterator{Points: []influxql.BooleanPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: false},
				}},
				&mock.BooleanIterator{Points: []influxql.BooleanPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: true},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: false},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 11 * Second, Value: false},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 12 * Second, Value: true},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.BooleanPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: true}},
				{&influxql.BooleanPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: false}},
				{&influxql.BooleanPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: false}},
				{&influxql.BooleanPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: true}},
				{&influxql.BooleanPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: false}},
			},
		},
		{
			name: "Mean_Float",
			s:    `SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 19.5, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2.5, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 100, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 3.2, Aggregated: 5}},
			},
		},
		{
			name: "Mean_Integer",
			s:    `SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 19.5, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2.5, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 100, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 3.2, Aggregated: 5}},
			},
		},
		{
			name: "Mean_String",
			s:    `SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			typ:  influxql.String,
			err:  `cannot use type string in argument to mean`,
		},
		{
			name: "Mean_Boolean",
			s:    `SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			typ:  influxql.Boolean,
			err:  `cannot use type boolean in argument to mean`,
		},
		{
			name: "Median_Float",
			s:    `SELECT median(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 19.5}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2.5}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 100}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 3}},
			},
		},
		{
			name: "Median_Integer",
			s:    `SELECT median(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 19.5}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2.5}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 100}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 3}},
			},
		},
		{
			name: "Median_String",
			s:    `SELECT median(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::string`,
			typ:  influxql.String,
			err:  `cannot use type string in argument to median`,
		},
		{
			name: "Median_Boolean",
			s:    `SELECT median(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::boolean`,
			typ:  influxql.Boolean,
			err:  `cannot use type boolean in argument to median`,
		},
		{
			name: "Mode_Float",
			s:    `SELECT mode(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 100}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 1}},
			},
		},
		{
			name: "Mode_Integer",
			s:    `SELECT mode(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 54 * Second, Value: 5},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 10}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 100}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 1}},
			},
		},
		{
			name: "Mode_String",
			s:    `SELECT mode(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::string`,
			typ:  influxql.String,
			itrs: []influxql.Iterator{
				&mock.StringIterator{Points: []influxql.StringPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: "a"},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 1 * Second, Value: "a"},
				}},
				&mock.StringIterator{Points: []influxql.StringPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: "cxxx"},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 6 * Second, Value: "zzzz"},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 7 * Second, Value: "zzzz"},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 8 * Second, Value: "zxxx"},
				}},
				&mock.StringIterator{Points: []influxql.StringPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: "b"},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: "d"},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 11 * Second, Value: "d"},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 12 * Second, Value: "d"},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: "a"}},
				{&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: "d"}},
				{&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: "zzzz"}},
			},
		},
		{
			name: "Mode_Boolean",
			s:    `SELECT mode(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::boolean`,
			typ:  influxql.Boolean,
			itrs: []influxql.Iterator{
				&mock.BooleanIterator{Points: []influxql.BooleanPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: true},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 1 * Second, Value: false},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 2 * Second, Value: false},
				}},
				&mock.BooleanIterator{Points: []influxql.BooleanPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: true},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 6 * Second, Value: false},
				}},
				&mock.BooleanIterator{Points: []influxql.BooleanPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: false},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: true},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 11 * Second, Value: false},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 12 * Second, Value: true},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.BooleanPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: false}},
				{&influxql.BooleanPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: true}},
				{&influxql.BooleanPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: true}},
			},
		},
		{
			name: "Top_NoTags_Float",
			s:    `SELECT top(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), host fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 9 * Second, Value: 19}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 31 * Second, Value: 100}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 5 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 53 * Second, Value: 5}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 53 * Second, Value: 4}},
			},
		},
		{
			name: "Top_NoTags_Integer",
			s:    `SELECT top(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), host fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 20}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 9 * Second, Value: 19}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 31 * Second, Value: 100}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 5 * Second, Value: 10}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 53 * Second, Value: 5}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 53 * Second, Value: 4}},
			},
		},
		{
			name: "Top_Tags_Float",
			s:    `SELECT top(value, host, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			points: [][]influxql.Point{
				{
					&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Time: 0 * Second, Value: "A"},
				},
				{
					&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					&influxql.StringPoint{Name: "cpu", Time: 5 * Second, Value: "B"},
				},
				{
					&influxql.FloatPoint{Name: "cpu", Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Time: 31 * Second, Value: "A"},
				},
				{
					&influxql.FloatPoint{Name: "cpu", Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
					&influxql.StringPoint{Name: "cpu", Time: 53 * Second, Value: "B"},
				},
			},
		},
		{
			name: "Top_Tags_Integer",
			s:    `SELECT top(value, host, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			points: [][]influxql.Point{
				{
					&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Time: 0 * Second, Value: "A"},
				},
				{
					&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					&influxql.StringPoint{Name: "cpu", Time: 5 * Second, Value: "B"},
				},
				{
					&influxql.IntegerPoint{Name: "cpu", Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Time: 31 * Second, Value: "A"},
				},
				{
					&influxql.IntegerPoint{Name: "cpu", Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
					&influxql.StringPoint{Name: "cpu", Time: 53 * Second, Value: "B"},
				},
			},
		},
		{
			name: "Top_GroupByTags_Float",
			s:    `SELECT top(value, host, 1) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			points: [][]influxql.Point{
				{
					&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("region=east"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("region=east"), Time: 9 * Second, Value: "A"},
				},
				{
					&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 0 * Second, Value: "A"},
				},
				{
					&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 31 * Second, Value: "A"},
				},
			},
		},
		{
			name: "Top_GroupByTags_Integer",
			s:    `SELECT top(value, host, 1) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			points: [][]influxql.Point{
				{
					&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("region=east"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("region=east"), Time: 9 * Second, Value: "A"},
				},
				{
					&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 0 * Second, Value: "A"},
				},
				{
					&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 31 * Second, Value: "A"},
				},
			},
		},
		{
			name: "Bottom_NoTags_Float",
			s:    `SELECT bottom(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), host fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 11 * Second, Value: 3}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 31 * Second, Value: 100}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 5 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 51 * Second, Value: 2}},
			},
		},
		{
			name: "Bottom_NoTags_Integer",
			s:    `SELECT bottom(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s), host fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 11 * Second, Value: 3}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 31 * Second, Value: 100}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 5 * Second, Value: 10}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 1}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 51 * Second, Value: 2}},
			},
		},
		{
			name: "Bottom_Tags_Float",
			s:    `SELECT bottom(value, host, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			points: [][]influxql.Point{
				{
					&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					&influxql.StringPoint{Name: "cpu", Time: 5 * Second, Value: "B"},
				},
				{
					&influxql.FloatPoint{Name: "cpu", Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Time: 10 * Second, Value: "A"},
				},
				{
					&influxql.FloatPoint{Name: "cpu", Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Time: 31 * Second, Value: "A"},
				},
				{
					&influxql.FloatPoint{Name: "cpu", Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					&influxql.StringPoint{Name: "cpu", Time: 50 * Second, Value: "B"},
				},
			},
		},
		{
			name: "Bottom_Tags_Integer",
			s:    `SELECT bottom(value, host, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(30s) fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			points: [][]influxql.Point{
				{
					&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					&influxql.StringPoint{Name: "cpu", Time: 5 * Second, Value: "B"},
				},
				{
					&influxql.IntegerPoint{Name: "cpu", Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Time: 10 * Second, Value: "A"},
				},
				{
					&influxql.IntegerPoint{Name: "cpu", Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Time: 31 * Second, Value: "A"},
				},
				{
					&influxql.IntegerPoint{Name: "cpu", Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					&influxql.StringPoint{Name: "cpu", Time: 50 * Second, Value: "B"},
				},
			},
		},
		{
			name: "Bottom_GroupByTags_Float",
			s:    `SELECT bottom(value, host, 1) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			points: [][]influxql.Point{
				{
					&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("region=east"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("region=east"), Time: 10 * Second, Value: "A"},
				},
				{
					&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 11 * Second, Value: "A"},
				},
				{
					&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 50 * Second, Value: "B"},
				},
			},
		},
		{
			name: "Bottom_GroupByTags_Integer",
			s:    `SELECT bottom(value, host, 1) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY region, time(30s) fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100, Aux: []interface{}{"A"}},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4, Aux: []interface{}{"B"}},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5, Aux: []interface{}{"B"}},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19, Aux: []interface{}{"A"}},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
				}},
			},
			points: [][]influxql.Point{
				{
					&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("region=east"), Time: 10 * Second, Value: 2, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("region=east"), Time: 10 * Second, Value: "A"},
				},
				{
					&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 11 * Second, Value: 3, Aux: []interface{}{"A"}},
					&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 11 * Second, Value: "A"},
				},
				{
					&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 50 * Second, Value: 1, Aux: []interface{}{"B"}},
					&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("region=west"), Time: 50 * Second, Value: "B"},
				},
			},
		},
		{
			name: "Fill_Null_Float",
			s:    `SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(null)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 12 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 20 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 40 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 50 * Second, Nil: true}},
			},
		},
		{
			name: "Fill_Number_Float",
			s:    `SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(1)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 12 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 20 * Second, Value: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 40 * Second, Value: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 50 * Second, Value: 1}},
			},
		},
		{
			name: "Fill_Previous_Float",
			s:    `SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(previous)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 12 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 20 * Second, Value: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 40 * Second, Value: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 50 * Second, Value: 2}},
			},
		},
		{
			name: "Fill_Linear_Float_One",
			s:    `SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(linear)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 12 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 32 * Second, Value: 4},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 20 * Second, Value: 3}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 4, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 40 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 50 * Second, Nil: true}},
			},
		},
		{
			name: "Fill_Linear_Float_Many",
			s:    `SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(linear)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 12 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 62 * Second, Value: 7},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 20 * Second, Value: 3}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 4}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 40 * Second, Value: 5}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 50 * Second, Value: 6}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 60 * Second, Value: 7, Aggregated: 1}},
			},
		},
		{
			name: "Fill_Linear_Float_MultipleSeries",
			s:    `SELECT mean(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(linear)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 12 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 32 * Second, Value: 4},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 20 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 40 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 50 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 10 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 20 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 30 * Second, Value: 4, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 40 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Nil: true}},
			},
		},
		{
			name: "Fill_Linear_Integer_One",
			s:    `SELECT max(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(linear)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 12 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 32 * Second, Value: 4},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Nil: true}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 1, Aggregated: 1}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 20 * Second, Value: 2}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 4, Aggregated: 1}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 40 * Second, Nil: true}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 50 * Second, Nil: true}},
			},
		},
		{
			name: "Fill_Linear_Integer_Many",
			s:    `SELECT max(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:20Z' GROUP BY host, time(10s) fill(linear)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 12 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 72 * Second, Value: 10},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Nil: true}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 1, Aggregated: 1}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 20 * Second, Value: 2}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 4}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 40 * Second, Value: 5}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 50 * Second, Value: 7}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 60 * Second, Value: 8}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 70 * Second, Value: 10, Aggregated: 1}},
			},
		},
		{
			name: "Fill_Linear_Integer_MultipleSeries",
			s:    `SELECT max(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:01:00Z' GROUP BY host, time(10s) fill(linear)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 12 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 32 * Second, Value: 4},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Nil: true}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 1}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 20 * Second, Nil: true}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Nil: true}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 40 * Second, Nil: true}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 50 * Second, Nil: true}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Nil: true}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 10 * Second, Nil: true}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 20 * Second, Nil: true}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 30 * Second, Value: 4, Aggregated: 1}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 40 * Second, Nil: true}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Nil: true}},
			},
		},
		{
			name: "Stddev_Float",
			s:    `SELECT stddev(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 0.7071067811865476}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 0.7071067811865476}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 1.5811388300841898}},
			},
		},
		{
			name: "Stddev_Integer",
			s:    `SELECT stddev(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 0.7071067811865476}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 0.7071067811865476}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 1.5811388300841898}},
			},
		},
		{
			name: "Spread_Float",
			s:    `SELECT spread(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 0}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 0}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 4}},
			},
		},
		{
			name: "Spread_Integer",
			s:    `SELECT spread(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 1},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 5},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 1}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 1}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 0}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 0}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 4}},
			},
		},
		{
			name: "Percentile_Float",
			s:    `SELECT percentile(value, 90) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 9},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 8},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 7},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 54 * Second, Value: 6},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 55 * Second, Value: 5},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 56 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 57 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 58 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 59 * Second, Value: 1},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 3}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 100}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 9}},
			},
		},
		{
			name: "Percentile_Integer",
			s:    `SELECT percentile(value, 90) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 9},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 8},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 7},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 54 * Second, Value: 6},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 55 * Second, Value: 5},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 56 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 57 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 58 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 59 * Second, Value: 1},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 20}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 3}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 100}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 50 * Second, Value: 9}},
			},
		},
		{
			name: "Sample_Float",
			s:    `SELECT sample(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::float`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 5 * Second, Value: 10},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=B"), Time: 10 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=B"), Time: 15 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 5 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 10 * Second, Value: 19}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 15 * Second, Value: 2}},
			},
		},
		{
			name: "Sample_Integer",
			s:    `SELECT sample(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::integer`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 5 * Second, Value: 10},
				}},
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=B"), Time: 10 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=B"), Time: 15 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 20}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 5 * Second, Value: 10}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 10 * Second, Value: 19}},
				{&influxql.IntegerPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 15 * Second, Value: 2}},
			},
		},
		{
			name: "Sample_String",
			s:    `SELECT sample(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::string`,
			typ:  influxql.String,
			itrs: []influxql.Iterator{
				&mock.StringIterator{Points: []influxql.StringPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: "a"},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 5 * Second, Value: "b"},
				}},
				&mock.StringIterator{Points: []influxql.StringPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=B"), Time: 10 * Second, Value: "c"},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=B"), Time: 15 * Second, Value: "d"},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: "a"}},
				{&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 5 * Second, Value: "b"}},
				{&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 10 * Second, Value: "c"}},
				{&influxql.StringPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 15 * Second, Value: "d"}},
			},
		},
		{
			name: "Sample_Boolean",
			s:    `SELECT sample(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			expr: `value::boolean`,
			typ:  influxql.Boolean,
			itrs: []influxql.Iterator{
				&mock.BooleanIterator{Points: []influxql.BooleanPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: true},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 5 * Second, Value: false},
				}},
				&mock.BooleanIterator{Points: []influxql.BooleanPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=B"), Time: 10 * Second, Value: false},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=B"), Time: 15 * Second, Value: true},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.BooleanPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: true}},
				{&influxql.BooleanPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 5 * Second, Value: false}},
				{&influxql.BooleanPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 10 * Second, Value: false}},
				{&influxql.BooleanPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 15 * Second, Value: true}},
			},
		},
		{
			name: "Raw",
			s:    `SELECT v1, v2 FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Time: 0, Aux: []interface{}{float64(1), nil}},
					{Time: 1, Aux: []interface{}{nil, float64(2)}},
					{Time: 5, Aux: []interface{}{float64(3), float64(4)}},
				}},
			},
			points: [][]influxql.Point{
				{
					&influxql.FloatPoint{Time: 0, Value: 1},
					&influxql.FloatPoint{Time: 0, Nil: true},
				},
				{
					&influxql.FloatPoint{Time: 1, Nil: true},
					&influxql.FloatPoint{Time: 1, Value: 2},
				},
				{
					&influxql.FloatPoint{Time: 5, Value: 3},
					&influxql.FloatPoint{Time: 5, Value: 4},
				},
			},
		},
		{
			name: "BinaryExpr_Float_AddNumberRHS",
			s:    `SELECT value + 2.0 FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			name: "BinaryExpr_Float_AddIntegerRHS",
			s:    `SELECT value + 2 FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			name: "BinaryExpr_Float_AddNumberLHS",
			s:    `SELECT 2.0 + value FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			name: "BinaryExpr_Float_AddIntegerLHS",
			s:    `SELECT 2 + value FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			name: "BinaryExpr_Float_AddTwoVariables",
			s:    `SELECT value + value FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			name: "BinaryExpr_Float_MultiplyNumberRHS",
			s:    `SELECT value * 2.0 FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			name: "BinaryExpr_Float_MultiplyIntegerRHS",
			s:    `SELECT value * 2 FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			name: "BinaryExpr_Float_MultiplyNumberLHS",
			s:    `SELECT 2.0 * value FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			name: "BinaryExpr_Float_MultiplyIntegerLHS",
			s:    `SELECT 2 * value FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			name: "BinaryExpr_Float_MultiplyTwoVariables",
			s:    `SELECT value * value FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 400}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 100}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 361}},
			},
		},
		{
			name: "BinaryExpr_Float_SubtractNumberRHS",
			s:    `SELECT value - 2.0 FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 18}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 8}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 17}},
			},
		},
		{
			name: "BinaryExpr_Float_SubtractIntegerRHS",
			s:    `SELECT value - 2 FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 18}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 8}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 17}},
			},
		},
		{
			name: "BinaryExpr_Float_SubtractNumberLHS",
			s:    `SELECT 2.0 - value FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: -18}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: -8}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: -17}},
			},
		},
		{
			name: "BinaryExpr_Float_SubtractIntegerLHS",
			s:    `SELECT 2 - value FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: -18}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: -8}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: -17}},
			},
		},
		{
			name: "BinaryExpr_Float_SubtractTwoVariables",
			s:    `SELECT value - value FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 0}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 0}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 0}},
			},
		},
		{
			name: "BinaryExpr_Float_DivideNumberRHS",
			s:    `SELECT value / 2.0 FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 5}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: float64(19) / 2}},
			},
		},
		{
			name: "BinaryExpr_Float_DivideIntegerRHS",
			s:    `SELECT value / 2 FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 5}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: float64(19) / 2}},
			},
		},
		{
			name: "BinaryExpr_Float_DivideNumberLHS",
			s:    `SELECT 38.0 / value FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 1.9}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 3.8}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 2}},
			},
		},
		{
			name: "BinaryExpr_Float_DivideIntegerLHS",
			s:    `SELECT 38 / value FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 1.9}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 3.8}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 2}},
			},
		},
		{
			name: "BinaryExpr_Float_DivideTwoVariables",
			s:    `SELECT value / value FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{float64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{float64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{float64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 1}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 1}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 1}},
			},
		},
		{
			name: "BinaryExpr_Integer_AddNumberRHS",
			s:    `SELECT value + 2.0 FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			name: "BinaryExpr_Integer_AddIntegerRHS",
			s:    `SELECT value + 2 FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			name: "BinaryExpr_Integer_AddNumberLHS",
			s:    `SELECT 2.0 + value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			name: "BinaryExpr_Integer_AddIntegerLHS",
			s:    `SELECT 2 + value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 22}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 12}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 21}},
			},
		},
		{
			name: "BinaryExpr_Integer_AddTwoVariables",
			s:    `SELECT value + value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			name: "BinaryExpr_Integer_MultiplyNumberRHS",
			s:    `SELECT value * 2.0 FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			name: "BinaryExpr_Integer_MultiplyIntegerRHS",
			s:    `SELECT value * 2 FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			name: "BinaryExpr_Integer_MultiplyNumberLHS",
			s:    `SELECT 2.0 * value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			name: "BinaryExpr_Integer_MultiplyIntegerLHS",
			s:    `SELECT 2 * value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 40}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 20}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 38}},
			},
		},
		{
			name: "BinaryExpr_Integer_MultiplyTwoVariables",
			s:    `SELECT value * value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 400}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 100}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 361}},
			},
		},
		{
			name: "BinaryExpr_Integer_SubtractNumberRHS",
			s:    `SELECT value - 2.0 FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 18}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 8}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 17}},
			},
		},
		{
			name: "BinaryExpr_Integer_SubtractIntegerRHS",
			s:    `SELECT value - 2 FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 18}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 8}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 17}},
			},
		},
		{
			name: "BinaryExpr_Integer_SubtractNumberLHS",
			s:    `SELECT 2.0 - value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: -18}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: -8}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: -17}},
			},
		},
		{
			name: "BinaryExpr_Integer_SubtractIntegerLHS",
			s:    `SELECT 2 - value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: -18}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: -8}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: -17}},
			},
		},
		{
			name: "BinaryExpr_Integer_SubtractTwoVariables",
			s:    `SELECT value - value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 0}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 0}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 0}},
			},
		},
		{
			name: "BinaryExpr_Integer_DivideNumberRHS",
			s:    `SELECT value / 2.0 FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 5}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: float64(19) / 2}},
			},
		},
		{
			name: "BinaryExpr_Integer_DivideIntegerRHS",
			s:    `SELECT value / 2 FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 5}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: float64(19) / 2}},
			},
		},
		{
			name: "BinaryExpr_Integer_DivideNumberLHS",
			s:    `SELECT 38.0 / value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 1.9}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 3.8}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 2}},
			},
		},
		{
			name: "BinaryExpr_Integer_DivideIntegerLHS",
			s:    `SELECT 38 / value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 1.9}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 3.8}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 2}},
			},
		},
		{
			name: "BinaryExpr_Integer_DivideTwoVariables",
			s:    `SELECT value / value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 1}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 1}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 1}},
			},
		},
		{
			name: "BinaryExpr_Integer_BitwiseAndRHS",
			s:    `SELECT value & 254 FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 10}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 18}},
			},
		},
		{
			name: "BinaryExpr_Integer_BitwiseOrLHS",
			s:    `SELECT 4 | value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 14}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 23}},
			},
		},
		{
			name: "BinaryExpr_Integer_BitwiseXorTwoVariables",
			s:    `SELECT value ^ value FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{int64(20)}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{int64(10)}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{int64(19)}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 0}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 5 * Second, Value: 0}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 9 * Second, Value: 0}},
			},
		},
		{
			name: "BinaryExpr_Mixed_Add",
			s:    `SELECT z + value FROM cpu`,
			typ:  influxql.Float,
			createFn: func(t *testing.T, opt influxql.IteratorOptions) []influxql.Iterator {
				values := []map[string]interface{}{
					{"value": float64(20), "z": int64(10)},
					{"value": float64(10), "z": int64(15)},
					{"value": float64(19), "z": int64(5)},
				}

				points := make([]influxql.FloatPoint, 3)
				for i, d := range []int64{0, 5, 9} {
					auxFields := make([]interface{}, 2)
					for j, ref := range opt.Aux {
						auxFields[j] = values[i][ref.Val]
					}
					points[i] = influxql.FloatPoint{
						Name: "cpu",
						Time: d * Second,
						Aux:  auxFields,
					}
				}
				return []influxql.Iterator{&mock.FloatIterator{Points: points}}
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 30}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 25}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 24}},
			},
		},
		{
			name: "BinaryExpr_Mixed_Subtract",
			s:    `SELECT z - value FROM cpu`,
			typ:  influxql.Float,
			createFn: func(t *testing.T, opt influxql.IteratorOptions) []influxql.Iterator {
				values := []map[string]interface{}{
					{"value": float64(10), "z": int64(20)},
					{"value": float64(15), "z": int64(10)},
					{"value": float64(5), "z": int64(19)},
				}

				points := make([]influxql.FloatPoint, 3)
				for i, d := range []int64{0, 5, 9} {
					auxFields := make([]interface{}, 2)
					for j, ref := range opt.Aux {
						auxFields[j] = values[i][ref.Val]
					}
					points[i] = influxql.FloatPoint{
						Name: "cpu",
						Time: d * Second,
						Aux:  auxFields,
					}
				}
				return []influxql.Iterator{&mock.FloatIterator{Points: points}}
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: -5}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 14}},
			},
		},
		{
			name: "BinaryExpr_Mixed_Multiply",
			s:    `SELECT z * value FROM cpu`,
			typ:  influxql.Float,
			createFn: func(t *testing.T, opt influxql.IteratorOptions) []influxql.Iterator {
				values := []map[string]interface{}{
					{"value": float64(20), "z": int64(10)},
					{"value": float64(10), "z": int64(15)},
					{"value": float64(19), "z": int64(5)},
				}

				points := make([]influxql.FloatPoint, 3)
				for i, d := range []int64{0, 5, 9} {
					auxFields := make([]interface{}, 2)
					for j, ref := range opt.Aux {
						auxFields[j] = values[i][ref.Val]
					}
					points[i] = influxql.FloatPoint{
						Name: "cpu",
						Time: d * Second,
						Aux:  auxFields,
					}
				}
				return []influxql.Iterator{&mock.FloatIterator{Points: points}}
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 200}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 150}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: 95}},
			},
		},
		{
			name: "BinaryExpr_Mixed_Division",
			s:    `SELECT z / value FROM cpu`,
			typ:  influxql.Float,
			createFn: func(t *testing.T, opt influxql.IteratorOptions) []influxql.Iterator {
				values := []map[string]interface{}{
					{"value": float64(10), "z": int64(20)},
					{"value": float64(15), "z": int64(10)},
					{"value": float64(5), "z": int64(19)},
				}

				points := make([]influxql.FloatPoint, 3)
				for i, d := range []int64{0, 5, 9} {
					auxFields := make([]interface{}, 2)
					for j, ref := range opt.Aux {
						auxFields[j] = values[i][ref.Val]
					}
					points[i] = influxql.FloatPoint{
						Name: "cpu",
						Time: d * Second,
						Aux:  auxFields,
					}
				}
				return []influxql.Iterator{&mock.FloatIterator{Points: points}}
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 2}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: float64(10) / float64(15)}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Value: float64(19) / float64(5)}},
			},
		},
		{
			name: "BinaryExpr_Boolean_BitwiseXorRHS",
			s:    `SELECT one ^ true FROM cpu`,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{true}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{false}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{true}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.BooleanPoint{Name: "cpu", Time: 0 * Second, Value: false}},
				{&influxql.BooleanPoint{Name: "cpu", Time: 5 * Second, Value: true}},
				{&influxql.BooleanPoint{Name: "cpu", Time: 9 * Second, Value: false}},
			},
		},
		{
			name: "BinaryExpr_Boolean_BitwiseOrLHS",
			s:    `SELECT true | two FROM cpu`,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Aux: []interface{}{true}},
					{Name: "cpu", Time: 5 * Second, Aux: []interface{}{false}},
					{Name: "cpu", Time: 9 * Second, Aux: []interface{}{true}},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.BooleanPoint{Name: "cpu", Time: 0 * Second, Value: true}},
				{&influxql.BooleanPoint{Name: "cpu", Time: 5 * Second, Value: true}},
				{&influxql.BooleanPoint{Name: "cpu", Time: 9 * Second, Value: true}},
			},
		},
		{
			name: "BinaryExpr_Boolean_BitwiseAndTwoVariables",
			s:    `SELECT one & two FROM cpu`,
			createFn: func(t *testing.T, opt influxql.IteratorOptions) []influxql.Iterator {
				values := []map[string]interface{}{
					{"one": true, "two": false},
					{"one": false, "two": false},
					{"one": true, "two": true},
				}

				points := make([]influxql.FloatPoint, 3)
				for i, d := range []int64{0, 5, 9} {
					auxFields := make([]interface{}, 2)
					for j, ref := range opt.Aux {
						auxFields[j] = values[i][ref.Val]
					}
					points[i] = influxql.FloatPoint{
						Name: "cpu",
						Time: d * Second,
						Aux:  auxFields,
					}
				}
				return []influxql.Iterator{&mock.FloatIterator{Points: points}}
			},
			points: [][]influxql.Point{
				{&influxql.BooleanPoint{Name: "cpu", Time: 0 * Second, Value: false}},
				{&influxql.BooleanPoint{Name: "cpu", Time: 5 * Second, Value: false}},
				{&influxql.BooleanPoint{Name: "cpu", Time: 9 * Second, Value: true}},
			},
		},
		{
			name: "BinaryExpr_NilValues_Add",
			s:    `SELECT v1 + v2 FROM cpu`,
			createFn: func(t *testing.T, opt influxql.IteratorOptions) []influxql.Iterator {
				values := []map[string]interface{}{
					{"v1": float64(20), "v2": nil},
					{"v1": float64(10), "v2": float64(15)},
					{"v1": nil, "v2": float64(5)},
				}

				points := make([]influxql.FloatPoint, 3)
				for i, d := range []int64{0, 5, 9} {
					auxFields := make([]interface{}, 2)
					for j, ref := range opt.Aux {
						auxFields[j] = values[i][ref.Val]
					}
					points[i] = influxql.FloatPoint{
						Name: "cpu",
						Time: d * Second,
						Aux:  auxFields,
					}
				}
				return []influxql.Iterator{&mock.FloatIterator{Points: points}}
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 25}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Nil: true}},
			},
		},
		{
			name: "BinaryExpr_NilValues_Subtract",
			s:    `SELECT v1 - v2 FROM cpu`,
			typ:  influxql.Float,
			createFn: func(t *testing.T, opt influxql.IteratorOptions) []influxql.Iterator {
				values := []map[string]interface{}{
					{"v1": float64(20), "v2": nil},
					{"v1": float64(10), "v2": float64(15)},
					{"v1": nil, "v2": float64(5)},
				}

				points := make([]influxql.FloatPoint, 3)
				for i, d := range []int64{0, 5, 9} {
					auxFields := make([]interface{}, 2)
					for j, ref := range opt.Aux {
						auxFields[j] = values[i][ref.Val]
					}
					points[i] = influxql.FloatPoint{
						Name: "cpu",
						Time: d * Second,
						Aux:  auxFields,
					}
				}
				return []influxql.Iterator{&mock.FloatIterator{Points: points}}
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: -5}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Nil: true}},
			},
		},
		{
			name: "BinaryExpr_NilValues_Multiply",
			s:    `SELECT v1 * v2 FROM cpu`,
			typ:  influxql.Float,
			createFn: func(t *testing.T, opt influxql.IteratorOptions) []influxql.Iterator {
				values := []map[string]interface{}{
					{"v1": float64(20), "v2": nil},
					{"v1": float64(10), "v2": float64(15)},
					{"v1": nil, "v2": float64(5)},
				}

				points := make([]influxql.FloatPoint, 3)
				for i, d := range []int64{0, 5, 9} {
					auxFields := make([]interface{}, 2)
					for j, ref := range opt.Aux {
						auxFields[j] = values[i][ref.Val]
					}
					points[i] = influxql.FloatPoint{
						Name: "cpu",
						Time: d * Second,
						Aux:  auxFields,
					}
				}
				return []influxql.Iterator{&mock.FloatIterator{Points: points}}
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: 150}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Nil: true}},
			},
		},
		{
			name: "BinaryExpr_NilValues_Division",
			s:    `SELECT v1 / v2 FROM cpu`,
			typ:  influxql.Float,
			createFn: func(t *testing.T, opt influxql.IteratorOptions) []influxql.Iterator {
				values := []map[string]interface{}{
					{"v1": float64(20), "v2": nil},
					{"v1": float64(10), "v2": float64(15)},
					{"v1": nil, "v2": float64(5)},
				}

				points := make([]influxql.FloatPoint, 3)
				for i, d := range []int64{0, 5, 9} {
					auxFields := make([]interface{}, 2)
					for j, ref := range opt.Aux {
						auxFields[j] = values[i][ref.Val]
					}
					points[i] = influxql.FloatPoint{
						Name: "cpu",
						Time: d * Second,
						Aux:  auxFields,
					}
				}
				return []influxql.Iterator{&mock.FloatIterator{Points: points}}
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Nil: true}},
				{&influxql.FloatPoint{Name: "cpu", Time: 5 * Second, Value: float64(10) / float64(15)}},
				{&influxql.FloatPoint{Name: "cpu", Time: 9 * Second, Nil: true}},
			},
		},
		{
			name: "ParenExpr_Min",
			s:    `SELECT (min(value)) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 19, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 100, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10, Aggregated: 1}},
			},
		},
		{
			name: "ParenExpr_Distinct",
			s:    `SELECT (distinct(value)) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-02T00:00:00Z' GROUP BY time(10s), host fill(none)`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 1 * Second, Value: 19},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 11 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 12 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 19}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 10 * Second, Value: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10}},
			},
		},
		{
			name: "Derivative_Float",
			s:    `SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 12 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.5}},
				{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 2.25}},
				{&influxql.FloatPoint{Name: "cpu", Time: 12 * Second, Value: -4}},
			},
		},
		{
			name: "Derivative_Integer",
			s:    `SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 12 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.5}},
				{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 2.25}},
				{&influxql.FloatPoint{Name: "cpu", Time: 12 * Second, Value: -4}},
			},
		},
		{
			name: "Derivative_Desc_Float",
			s:    `SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z' ORDER BY desc`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 12 * Second, Value: 3},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 0 * Second, Value: 20},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
				{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.25}},
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 2.5}},
			},
		},
		{
			name: "Derivative_Desc_Integer",
			s:    `SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z' ORDER BY desc`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Time: 12 * Second, Value: 3},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 0 * Second, Value: 20},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
				{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.25}},
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 2.5}},
			},
		},
		{
			name: "Derivative_Duplicate_Float",
			s:    `SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 0 * Second, Value: 19},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 4 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.5}},
			},
		},
		{
			name: "Derivative_Duplicate_Integer",
			s:    `SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 0 * Second, Value: 19},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 4 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.5}},
			},
		},
		{
			name: "Difference_Float",
			s:    `SELECT difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 12 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -10}},
				{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 9}},
				{&influxql.FloatPoint{Name: "cpu", Time: 12 * Second, Value: -16}},
			},
		},
		{
			name: "Difference_Integer",
			s:    `SELECT difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 12 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: -10}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 9}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 12 * Second, Value: -16}},
			},
		},
		{
			name: "Difference_Duplicate_Float",
			s:    `SELECT difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 0 * Second, Value: 19},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 4 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -10}},
			},
		},
		{
			name: "Difference_Duplicate_Integer",
			s:    `SELECT difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 0 * Second, Value: 19},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 4 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: -10}},
			},
		},
		{
			name: "Non_Negative_Difference_Float",
			s:    `SELECT non_negative_difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 8 * Second, Value: 29},
					{Name: "cpu", Time: 12 * Second, Value: 3},
					{Name: "cpu", Time: 16 * Second, Value: 39},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 19}},
				{&influxql.FloatPoint{Name: "cpu", Time: 16 * Second, Value: 36}},
			},
		},
		{
			name: "Non_Negative_Difference_Integer",
			s:    `SELECT non_negative_difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 8 * Second, Value: 21},
					{Name: "cpu", Time: 12 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 11}},
			},
		},
		{
			name: "Non_Negative_Difference_Duplicate_Float",
			s:    `SELECT non_negative_difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 0 * Second, Value: 19},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 4 * Second, Value: 3},
					{Name: "cpu", Time: 8 * Second, Value: 30},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 12 * Second, Value: 10},
					{Name: "cpu", Time: 12 * Second, Value: 3},
					{Name: "cpu", Time: 16 * Second, Value: 40},
					{Name: "cpu", Time: 16 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Time: 16 * Second, Value: 30}},
			},
		},
		{
			name: "Non_Negative_Difference_Duplicate_Integer",
			s:    `SELECT non_negative_difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 0 * Second, Value: 19},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 4 * Second, Value: 3},
					{Name: "cpu", Time: 8 * Second, Value: 30},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 12 * Second, Value: 10},
					{Name: "cpu", Time: 12 * Second, Value: 3},
					{Name: "cpu", Time: 16 * Second, Value: 40},
					{Name: "cpu", Time: 16 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 20}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 16 * Second, Value: 30}},
			},
		},
		{
			name: "Elapsed_Float",
			s:    `SELECT elapsed(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 11 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 4}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 11 * Second, Value: 3}},
			},
		},
		{
			name: "Elapsed_Integer",
			s:    `SELECT elapsed(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 11 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 4}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 11 * Second, Value: 3}},
			},
		},
		{
			name: "Elapsed_String",
			s:    `SELECT elapsed(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.String,
			itrs: []influxql.Iterator{
				&mock.StringIterator{Points: []influxql.StringPoint{
					{Name: "cpu", Time: 0 * Second, Value: "a"},
					{Name: "cpu", Time: 4 * Second, Value: "b"},
					{Name: "cpu", Time: 8 * Second, Value: "c"},
					{Name: "cpu", Time: 11 * Second, Value: "d"},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 4}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 11 * Second, Value: 3}},
			},
		},
		{
			name: "Elapsed_Boolean",
			s:    `SELECT elapsed(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Boolean,
			itrs: []influxql.Iterator{
				&mock.BooleanIterator{Points: []influxql.BooleanPoint{
					{Name: "cpu", Time: 0 * Second, Value: true},
					{Name: "cpu", Time: 4 * Second, Value: false},
					{Name: "cpu", Time: 8 * Second, Value: false},
					{Name: "cpu", Time: 11 * Second, Value: true},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 4}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 11 * Second, Value: 3}},
			},
		},
		{
			name: "Integral_Float",
			s:    `SELECT integral(value) FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 10 * Second, Value: 20},
					{Name: "cpu", Time: 15 * Second, Value: 10},
					{Name: "cpu", Time: 20 * Second, Value: 0},
					{Name: "cpu", Time: 30 * Second, Value: -10},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 50}},
			},
		},
		{
			name: "Integral_Float_GroupByTime",
			s:    `SELECT integral(value) FROM cpu WHERE time > 0s AND time < 60s GROUP BY time(20s)`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 10 * Second, Value: 20},
					{Name: "cpu", Time: 15 * Second, Value: 10},
					{Name: "cpu", Time: 20 * Second, Value: 0},
					{Name: "cpu", Time: 30 * Second, Value: -10},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 100}},
				{&influxql.FloatPoint{Name: "cpu", Time: 20 * Second, Value: -50}},
			},
		},
		{
			name: "Integral_Float_InterpolateGroupByTime",
			s:    `SELECT integral(value) FROM cpu WHERE time > 0s AND time < 60s GROUP BY time(20s)`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 10 * Second, Value: 20},
					{Name: "cpu", Time: 15 * Second, Value: 10},
					{Name: "cpu", Time: 25 * Second, Value: 0},
					{Name: "cpu", Time: 30 * Second, Value: -10},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 112.5}},
				{&influxql.FloatPoint{Name: "cpu", Time: 20 * Second, Value: -12.5}},
			},
		},
		{
			name: "Integral_Integer",
			s:    `SELECT integral(value) FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 5 * Second, Value: 10},
					{Name: "cpu", Time: 10 * Second, Value: 0},
					{Name: "cpu", Time: 20 * Second, Value: -10},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 50}},
			},
		},
		{
			name: "Integral_Duplicate_Float",
			s:    `SELECT integral(value) FROM cpu`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 5 * Second, Value: 10},
					{Name: "cpu", Time: 5 * Second, Value: 30},
					{Name: "cpu", Time: 10 * Second, Value: 40},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 250}},
			},
		},
		{
			name: "Integral_Duplicate_Integer",
			s:    `SELECT integral(value, 2s) FROM cpu`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 5 * Second, Value: 10},
					{Name: "cpu", Time: 5 * Second, Value: 30},
					{Name: "cpu", Time: 10 * Second, Value: 40},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 125}},
			},
		},
		{
			name: "MovingAverage_Float",
			s:    `SELECT moving_average(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 12 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 15, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 14.5, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Time: 12 * Second, Value: 11, Aggregated: 2}},
			},
		},
		{
			name: "MovingAverage_Integer",
			s:    `SELECT moving_average(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 12 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 15, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 14.5, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Time: 12 * Second, Value: 11, Aggregated: 2}},
			},
		},
		{
			name: "CumulativeSum_Float",
			s:    `SELECT cumulative_sum(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 12 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 30}},
				{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 49}},
				{&influxql.FloatPoint{Name: "cpu", Time: 12 * Second, Value: 52}},
			},
		},
		{
			name: "CumulativeSum_Integer",
			s:    `SELECT cumulative_sum(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 8 * Second, Value: 19},
					{Name: "cpu", Time: 12 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 30}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 49}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 12 * Second, Value: 52}},
			},
		},
		{
			name: "CumulativeSum_Duplicate_Float",
			s:    `SELECT cumulative_sum(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 0 * Second, Value: 19},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 4 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 39}},
				{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 49}},
				{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 52}},
			},
		},
		{
			name: "CumulativeSum_Duplicate_Integer",
			s:    `SELECT cumulative_sum(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`,
			typ:  influxql.Integer,
			itrs: []influxql.Iterator{
				&mock.IntegerIterator{Points: []influxql.IntegerPoint{
					{Name: "cpu", Time: 0 * Second, Value: 20},
					{Name: "cpu", Time: 0 * Second, Value: 19},
					{Name: "cpu", Time: 4 * Second, Value: 10},
					{Name: "cpu", Time: 4 * Second, Value: 3},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 39}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 49}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 52}},
			},
		},
		{
			name: "HoltWinters_GroupBy_Agg",
			s:    `SELECT holt_winters(mean(value), 2, 2) FROM cpu WHERE time >= '1970-01-01T00:00:10Z' AND time < '1970-01-01T00:00:20Z' GROUP BY time(2s)`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Time: 10 * Second, Value: 4},
					{Name: "cpu", Time: 11 * Second, Value: 6},

					{Name: "cpu", Time: 12 * Second, Value: 9},
					{Name: "cpu", Time: 13 * Second, Value: 11},

					{Name: "cpu", Time: 14 * Second, Value: 5},
					{Name: "cpu", Time: 15 * Second, Value: 7},

					{Name: "cpu", Time: 16 * Second, Value: 10},
					{Name: "cpu", Time: 17 * Second, Value: 12},

					{Name: "cpu", Time: 18 * Second, Value: 6},
					{Name: "cpu", Time: 19 * Second, Value: 8},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Time: 20 * Second, Value: 11.960623419918432}},
				{&influxql.FloatPoint{Name: "cpu", Time: 22 * Second, Value: 7.953140268154609}},
			},
		},
		{
			name: "OptimizedFunctionCall",
			s:    `SELECT mean(value) FROM cpu`,
			typ:  influxql.Float,
			createFn: func(t *testing.T, opt influxql.IteratorOptions) []influxql.Iterator {
				if diff := cmp.Diff(opt.Expr, influxql.MustParseExpr("mean(value::float)")); diff != "" {
					t.Fatalf("unexpected expr:\n%s", diff)
				}
				return []influxql.Iterator{
					&mock.FloatIterator{Points: []influxql.FloatPoint{
						{Name: "cpu", Value: 3},
					}},
				}
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Value: 3}},
			},
			optimize: true,
		},
		{
			name: "SubqueryBasic",
			s:    `SELECT mean(max) FROM (SELECT max(value) FROM cpu GROUP BY time(30s), host) WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T01:00:00Z' GROUP BY host FILL(none)`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 9},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 8},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 7},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 54 * Second, Value: 6},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 55 * Second, Value: 5},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 56 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 57 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 58 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 59 * Second, Value: 1},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 60, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10, Aggregated: 2}},
			},
		},
		{
			name: "SubqueryWithFieldFilter",
			s:    `SELECT mean(max) FROM (SELECT max(value) FROM cpu GROUP BY time(30s), host) WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T01:00:00Z' AND max > 50 GROUP BY host FILL(none)`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 9},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 8},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 7},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 54 * Second, Value: 6},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 55 * Second, Value: 5},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 56 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 57 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 58 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 59 * Second, Value: 1},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 100, Aggregated: 1}},
			},
		},
		{
			name: "SubqueryWithTagFilter",
			s:    `SELECT mean(max) FROM (SELECT max(value) FROM cpu GROUP BY time(30s), host) WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T01:00:00Z' AND host = 'B' GROUP BY host FILL(none)`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 9},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 8},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 7},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 54 * Second, Value: 6},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 55 * Second, Value: 5},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 56 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 57 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 58 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 59 * Second, Value: 1},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10, Aggregated: 2}},
			},
		},
		{
			name: "SubqueryWithDifferentTags",
			s:    `SELECT mean(max) FROM (SELECT max(value) FROM cpu GROUP BY time(30s), host, region) WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T01:00:00Z' GROUP BY time(30s), host FILL(none)`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 9},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 8},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 7},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 54 * Second, Value: 6},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 55 * Second, Value: 5},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 56 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 57 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 58 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 59 * Second, Value: 1},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 19.5, Aggregated: 2}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 100, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10, Aggregated: 1}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 30 * Second, Value: 10, Aggregated: 1}},
			},
		},
		{
			name: "SubqueryRaw",
			s:    `SELECT max, host FROM (SELECT max(value) FROM cpu GROUP BY time(30s), host FILL(none)) WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T01:00:00Z'`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{
					&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 20},
					&influxql.StringPoint{Name: "cpu", Time: 0 * Second, Value: "A"},
				},
				{
					&influxql.FloatPoint{Name: "cpu", Time: 30 * Second, Value: 100},
					&influxql.StringPoint{Name: "cpu", Time: 30 * Second, Value: "A"},
				},
			},
		},
		{
			name: "SubqueryCountDimensions",
			s:    `SELECT count(host) FROM (SELECT first(value) FROM cpu GROUP BY time(30s), host, region) WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T01:00:00Z' GROUP BY time(30s) FILL(none)`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 9},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 8},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 7},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 54 * Second, Value: 6},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 55 * Second, Value: 5},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 56 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 57 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 58 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 59 * Second, Value: 1},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 3, Aggregated: 3}},
				{&influxql.IntegerPoint{Name: "cpu", Time: 30 * Second, Value: 2, Aggregated: 2}},
			},
		},
		{
			name: "SubqueryWildcards",
			s:    `SELECT * FROM (SELECT max(value) FROM cpu GROUP BY time(30s), host FILL(none)) WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T01:00:00Z' GROUP BY *`,
			typ:  influxql.Float,
			itrs: []influxql.Iterator{
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 0 * Second, Value: 20},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 11 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=A"), Time: 31 * Second, Value: 100},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 5 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 50 * Second, Value: 10},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 51 * Second, Value: 9},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 52 * Second, Value: 8},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 53 * Second, Value: 7},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 54 * Second, Value: 6},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 55 * Second, Value: 5},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 56 * Second, Value: 4},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 57 * Second, Value: 3},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 58 * Second, Value: 2},
					{Name: "cpu", Tags: mock.ParseTags("region=west,host=B"), Time: 59 * Second, Value: 1},
				}},
				&mock.FloatIterator{Points: []influxql.FloatPoint{
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 9 * Second, Value: 19},
					{Name: "cpu", Tags: mock.ParseTags("region=east,host=A"), Time: 10 * Second, Value: 2},
				}},
			},
			points: [][]influxql.Point{
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 0 * Second, Value: 20}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=A"), Time: 30 * Second, Value: 100}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 0 * Second, Value: 10}},
				{&influxql.FloatPoint{Name: "cpu", Tags: mock.ParseTags("host=B"), Time: 30 * Second, Value: 10}},
			},
		},
		//{name: "SubqueryWithAliases"},
		//{name: "SubqueryWithNameConflicts"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip()
			}

			stmt, err := influxql.ParseStatement(tt.s)
			if err != nil {
				t.Fatalf("unable to parse statement: %s", err)
			}

			opt := query.CompileOptions{
				Now:                  now,
				DisableOptimizations: !tt.optimize,
			}
			c, err := query.Compile(stmt.(*influxql.SelectStatement), opt)
			if err != nil {
				t.Fatalf("unable to compile statement: %s", err)
			}

			linker := mock.NewShardMapper(func(stub *mock.ShardMapper) {
				stub.ShardsByTimeRangeFn = func(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
					return []meta.ShardInfo{{ID: 1}}, nil
				}
				stub.ShardGroupFn = func(ids []uint64) query.ShardGroup {
					if diff := cmp.Diff(ids, []uint64{1}); diff != "" {
						t.Fatalf("unexpected shard ids:\n%s", diff)
					}
					return &mock.ShardGroup{
						Measurements: map[string]mock.ShardMeta{
							"cpu": {
								Fields: map[string]influxql.DataType{
									"value": tt.typ,
									"v1":    influxql.Float,
									"v2":    influxql.Float,
									"z":     influxql.Integer,
									"one":   influxql.Boolean,
									"two":   influxql.Boolean,
								},
								Dimensions: []string{"host", "region"},
							},
						},
						CreateIteratorFn: func(name string, opt influxql.IteratorOptions) (influxql.Iterator, error) {
							if name != "cpu" {
								t.Fatalf("unexpected source: %s", name)
							}
							if tt.expr != "" {
								if diff := cmp.Diff(opt.Expr, influxql.MustParseExpr(tt.expr)); diff != "" {
									t.Fatalf("unexpected expr:\n%s", diff)
								}
							}

							itrs := tt.itrs
							if tt.createFn != nil {
								itrs = tt.createFn(t, opt)
							}
							if len(itrs) == 1 {
								return itrs[0], nil
							}
							return influxql.Iterators(itrs).Merge(opt)
						},
					}
				}
			})

			fields, _, err := c.Select(linker)
			if err != nil {
				if tt.err == "" {
					t.Fatalf("unable to link statement: %s", err)
				} else if tt.err != err.Error() {
					t.Fatalf("unexpected error: have=%s want=%s", err, tt.err)
				}
				return
			} else if tt.err != "" {
				t.Fatal("expected error")
			}

			if err := query.ExecuteTargets(fields, func(n query.Node) error {
				return n.Execute()
			}); err != nil {
				t.Fatalf("error while executing the plan: %s", err)
			}

			itrs := make([]influxql.Iterator, len(fields))
			for i, f := range fields {
				itrs[i] = f.Iterator()
			}

			if a, err := mock.Iterators(itrs).ReadAll(); err != nil {
				t.Fatalf("unexpected error: %s", err)
			} else if diff := cmp.Diff(tt.points, a); diff != "" {
				t.Fatalf("unexpected points:\n%s", diff)
			}
		})
	}
}

func BenchmarkSelect_Raw_1K(b *testing.B)   { benchmarkSelectRaw(b, 1000) }
func BenchmarkSelect_Raw_100K(b *testing.B) { benchmarkSelectRaw(b, 1000000) }

func benchmarkSelectRaw(b *testing.B, pointN int) {
	mapper := mock.NewShardMapper(func(stub *mock.ShardMapper) {
		stub.ShardsByTimeRangeFn = func(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
			return []meta.ShardInfo{{ID: 1}}, nil
		}
		stub.ShardGroupFn = func(ids []uint64) query.ShardGroup {
			if diff := cmp.Diff(ids, []uint64{1}); diff != "" {
				b.Fatalf("unexpected shard ids:\n%s", diff)
			}
			return &mock.ShardGroup{
				Measurements: map[string]mock.ShardMeta{
					"cpu": {
						Fields: map[string]influxql.DataType{
							"fval": influxql.Float,
						},
					},
				},
				CreateIteratorFn: func(name string, opt influxql.IteratorOptions) (influxql.Iterator, error) {
					if name != "cpu" {
						return nil, fmt.Errorf("unexpected source: %s", name)
					}
					return NewRawBenchmarkIterator(pointN, opt), nil
				},
			}
		}
	})
	benchmarkSelect(b, `SELECT fval FROM cpu`, mapper)
}

func benchmarkSelect(b *testing.B, s string, mapper query.ShardMapper) {
	mustParseTime := func(value string) time.Time {
		ts, err := time.Parse(time.RFC3339, value)
		if err != nil {
			b.Fatalf("unable to parse time: %s", err)
		}
		return ts
	}
	now := mustParseTime("2000-01-01T00:00:00Z")

	stmt, err := influxql.ParseStatement(s)
	if err != nil {
		b.Fatalf("unable to parse statement: %s", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		opt := query.CompileOptions{Now: now}
		c, err := query.Compile(stmt.(*influxql.SelectStatement), opt)
		if err != nil {
			b.Fatalf("unable to compile statement: %s", err)
		}

		fields, _, err := c.Select(mapper)
		if err != nil {
			b.Fatalf("unable to link statement: %s", err)
		}

		if err := query.ExecuteTargets(fields, func(n query.Node) error {
			return n.Execute()
		}); err != nil {
			b.Fatalf("error while executing the plan: %s", err)
		}

		itrs := make([]influxql.Iterator, len(fields))
		for i, f := range fields {
			itrs[i] = f.Iterator()
		}
		influxql.DrainIterators(itrs)
	}
}

// NewRawBenchmarkIteratorCreator returns a new mock iterator creator with generated fields.
func NewRawBenchmarkIterator(pointN int, opt influxql.IteratorOptions) influxql.Iterator {
	if opt.Expr != nil {
		panic("unexpected expression")
	}

	p := influxql.FloatPoint{
		Name: "cpu",
		Aux:  make([]interface{}, len(opt.Aux)),
	}

	for i := range opt.Aux {
		switch opt.Aux[i].Val {
		case "fval":
			p.Aux[i] = float64(100)
		default:
			panic("unknown iterator expr: " + opt.Expr.String())
		}
	}

	return &mock.FloatPointGenerator{N: pointN, Fn: func(i int) *influxql.FloatPoint {
		p.Time = int64(i) * 10 * Second
		return &p
	}}
}

func benchmarkSelectTop(b *testing.B, seriesN, pointsPerSeries int) {
	mapper := mock.NewShardMapper(func(stub *mock.ShardMapper) {
		stub.ShardsByTimeRangeFn = func(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
			return []meta.ShardInfo{{ID: 1}}, nil
		}
		stub.ShardGroupFn = func(ids []uint64) query.ShardGroup {
			if diff := cmp.Diff(ids, []uint64{1}); diff != "" {
				b.Fatalf("unexpected shard ids:\n%s", diff)
			}
			return &mock.ShardGroup{
				Measurements: map[string]mock.ShardMeta{
					"cpu": {
						Fields: map[string]influxql.DataType{
							"sval": influxql.Float,
						},
					},
				},
				CreateIteratorFn: func(name string, opt influxql.IteratorOptions) (influxql.Iterator, error) {
					if name != "cpu" {
						return nil, fmt.Errorf("unexpected source: %s", name)
					}

					p := influxql.FloatPoint{
						Name: "cpu",
					}

					return &mock.FloatPointGenerator{N: seriesN * pointsPerSeries, Fn: func(i int) *influxql.FloatPoint {
						p.Value = float64(rand.Int63())
						p.Time = int64(time.Duration(i) * (10 * time.Second))
						return &p
					}}, nil
				},
			}
		}
	})
	benchmarkSelect(b, `SELECT top(sval, 10) FROM cpu`, mapper)
}

func BenchmarkSelect_Top_1K(b *testing.B) { benchmarkSelectTop(b, 1000, 1000) }
