package testing

var FluxEndToEndSkipList = map[string]map[string]string{
	"universe": {
		// TODO(adam) determine the reason for these test failures.
		"cov":                      "Reason TBD",
		"covariance":               "Reason TBD",
		"cumulative_sum":           "Reason TBD",
		"cumulative_sum_default":   "Reason TBD",
		"cumulative_sum_noop":      "Reason TBD",
		"drop_non_existent":        "Reason TBD",
		"first":                    "Reason TBD",
		"highestAverage":           "Reason TBD",
		"highestMax":               "Reason TBD",
		"histogram":                "Reason TBD",
		"histogram_normalize":      "Reason TBD",
		"histogram_quantile":       "Reason TBD",
		"join":                     "Reason TBD",
		"join_across_measurements": "Reason TBD",
		"join_agg":                 "Reason TBD",
		"keep_non_existent":        "Reason TBD",
		"key_values":               "Reason TBD",
		"key_values_host_name":     "Reason TBD",
		"last":                     "Reason TBD",
		"lowestAverage":            "Reason TBD",
		"max":                      "Reason TBD",
		"min":                      "Reason TBD",
		"sample":                   "Reason TBD",
		"selector_preserve_time":   "Reason TBD",
		"shift":                    "Reason TBD",
		"shift_negative_duration":  "Reason TBD",
		"task_per_line":            "Reason TBD",
		"top":                      "Reason TBD",
		"union":                    "Reason TBD",
		"union_heterogeneous":      "Reason TBD",
		"unique":                   "Reason TBD",
		"distinct":                 "Reason TBD",

		// it appears these occur when writing the input data.  `to` may not be null safe.
		"fill_bool":   "failed to read meta data: panic: interface conversion: interface {} is nil, not uint64",
		"fill_float":  "failed to read meta data: panic: interface conversion: interface {} is nil, not uint64",
		"fill_int":    "failed to read meta data: panic: interface conversion: interface {} is nil, not uint64",
		"fill_string": "failed to read meta data: panic: interface conversion: interface {} is nil, not uint64",
		"fill_time":   "failed to read meta data: panic: interface conversion: interface {} is nil, not uint64",
		"fill_uint":   "failed to read meta data: panic: interface conversion: interface {} is nil, not uint64",
		"window_null": "failed to read meta data: panic: interface conversion: interface {} is nil, not float64",

		// these may just be missing calls to range() in the tests.  easy to fix in a new PR.
		"group_nulls":         "unbounded test",
		"integral":            "unbounded test",
		"integral_columns":    "unbounded test",
		"map":                 "unbounded test",
		"join_missing_on_col": "unbounded test",
		"join_use_previous":   "unbounded test (https://github.com/influxdata/flux/issues/2996)",
		"join_panic":          "unbounded test (https://github.com/influxdata/flux/issues/3465)",
		"rowfn_with_import":   "unbounded test",

		// the following tests have a difference between the CSV-decoded input table, and the storage-retrieved version of that table
		"columns":            "group key mismatch",
		"set":                "column order mismatch",
		"simple_max":         "_stop missing from expected output",
		"derivative":         "time bounds mismatch (engine uses now() instead of bounds on input table)",
		"difference_columns": "data write/read path loses columns x and y",
		"keys":               "group key mismatch",

		// failed to read meta data errors: the CSV encoding is incomplete probably due to data schema errors.  needs more detailed investigation to find root cause of error
		// "filter_by_regex":             "failed to read metadata",
		// "filter_by_tags":              "failed to read metadata",
		"group":                       "failed to read metadata",
		"group_except":                "failed to read metadata",
		"group_ungroup":               "failed to read metadata",
		"pivot_mean":                  "failed to read metadata",
		"histogram_quantile_minvalue": "failed to read meta data: no column with label _measurement exists",
		"increase":                    "failed to read meta data: table has no _value column",

		"string_max":                  "error: invalid use of function: *functions.MaxSelector has no implementation for type string (https://github.com/influxdata/platform/issues/224)",
		"null_as_value":               "null not supported as value in influxql (https://github.com/influxdata/platform/issues/353)",
		"string_interp":               "string interpolation not working as expected in flux (https://github.com/influxdata/platform/issues/404)",
		"to":                          "to functions are not supported in the testing framework (https://github.com/influxdata/flux/issues/77)",
		"covariance_missing_column_1": "need to support known errors in new test framework (https://github.com/influxdata/flux/issues/536)",
		"covariance_missing_column_2": "need to support known errors in new test framework (https://github.com/influxdata/flux/issues/536)",
		"drop_before_rename":          "need to support known errors in new test framework (https://github.com/influxdata/flux/issues/536)",
		"drop_referenced":             "need to support known errors in new test framework (https://github.com/influxdata/flux/issues/536)",
		"yield":                       "yield requires special test case (https://github.com/influxdata/flux/issues/535)",

		"window_group_mean_ungroup": "window trigger optimization modifies sort order of its output tables (https://github.com/influxdata/flux/issues/1067)",

		"median_column": "failing in different ways (https://github.com/influxdata/influxdb/issues/13909)",
		"dynamic_query": "tableFind does not work in e2e tests: https://github.com/influxdata/influxdb/issues/13975",

		"to_int":  "dateTime conversion issue: https://github.com/influxdata/influxdb/issues/14575",
		"to_uint": "dateTime conversion issue: https://github.com/influxdata/influxdb/issues/14575",

		"holt_winters_panic": "Expected output is an empty table which breaks the testing framework (https://github.com/influxdata/influxdb/issues/14749)",
		"map_nulls":          "to cannot write null values",
	},
	"array": {
		"from":       "test not meant to be consumed by influxdb",
		"from_group": "test not meant to be consumed by influxdb",
	},
	"experimental": {
		"set":                "Reason TBD",
		"join":               "unbounded test",
		"alignTime":          "unbounded test",
		"histogram_quantile": "mis-named columns for storage",
		"distinct":           "failing test",
		"fill":               "failing test",
		"histogram":          "failing test",
		"unique":             "failing test",
	},
	"experimental/oee": {
		"apq":        "failing test",
		"computeapq": "failing test",
	},
	"experimental/geo": {
		"filterRowsNotStrict": "tableFind does not work in e2e tests: https://github.com/influxdata/influxdb/issues/13975",
		"filterRowsStrict":    "tableFind does not work in e2e tests: https://github.com/influxdata/influxdb/issues/13975",
		"gridFilterLevel":     "tableFind does not work in e2e tests: https://github.com/influxdata/influxdb/issues/13975",
		"gridFilter":          "tableFind does not work in e2e tests: https://github.com/influxdata/influxdb/issues/13975",
		"groupByArea":         "tableFind does not work in e2e tests: https://github.com/influxdata/influxdb/issues/13975",
		"filterRowsPivoted":   "tableFind does not work in e2e tests: https://github.com/influxdata/influxdb/issues/13975",
		"shapeDataWithFilter": "tableFind does not work in e2e tests: https://github.com/influxdata/influxdb/issues/13975",
		"shapeData":           "test run before to() is finished: https://github.com/influxdata/influxdb/issues/13975",
	},
	"regexp": {
		"replaceAllString": "Reason TBD",
	},
	"http": {
		"http_endpoint": "need ability to test side effects in e2e tests: (https://github.com/influxdata/flux/issues/1723)",
	},
	"influxdata/influxdb/schema": {
		"show_tag_keys": "failing due to bug in test, unskip this after upgrading from Flux v0.91.0",
	},
	"influxdata/influxdb/monitor": {
		"state_changes_big_any_to_any":     "unbounded test",
		"state_changes_big_info_to_ok":     "unbounded test",
		"state_changes_big_ok_to_info":     "unbounded test",
		"state_changes_any_to_any":         "test run before to() is finished: https://github.com/influxdata/influxdb/issues/13975",
		"state_changes_info_to_any":        "test run before to() is finished: https://github.com/influxdata/influxdb/issues/13975",
		"state_changes_invalid_any_to_any": "test run before to() is finished: https://github.com/influxdata/influxdb/issues/13975",
		"state_changes":                    "test run before to() is finished: https://github.com/influxdata/influxdb/issues/13975",
	},
	"influxdata/influxdb/secrets": {
		"secrets": "Cannot inject custom deps into the test framework so the secrets don't lookup correctly",
	},
	"internal/promql": {
		"join": "unbounded test",
	},
	"testing/chronograf": {
		"buckets":                "unbounded test",
		"aggregate_window_count": "flakey test: https://github.com/influxdata/influxdb/issues/18463",
	},
	"testing/kapacitor": {
		"fill_default": "unknown field type for f1",
	},
	"testing/pandas": {
		"extract_regexp_findStringIndex": "pandas. map does not correctly handled returned arrays (https://github.com/influxdata/flux/issues/1387)",
		"partition_strings_splitN":       "pandas. map does not correctly handled returned arrays (https://github.com/influxdata/flux/issues/1387)",
	},
	"testing/promql": {
		"emptyTable":                    "tests a source",
		"year":                          "flakey test: https://github.com/influxdata/influxdb/issues/15667",
		"extrapolatedRate_counter_rate": "option \"testing.loadStorage\" reassigned: https://github.com/influxdata/flux/issues/3155",
		"extrapolatedRate_nocounter":    "option \"testing.loadStorage\" reassigned: https://github.com/influxdata/flux/issues/3155",
		"extrapolatedRate_norate":       "option \"testing.loadStorage\" reassigned: https://github.com/influxdata/flux/issues/3155",
		"linearRegression_nopredict":    "option \"testing.loadStorage\" reassigned: https://github.com/influxdata/flux/issues/3155",
		"linearRegression_predict":      "option \"testing.loadStorage\" reassigned: https://github.com/influxdata/flux/issues/3155",
	},
	"testing/influxql": {
		"cumulative_sum": "invalid test data requires loadStorage to be overridden. See https://github.com/influxdata/flux/issues/3145",
		"elapsed":        "failing since split with Flux upgrade: https://github.com/influxdata/influxdb/issues/19568",
	},
	"contrib/RohanSreerama5/naiveBayesClassifier": {
		"bayes": "error calling tableFind: ",
	},
}

type PerTestFeatureFlagMap = map[string]map[string]map[string]string

var FluxEndToEndFeatureFlags = PerTestFeatureFlagMap{}
