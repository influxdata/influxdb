package promqltests

import (
	"bytes"
	"fmt"
	"text/template"
	"time"
)

type TestCase struct {
	Text           string
	SkipComparison string
	Skip           string
	ShouldFail     bool
}

var (
	InputPath  = "input.txt"
	Start      = time.Unix(1550767830, 123000000).UTC()
	End        = time.Unix(1550767900, 321000000).UTC()
	Resolution = 10 * time.Second
	TestCases  = getTestCases()

	testVariantArgs = map[string][]string{
		"range":  {"1s", "15s", "1m", "5m", "15m", "1h"},
		"offset": {"1m", "5m", "10m"},
		// TODO(affo): PromQL "stdvar" makes "pow" appear in the transpiled Flux query: undefined identifier "pow".
		//  See issue https://github.com/influxdata/flux/issues/1926.
		"simpleAggrOp": {"sum", "avg", "max", "min", "count", "stddev"}, //, "stdvar"},
		"topBottomOp":  {"topk", "bottomk"},
		"quantile": {
			// TODO: Should return -Inf.
			// "-0.5",
			"0.1",
			"0.5",
			"0.75",
			"0.95",
			"0.90",
			"0.99",
			"1",
			// TODO: Should return +Inf.
			// "1.5",
		},
		"arithBinOp":           {"+", "-", "*", "/", "%", "^"},
		"compBinOp":            {"==", "!=", "<", ">", "<=", ">="},
		"binOp":                {"+", "-", "*", "/", "%", "^", "==", "!=", "<", ">", "<=", ">="},
		"simpleMathFunc":       {"abs", "ceil", "floor", "exp", "sqrt", "ln", "log2", "log10", "round"},
		"extrapolatedRateFunc": {"delta", "rate", "increase"},
		"clampFunc":            {"clamp_min", "clamp_max"},
		"instantRateFunc":      {"idelta"},
		"dateFunc":             {"day_of_month", "day_of_week", "days_in_month", "hour", "minute", "month", "year"},
		"smoothingFactor":      {"0.1", "0.5", "0.8"},
		"trendFactor":          {"0.1", "0.5", "0.8"},
	}
	queries = []struct {
		Query       string
		VariantArgs []string
		// Needed for subqueries, which will never return 100% identical results.
		SkipComparison string
		Skip           string
		ShouldFail     bool
	}{
		// Vector selectors.
		{
			Query: `demo_cpu_usage_seconds_total`,
		},
		{
			Query: `{__name__="demo_cpu_usage_seconds_total"}`,
		},
		{
			Query: `demo_cpu_usage_seconds_total{mode="idle"}`,
		},
		{
			Query: `demo_cpu_usage_seconds_total{mode!="idle"}`,
		},
		{
			Query: `demo_cpu_usage_seconds_total{instance=~"localhost:.*"}`,
		},
		{
			Query: `demo_cpu_usage_seconds_total{instance=~"host"}`,
		},
		{
			Query: `demo_cpu_usage_seconds_total{instance!~".*:10000"}`,
		},
		{
			Query: `demo_cpu_usage_seconds_total{mode="idle", instance!="localhost:10000"}`,
		},
		{
			Query: `{mode="idle", instance!="localhost:10000"}`,
		},
		{
			Query: "nonexistent_metric_name",
		},
		{
			Query:       `demo_cpu_usage_seconds_total offset {{.offset}}`,
			VariantArgs: []string{"offset"},
		},
		// Aggregation operators.
		{
			Query:       `{{.simpleAggrOp}} (demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"simpleAggrOp"},
		},
		{
			Query:       `{{.simpleAggrOp}} by() (demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"simpleAggrOp"},
		},
		{
			Query:       `{{.simpleAggrOp}} by(instance) (demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"simpleAggrOp"},
		},
		{
			Query:       `{{.simpleAggrOp}} by(instance, mode) (demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"simpleAggrOp"},
		},
		// TODO: grouping by non-existent columns is not supported in Flux: https://github.com/influxdata/flux/issues/1117
		{
			Query:       `{{.simpleAggrOp}} by(nonexistent) (demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"simpleAggrOp"},
			Skip:        "grouping by non-existent columns is not supported in Flux: https://github.com/influxdata/flux/issues/1117",
		},
		{
			Query:       `{{.simpleAggrOp}} without() (demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"simpleAggrOp"},
		},
		// TODO: Need to handle & test external injections of special column names more systematically.
		{
			Query:       `{{.simpleAggrOp}} without(_value) (demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"simpleAggrOp"},
		},
		{
			Query:       `{{.simpleAggrOp}} without(instance) (demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"simpleAggrOp"},
		},
		{
			Query:       `{{.simpleAggrOp}} without(instance, mode) (demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"simpleAggrOp"},
		},
		{
			Query:       `{{.simpleAggrOp}} without(nonexistent) (demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"simpleAggrOp"},
		},
		{
			Query:       `{{.topBottomOp}} (3, demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"topBottomOp"},
		},
		{
			Query:       `{{.topBottomOp}} by(instance) (2, demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"topBottomOp"},
		},
		{
			Query:       `quantile({{.quantile}}, demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"quantile"},
		},
		{
			Query: `avg(max by(mode) (demo_cpu_usage_seconds_total))`,
		},
		{
			Query: `count(go_memstats_heap_released_bytes_total)`,
		},
		// Binary operators.
		{
			Query: `1 * 2 + 4 / 6 - 10`,
		},
		{
			Query:       `demo_num_cpus + (1 {{.compBinOp}} bool 2)`,
			VariantArgs: []string{"compBinOp"},
		},
		{
			Query: `-demo_cpu_usage_seconds_total`,
		},
		{
			Query:       `demo_cpu_usage_seconds_total {{.binOp}} 1.2345`,
			VariantArgs: []string{"binOp"},
		},
		{
			Query:       `demo_cpu_usage_seconds_total {{.compBinOp}} bool 1.2345`,
			VariantArgs: []string{"compBinOp"},
		},
		{
			Query:       `1.2345 {{.compBinOp}} bool demo_cpu_usage_seconds_total`,
			VariantArgs: []string{"compBinOp"},
		},
		{
			Query:       `0.12345 {{.binOp}} demo_cpu_usage_seconds_total`,
			VariantArgs: []string{"binOp"},
		},
		{
			Query:       `(1 * 2 + 4 / 6 - (10%7)^2) {{.binOp}} demo_cpu_usage_seconds_total`,
			VariantArgs: []string{"binOp"},
		},
		{
			Query:       `demo_cpu_usage_seconds_total {{.binOp}} (1 * 2 + 4 / 6 - 10)`,
			VariantArgs: []string{"binOp"},
		},
		// TODO: Check this systematically for every node type.
		{
			// Check that vector-vector binops preserve time fields required by aggregations.
			Query: `sum(demo_cpu_usage_seconds_total / on(instance, job, mode) demo_cpu_usage_seconds_total)`,
		},
		{
			// Check that scalar-vector binops sets _time field to the window's _stop.
			Query: `timestamp(demo_cpu_usage_seconds_total * 1)`,
		},
		{
			// Check that unary minus sets _time field to the window's _stop.
			Query: `timestamp(-demo_cpu_usage_seconds_total)`,
		},
		// TODO: Blocked on new join() implementation. https://github.com/influxdata/flux/issues/1219
		{
			Query:       `demo_cpu_usage_seconds_total {{.binOp}} on(instance, job, mode) demo_cpu_usage_seconds_total`,
			VariantArgs: []string{"binOp"},
			Skip:        "blocked on new join() implementation. https://github.com/influxdata/flux/issues/1219",
		},
		{
			Query:       `sum by(instance, mode) (demo_cpu_usage_seconds_total) {{.binOp}} on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
			VariantArgs: []string{"binOp"},
			Skip:        "blocked on new join() implementation. https://github.com/influxdata/flux/issues/1219",
		},
		{
			Query:       `demo_cpu_usage_seconds_total {{.compBinOp}} bool on(instance, job, mode) demo_cpu_usage_seconds_total`,
			VariantArgs: []string{"compBinOp"},
			Skip:        "blocked on new join() implementation. https://github.com/influxdata/flux/issues/1219",
		},
		{
			// Check that __name__ is always dropped, even if it's part of the matching labels.
			Query: `demo_cpu_usage_seconds_total / on(instance, job, mode, __name__) demo_cpu_usage_seconds_total`,
			Skip:  "Blocked on new join() implementation. https://github.com/influxdata/flux/issues/1219",
		},
		{
			Query: `sum without(job) (demo_cpu_usage_seconds_total) / on(instance, mode) demo_cpu_usage_seconds_total`,
			Skip:  "Blocked on new join() implementation. https://github.com/influxdata/flux/issues/1219",
		},
		{
			Query: `sum without(job) (demo_cpu_usage_seconds_total) / on(instance, mode) group_left demo_cpu_usage_seconds_total`,
			Skip:  "Blocked on new join() implementation. https://github.com/influxdata/flux/issues/1219",
		},
		{
			Query: `sum without(job) (demo_cpu_usage_seconds_total) / on(instance, mode) group_left(job) demo_cpu_usage_seconds_total`,
			Skip:  "Blocked on new join() implementation. https://github.com/influxdata/flux/issues/1219",
		},
		{
			Query: `demo_cpu_usage_seconds_total / on(instance, job) group_left demo_num_cpus`,
			Skip:  "Blocked on new join() implementation. https://github.com/influxdata/flux/issues/1219",
		},
		//TODO: See https://github.com/influxdata/flux/issues/1118
		{
			Query: `demo_cpu_usage_seconds_total / on(instance, mode, job, non_existent) demo_cpu_usage_seconds_total`,
			Skip:  "See https://github.com/influxdata/flux/issues/1118",
		},
		// TODO: Add non-explicit many-to-one / one-to-many that errors.
		// TODO: Add many-to-many that errors.
		// TODO: NaN/Inf/-Inf support: https://github.com/influxdata/flux/issues/1040
		{
			Query: `demo_num_cpus * Inf`,
			Skip:  "NaN/Inf/-Inf support: https://github.com/influxdata/flux/issues/1040",
		},
		{
			Query: `demo_num_cpus * -Inf`,
			Skip:  "NaN/Inf/-Inf support: https://github.com/influxdata/flux/issues/1040",
		},
		{
			Query: `demo_num_cpus * NaN`,
			Skip:  "NaN/Inf/-Inf support: https://github.com/influxdata/flux/issues/1040",
		},
		// Unary expressions.
		{
			Query: `demo_cpu_usage_seconds_total + -(1 + 1)`,
		},
		// Binops involving non-const scalars.
		{
			Query:       "1 {{.arithBinOp}} time()",
			VariantArgs: []string{"arithBinOp"},
		},
		{
			Query:       "time() {{.arithBinOp}} 1",
			VariantArgs: []string{"arithBinOp"},
		},
		{
			Query:       "time() {{.compBinOp}} bool 1",
			VariantArgs: []string{"compBinOp"},
		},
		{
			Query:       "1 {{.compBinOp}} bool time()",
			VariantArgs: []string{"compBinOp"},
		},
		{
			Query:       "time() {{.arithBinOp}} time()",
			VariantArgs: []string{"arithBinOp"},
		},
		{
			Query:       "time() {{.compBinOp}} bool time()",
			VariantArgs: []string{"compBinOp"},
		},
		// TODO(affo): add reason for skipping
		{
			Query:       "time() {{.binOp}} demo_cpu_usage_seconds_total",
			VariantArgs: []string{"binOp"},
			Skip:        "unknown reason",
		},
		{
			Query:       "demo_cpu_usage_seconds_total {{.binOp}} time()",
			VariantArgs: []string{"binOp"},
			Skip:        "unknown reason",
		},
		// Functions.
		{
			Query:       `{{.simpleAggrOp}}_over_time(demo_cpu_usage_seconds_total[{{.range}}])`,
			VariantArgs: []string{"simpleAggrOp", "range"},
		},
		{
			Query:       `quantile_over_time({{.quantile}}, demo_cpu_usage_seconds_total[{{.range}}])`,
			VariantArgs: []string{"quantile", "range"},
		},
		{
			Query: `timestamp(demo_num_cpus)`,
		},
		{
			Query: `timestamp(timestamp(demo_num_cpus))`,
		},
		{
			Query:       `{{.simpleMathFunc}}(demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"simpleMathFunc"},
		},
		{
			Query:       `{{.simpleMathFunc}}(-demo_cpu_usage_seconds_total)`,
			VariantArgs: []string{"simpleMathFunc"},
		},
		{
			Query:       "{{.extrapolatedRateFunc}}(nonexistent_metric[5m])",
			VariantArgs: []string{"extrapolatedRateFunc"},
		},
		{
			Query:       "{{.extrapolatedRateFunc}}(demo_cpu_usage_seconds_total[{{.range}}])",
			VariantArgs: []string{"extrapolatedRateFunc", "range"},
		},
		{
			Query:       `deriv(demo_disk_usage_bytes[{{.range}}])`,
			VariantArgs: []string{"range"},
		},
		{
			Query:       `predict_linear(demo_disk_usage_bytes[{{.range}}], 600)`,
			VariantArgs: []string{"range"},
		},
		{
			Query: "time()",
		},
		{
			Query: `label_replace(demo_num_cpus, "_value", "destination-value-$1", "instance", ".*")`,
		},
		{
			// label_replace does a full-string match and replace.
			Query: `label_replace(demo_num_cpus, "job", "destination-value-$1", "instance", "localhost:(.*)")`,
		},
		{
			// label_replace does not do a sub-string match.
			Query: `label_replace(demo_num_cpus, "job", "destination-value-$1", "instance", "host:(.*)")`,
		},
		{
			// label_replace works with multiple capture groups.
			Query: `label_replace(demo_num_cpus, "job", "$1-$2", "instance", "local(.*):(.*)")`,
		},
		{
			// label_replace does not overwrite the destination label if the source label does not exist.
			Query: `label_replace(demo_num_cpus, "job", "value-$1", "nonexistent-src", "source-value-(.*)")`,
		},
		{
			// label_replace overwrites the destination label if the source label is empty, but matched.
			Query: `label_replace(demo_num_cpus, "job", "value-$1", "nonexistent-src", "(.*)")`,
		},
		{
			// label_replace does not overwrite the destination label if the source label is not matched.
			Query: `label_replace(demo_num_cpus, "job", "value-$1", "instance", "non-matching-regex")`,
		},
		{
			// label_replace drops labels that are set to empty values.
			Query: `label_replace(demo_num_cpus, "job", "", "dst", ".*")`,
		},
		{
			// label_replace fails when the regex is invalid.
			Query:      `label_replace(demo_num_cpus, "job", "value-$1", "src", "(.*")`,
			ShouldFail: true,
		},
		{
			// label_replace fails when the destination label name is not a valid Prometheus label name.
			Query:      `label_replace(demo_num_cpus, "~invalid", "", "src", "(.*)")`,
			ShouldFail: true,
		},
		{
			// label_replace fails when there would be duplicated identical output label sets.
			Query:      `label_replace(demo_num_cpus, "instance", "", "", "")`,
			ShouldFail: true,
		},
		{
			Query: `label_join(demo_num_cpus, "new_label", "-", "instance", "job")`,
		},
		{
			Query: `label_join(demo_num_cpus, "job", "-", "instance", "job")`,
		},
		{
			Query: `label_join(demo_num_cpus, "job", "-", "instance")`,
		},
		{
			Query:      `label_join(demo_num_cpus, "~invalid", "-", "instance")`,
			ShouldFail: true,
		},
		{
			Query: `label_join(demo_num_cpus, "_value", "-", "_stop")`,
		},
		{
			Query:       `{{.dateFunc}}()`,
			VariantArgs: []string{"dateFunc"},
		},
		{
			Query:       `{{.dateFunc}}(demo_batch_last_success_timestamp_seconds)`,
			VariantArgs: []string{"dateFunc"},
		},
		{
			Query:       `{{.instantRateFunc}}(demo_cpu_usage_seconds_total[{{.range}}])`,
			VariantArgs: []string{"instantRateFunc", "range"},
		},
		{
			Query:       `{{.clampFunc}}(demo_cpu_usage_seconds_total, 2)`,
			VariantArgs: []string{"clampFunc"},
		},
		{
			Query:       `resets(demo_cpu_usage_seconds_total[{{.range}}])`,
			VariantArgs: []string{"range"},
		},
		{
			Query:       `changes(demo_batch_last_success_timestamp_seconds[{{.range}}])`,
			VariantArgs: []string{"range"},
		},
		{
			Query: `vector(1)`,
		},
		{
			Query: `vector(time())`,
		},
		{
			Query:       `histogram_quantile({{.quantile}}, rate(demo_api_request_duration_seconds_bucket[1m]))`,
			VariantArgs: []string{"quantile"},
		},
		{
			Query: `histogram_quantile(0.9, nonexistent_metric)`,
		},
		{
			// Missing "le" label.
			Query: `histogram_quantile(0.9, demo_cpu_usage_seconds_total)`,
		},
		{
			// Missing "le" label only in some series of the same grouping.
			Query: `histogram_quantile(0.9, {__name__=~"demo_api_request_duration_seconds_.+"})`,
		},
		{
			Query:       `holt_winters(demo_disk_usage_bytes[10m], {{.smoothingFactor}}, {{.trendFactor}})`,
			VariantArgs: []string{"smoothingFactor", "trendFactor"},
		},
		{
			Query:          `max_over_time((time() - max(demo_batch_last_success_timestamp_seconds) < 1000)[5m:10s] offset 5m)`,
			SkipComparison: "the implementation cannot guarantee completely identical results for subqueries",
		},
		{
			Query:          `avg_over_time(rate(demo_cpu_usage_seconds_total[1m])[2m:10s])`,
			SkipComparison: "the implementation cannot guarantee completely identical results for subqueries",
		},
	}
)

// tprintf replaces template arguments in a string with their instantiations from the provided map.
func tprintf(tmpl string, data map[string]string) string {
	t := template.Must(template.New("Query").Parse(tmpl))
	buf := &bytes.Buffer{}
	if err := t.Execute(buf, data); err != nil {
		panic(err)
	}
	return buf.String()
}

// getVariants returns every possible combinations (variants) of a template query.
func getVariants(query string, variantArgsLeft []string, args map[string]string) []string {
	// Either this Query had no variants defined to begin with or they have
	// been fully filled out in "args" from recursive parent calls.
	if len(variantArgsLeft) == 0 {
		return []string{tprintf(query, args)}
	}

	// Recursively iterate through the values for each variant arg dimension,
	// selecting one dimension (arg) to vary per recursion level and let the
	// other recursion levels iterate through the remaining dimensions until
	// all args are defined.
	queries := make([]string, 0)
	for _, vArg := range variantArgsLeft {
		filteredVArgs := make([]string, 0, len(variantArgsLeft)-1)
		for _, va := range variantArgsLeft {
			if va != vArg {
				filteredVArgs = append(filteredVArgs, va)
			}
		}

		vals := testVariantArgs[vArg]
		if len(vals) == 0 {
			panic(fmt.Errorf("unknown variant arg %q", vArg))
		}
		for _, variantVal := range vals {
			args[vArg] = variantVal
			qs := getVariants(query, filteredVArgs, args)
			queries = append(queries, qs...)
		}
	}
	return queries
}

// getTestCases returns the test cases for this suite.
func getTestCases() []TestCase {
	tcs := make([]TestCase, 0)
	for _, q := range queries {
		vs := getVariants(q.Query, q.VariantArgs, make(map[string]string))
		for _, v := range vs {
			tc := TestCase{
				Text:           v,
				SkipComparison: q.SkipComparison,
				Skip:           q.Skip,
				ShouldFail:     q.ShouldFail,
			}
			tcs = append(tcs, tc)
		}
	}
	return tcs
}
