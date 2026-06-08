// Package promtest provides helpers for parsing and extracting prometheus metrics.
// These functions are only intended to be called from test files,
// as there is a dependency on the standard library testing package.
package promtest

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// FromHTTPResponse parses the prometheus metrics from the given *http.Response.
// It relies on properly set response headers to correctly parse.
// It will unconditionally close the response body.
//
// This is particularly helpful when testing the output of the /metrics endpoint of a service.
// However, for comprehensive testing of metrics, it usually makes more sense to
// add collectors to a registry and call Registry.Gather to get the metrics without involving HTTP.
func FromHTTPResponse(r *http.Response) ([]*dto.MetricFamily, error) {
	defer r.Body.Close()

	dec := expfmt.NewDecoder(r.Body, expfmt.ResponseFormat(r.Header))
	var mfs []*dto.MetricFamily
	for {
		mf := new(dto.MetricFamily)
		if err := dec.Decode(mf); err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}

		mfs = append(mfs, mf)
	}

	return mfs, nil
}

// FindMetric iterates through mfs to find the first metric family matching name.
// If a metric family matches, then the metrics inside the family are searched,
// and the first metric whose labels match the given labels are returned.
// If no matches are found, FindMetric returns nil.
//
// FindMetric assumes that the labels on the metric family are well formed,
// i.e. there are no duplicate label names, and the label values are not empty strings.
func FindMetric(mfs []*dto.MetricFamily, name string, labels map[string]string) *dto.Metric {
	_, m := findMetric(mfs, name, labels)
	return m
}

// MustFindMetric returns the matching metric, or if no matching metric could be found,
// it calls tb.Log with helpful output of what was actually available, before calling tb.FailNow.
func MustFindMetric(tb testing.TB, mfs []*dto.MetricFamily, name string, labels map[string]string) *dto.Metric {
	tb.Helper()

	fam, m := findMetric(mfs, name, labels)
	if fam == nil {
		tb.Logf("metric family with name %q not found", name)
		tb.Log("available names:")
		for _, mf := range mfs {
			tb.Logf("\t%s", mf.GetName())
		}
		tb.FailNow()
		return nil // Need an explicit return here for test.
	}

	if m == nil {
		tb.Logf("found metric family with name %q, but metric with labels %v not found", name, labels)
		tb.Logf("available labels on metric family %q:", name)

		for _, m := range fam.Metric {
			pairs := make([]string, len(m.Label))
			for i, l := range m.Label {
				pairs[i] = fmt.Sprintf("%q: %q", l.GetName(), l.GetValue())
			}
			tb.Logf("\t%s", strings.Join(pairs, ", "))
		}
		tb.FailNow()
		return nil // Need an explicit return here for test.
	}

	return m
}

// findMetric is a helper that returns the matching family and the matching metric.
// The exported FindMetric function specifically only finds the metric, not the family,
// but for test it is more helpful to identify whether the family was matched.
func findMetric(mfs []*dto.MetricFamily, name string, labels map[string]string) (*dto.MetricFamily, *dto.Metric) {
	var fam *dto.MetricFamily

	for _, mf := range mfs {
		if mf.GetName() == name {
			fam = mf
			break
		}
	}

	if fam == nil {
		// No family matching the name.
		return nil, nil
	}

	for _, m := range fam.Metric {
		if len(m.Label) != len(labels) {
			continue
		}

		match := true
		for _, l := range m.Label {
			if labels[l.GetName()] != l.GetValue() {
				match = false
				break
			}
		}

		if !match {
			continue
		}

		// All labels matched.
		return fam, m
	}

	// Didn't find a metric whose labels all matched.
	return fam, nil
}

// CountMetricsByLabel gathers from g and returns, keyed by metric family name,
// the number of series in each family that carry the given label name=value.
// Families with no matching series are omitted, so the returned map is empty
// when nothing matches.
//
// This is useful for asserting that a whole set of series sharing one label
// (e.g. all of a single shard's series, keyed by its unique "path" label) is
// either present or fully removed across many metric families at once. Unlike
// FindMetric, it matches on a single label rather than an exact full label set.
func CountMetricsByLabel(tb testing.TB, g prometheus.Gatherer, label, value string) map[string]int {
	tb.Helper()

	out := make(map[string]int)
	for _, mf := range MustGather(tb, g) {
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == value {
					out[mf.GetName()]++
				}
			}
		}
	}
	return out
}

// MustGather calls g.Gather and calls tb.Fatal if there was an error.
func MustGather(tb testing.TB, g prometheus.Gatherer) []*dto.MetricFamily {
	tb.Helper()

	mfs, err := g.Gather()
	if err != nil {
		tb.Fatalf("error while gathering metrics: %v", err)
		return nil // Need an explicit return here for test.
	}

	return mfs
}
