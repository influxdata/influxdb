package prometheus

import (
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

var _ prometheus.Gatherer = (*Filter)(nil)

// Filter filters the metrics from Gather using Matcher.
type Filter struct {
	Gatherer prometheus.Gatherer
	Matcher  Matcher
}

// Gather filters all metrics to only those that match the Matcher.
func (f *Filter) Gather() ([]*dto.MetricFamily, error) {
	mfs, err := f.Gatherer.Gather()
	if err != nil {
		return nil, err
	}
	return f.Matcher.Match(mfs), nil
}

// Matcher is used to match families of prometheus metrics.
type Matcher map[string]Labels // family name to label/value

// NewMatcher returns a new matcher.
func NewMatcher() Matcher {
	return Matcher{}
}

// Family helps construct match by adding a metric family to match to.
func (m Matcher) Family(name string, lps ...*dto.LabelPair) Matcher {
	// prometheus metrics labels are sorted by label name.
	sort.Slice(lps, func(i, j int) bool {
		return lps[i].GetName() < lps[j].GetName()
	})

	pairs := &labelPairs{
		Label: lps,
	}

	family, ok := m[name]
	if !ok {
		family = make(Labels)
	}

	family[pairs.String()] = true
	m[name] = family
	return m
}

// Match returns all metric families that match.
func (m Matcher) Match(mfs []*dto.MetricFamily) []*dto.MetricFamily {
	if len(mfs) == 0 {
		return mfs
	}

	filteredFamilies := []*dto.MetricFamily{}
	for _, mf := range mfs {
		labels, ok := m[mf.GetName()]
		if !ok {
			continue
		}

		metrics := []*dto.Metric{}
		match := false
		for _, metric := range mf.Metric {
			if labels.Match(metric) {
				match = true
				metrics = append(metrics, metric)
			}
		}
		if match {
			filteredFamilies = append(filteredFamilies, &dto.MetricFamily{
				Name:   mf.Name,
				Help:   mf.Help,
				Type:   mf.Type,
				Metric: metrics,
			})
		}
	}

	sort.Sort(familySorter(filteredFamilies))
	return filteredFamilies
}

// L is used with Family to create a series of label pairs for matching.
func L(name, value string) *dto.LabelPair {
	return &dto.LabelPair{
		Name:  proto.String(name),
		Value: proto.String(value),
	}
}

// Labels are string representations of a set of prometheus label pairs that
// are used to match to metric.
type Labels map[string]bool

// Match checks if the metric's labels matches this set of labels.
func (ls Labels) Match(metric *dto.Metric) bool {
	lp := &labelPairs{metric.Label}
	return ls[lp.String()] || ls[""] // match empty string so no labels can be matched.
}

// labelPairs is used to serialize a portion of dto.Metric into a serializable
// string.
type labelPairs struct {
	Label []*dto.LabelPair `protobuf:"bytes,1,rep,name=label" json:"label,omitempty"`
}

func (l *labelPairs) Reset() {}

func (l *labelPairs) String() string {
	var a []string
	for _, lbl := range l.Label {
		a = append(a, fmt.Sprintf("label:<%s> ", lbl.String()))
	}
	return strings.Join(a, "")
}

func (*labelPairs) ProtoMessage() {}
