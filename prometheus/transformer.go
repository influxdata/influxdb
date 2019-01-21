package prometheus

import (
	"sort"

	dto "github.com/prometheus/client_model/go"
)

// Transformer modifies prometheus metrics families.
type Transformer interface {
	// Transform updates the metrics family
	Transform(mfs []*dto.MetricFamily) []*dto.MetricFamily
}

var _ Transformer = (*AddLabels)(nil)

// AddLabels adds labels to all metrics. It will overwrite
// the label if it already exists.
type AddLabels struct {
	Labels map[string]string
}

// Transform adds labels to the metrics.
func (a *AddLabels) Transform(mfs []*dto.MetricFamily) []*dto.MetricFamily {
	for i := range mfs {
		for j, m := range mfs[i].Metric {
			// Filter out labels to add
			labels := m.Label[:0]
			for _, l := range m.Label {
				if _, ok := a.Labels[l.GetName()]; !ok {
					labels = append(labels, l)
				}
			}

			// Add all new labels to the metric
			for k, v := range a.Labels {
				labels = append(labels, L(k, v))
			}
			sort.Sort(labelPairSorter(labels))
			mfs[i].Metric[j].Label = labels
		}
	}
	return mfs
}

var _ Transformer = (*RemoveLabels)(nil)

// RemoveLabels adds labels to all metrics. It will overwrite
// the label if it already exists.
type RemoveLabels struct {
	Labels map[string]struct{}
}

// Transform removes labels from the metrics.
func (r *RemoveLabels) Transform(mfs []*dto.MetricFamily) []*dto.MetricFamily {
	for i := range mfs {
		for j, m := range mfs[i].Metric {
			// Filter out labels
			labels := m.Label[:0]
			for _, l := range m.Label {
				if _, ok := r.Labels[l.GetName()]; !ok {
					labels = append(labels, l)
				}
			}
			mfs[i].Metric[j].Label = labels
		}
	}
	return mfs
}

var _ Transformer = (*RenameFamilies)(nil)

// RenameFamilies changes the name of families to another name
type RenameFamilies struct {
	FromTo map[string]string
}

// Transform renames metric families names.
func (r *RenameFamilies) Transform(mfs []*dto.MetricFamily) []*dto.MetricFamily {
	renamed := mfs[:0]
	for _, mf := range mfs {
		if to, ok := r.FromTo[mf.GetName()]; ok {
			mf.Name = &to
		}
		renamed = append(renamed, mf)

	}
	sort.Sort(familySorter(renamed))
	return renamed
}
